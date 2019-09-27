/*
 * Tencent is pleased to support the open source community by making TubeMQ available.
 *
 * Copyright (C) 2012-2019 Tencent. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.tubemq.server.broker.msgstore.mem;

import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.TErrCodeConstants;
import com.tencent.tubemq.server.broker.BrokerConfig;
import com.tencent.tubemq.server.broker.msgstore.disk.MsgFileStore;
import com.tencent.tubemq.server.broker.utils.DataStoreUtils;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

/***
 * Message's memory storage. It use direct memory store messages that received but not have been flushed to disk.
 */
public class MsgMemStore implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(MsgMemStore.class);
    //　used for align
    private static final int MASK_64_ALIGN = ~(64 - 1);
    //　statistics of memory store
    private final AtomicInteger cacheDataSize = new AtomicInteger(0);
    private final AtomicInteger cacheDataOffset = new AtomicInteger(0);
    private final AtomicInteger cacheIndexSize = new AtomicInteger(0);
    private final AtomicInteger cacheIndexOffset = new AtomicInteger(0);
    private final AtomicInteger curMessageCount = new AtomicInteger(0);
    private final ReentrantLock writeLock = new ReentrantLock();
    //　partitionId to index position, accelerate query
    private final ConcurrentHashMap<Integer, Integer> queuesMap =
            new ConcurrentHashMap<Integer, Integer>(20);
    //　key to index position, used for filter consume
    private final ConcurrentHashMap<Integer, Integer> keysMap =
            new ConcurrentHashMap<Integer, Integer>(100);
    //　where messages in memory will sink to disk
    private long writeDataStartPos = -1;
    private ByteBuffer cacheDataSegment;
    private int maxDataCacheSize;
    private long writeIndexStartPos = -1;
    private ByteBuffer cachedIndexSegment;
    private int maxIndexCacheSize;
    private int maxAllowedMsgCount;
    private int indexUnitLength = 32;

    public MsgMemStore(int maxCacheSize, int maxMsgCount, final BrokerConfig tubeConfig) {
        this.maxDataCacheSize = maxCacheSize;
        this.maxAllowedMsgCount = maxMsgCount;
        this.maxIndexCacheSize = this.maxAllowedMsgCount * this.indexUnitLength;
        this.cacheDataSegment = ByteBuffer.allocateDirect(this.maxDataCacheSize);
        this.cachedIndexSegment = ByteBuffer.allocateDirect(this.maxIndexCacheSize);
    }

    public void resetStartPos(long writeDataStartPos, long writeIndexStartPos) {
        this.clear();
        this.writeDataStartPos = writeDataStartPos;
        this.writeIndexStartPos = writeIndexStartPos;
    }

    public boolean appendMsg(final MsgMemStatisInfo msgMemStatisInfo,
                             final int partitionId, final int keyCode,
                             final long timeRecv, final int entryLength,
                             final ByteBuffer entry) {
        int alignedSize = (entryLength + 64 - 1) & MASK_64_ALIGN;
        boolean fullDataSize = false;
        boolean fullIndexSize = false;
        boolean fullCount = false;
        this.writeLock.lock();
        try {
            //　judge whether can write to memory or not.
            int dataOffset = this.cacheDataOffset.get();
            int indexOffset = this.cacheIndexOffset.get();
            if ((fullDataSize = (dataOffset + alignedSize > this.maxDataCacheSize))
                    || (fullIndexSize = (indexOffset + this.indexUnitLength > this.maxIndexCacheSize))
                    || (fullCount = (this.curMessageCount.get() + 1 > this.maxAllowedMsgCount))) {
                msgMemStatisInfo.addFullTypeCount(timeRecv, fullDataSize, fullIndexSize, fullCount);
                return false;
            }
            // conduct message with filling process
            entry.putLong(DataStoreUtils.STORE_HEADER_POS_QUEUE_LOGICOFF,
                    this.writeIndexStartPos + this.cacheIndexSize.get());
            this.cacheDataSegment.position(dataOffset);
            this.cacheDataSegment.put(entry.array());
            this.cachedIndexSegment.position(indexOffset);
            this.cachedIndexSegment.putInt(partitionId);
            this.cachedIndexSegment.putInt(keyCode);
            this.cachedIndexSegment.putInt(dataOffset);
            this.cachedIndexSegment.putInt(entryLength);
            this.cachedIndexSegment.putLong(timeRecv);
            this.cachedIndexSegment
                    .putLong(this.writeDataStartPos + this.cacheDataSize.getAndAdd(entryLength));
            Integer indexSizePos =
                    this.cacheIndexSize.getAndAdd(DataStoreUtils.STORE_INDEX_HEAD_LEN);
            this.queuesMap.put(partitionId, indexSizePos);
            this.keysMap.put(keyCode, indexSizePos);
            this.cacheDataOffset.getAndAdd(alignedSize);
            this.cacheIndexOffset.getAndAdd(this.indexUnitLength);
            this.curMessageCount.getAndAdd(1);
            msgMemStatisInfo.addMsgSizeStatis(timeRecv, entryLength);
        } finally {
            this.writeLock.unlock();
        }
        return true;
    }

    /***
     * Read from memory, read index, then data.
     *
     * @param lastRdOffset
     * @param lastOffset
     * @param maxReadSize
     * @param maxReadCount
     * @param partitionId
     * @param isSecond
     * @param isFilterConsume
     * @param filterKeySet
     * @return
     */
    public GetCacheMsgResult getMessages(final long lastRdOffset, final long lastOffset,
                                         final int maxReadSize, final int maxReadCount,
                                         final int partitionId, final boolean isSecond,
                                         final boolean isFilterConsume,
                                         final Set<Integer> filterKeySet) {
        // #lizard forgives
        Integer lastWritePos = 0;
        boolean hasMsg = false;
        //　judge memory contains the given offset or not.
        List<ByteBuffer> cacheMsgList = new ArrayList<ByteBuffer>();
        if (lastOffset < this.writeIndexStartPos) {
            return new GetCacheMsgResult(false, TErrCodeConstants.MOVED,
                    lastOffset, "Request offset lower than cache minOffset");
        }
        if (lastOffset >= this.writeIndexStartPos + this.cacheIndexSize.get()) {
            return new GetCacheMsgResult(false, TErrCodeConstants.NOT_FOUND,
                    lastOffset, "Request offset reached cache maxOffset");
        }
        int totalReadSize = 0;
        int currIndexOffset;
        int currDataOffset;
        int currIndexSize;
        long lastDataRdOff = lastRdOffset;
        int startReadSize = (int) (lastOffset - this.writeIndexStartPos);
        this.writeLock.lock();
        try {
            if (isFilterConsume) {
                //　filter conduct. accelerate by keysMap.
                for (Integer keyCode : filterKeySet) {
                    if (keyCode != null) {
                        lastWritePos = this.keysMap.get(keyCode);
                        if ((lastWritePos != null) && (lastWritePos >= startReadSize)) {
                            hasMsg = true;
                            break;
                        }
                    }
                }
            } else {
                // orderly consume by partition id.
                lastWritePos = this.queuesMap.get(partitionId);
                if ((lastWritePos != null) && (lastWritePos >= startReadSize)) {
                    hasMsg = true;
                }
            }
            lastDataRdOff = this.writeDataStartPos + this.cacheDataSize.get();
            currIndexSize = this.cacheIndexSize.get();
            currIndexOffset = this.cacheIndexOffset.get();
            currDataOffset = this.cacheDataOffset.get();
        } finally {
            this.writeLock.unlock();
        }
        int usedPos = 0;
        int limitReadSize = currIndexSize - startReadSize;
        // cannot find message, return not found
        if (!hasMsg) {
            if (isSecond && !isFilterConsume) {
                return new GetCacheMsgResult(true, 0, "Ok2",
                        lastOffset, limitReadSize, lastDataRdOff, totalReadSize, cacheMsgList);
            } else {
                return new GetCacheMsgResult(false, TErrCodeConstants.NOT_FOUND,
                        "Can't found Message by index!", lastOffset,
                        limitReadSize, lastDataRdOff, totalReadSize, cacheMsgList);
            }
        }
        //　fetch data by index.
        int cPartitionId = 0;
        int cKeyCode = 0;
        int cDataOffset = 0;
        int cDataSize = 0;
        long cTimeRecv = 0L;
        long cDataPos = 0L;
        int readedOff = 0;
        ByteBuffer tmpIndexRdBuf = this.cachedIndexSegment.asReadOnlyBuffer();
        ByteBuffer tmpDataRdBuf = this.cacheDataSegment.asReadOnlyBuffer();
        int startReadOff = (int) (startReadSize / DataStoreUtils.STORE_INDEX_HEAD_LEN * this.indexUnitLength);
        //　loop read by index
        for (int count = 0; count < maxReadCount;
             count++, startReadOff += this.indexUnitLength,
                     usedPos += DataStoreUtils.STORE_INDEX_HEAD_LEN) {
            //　cannot find matched message, return
            if ((usedPos >= limitReadSize)
                    || (usedPos + DataStoreUtils.STORE_INDEX_HEAD_LEN > limitReadSize)
                    || (startReadOff >= currIndexOffset)
                    || (startReadOff + this.indexUnitLength > currIndexOffset)) {
                break;
            }
            // read index content.
            tmpIndexRdBuf.position(startReadOff);
            cPartitionId = tmpIndexRdBuf.getInt();
            cKeyCode = tmpIndexRdBuf.getInt();
            cDataOffset = tmpIndexRdBuf.getInt();
            cDataSize = tmpIndexRdBuf.getInt();
            cTimeRecv = tmpIndexRdBuf.getLong();
            cDataPos = tmpIndexRdBuf.getLong();
            //　skip when mismatch condition
            if ((cDataOffset < 0)
                    || (cDataSize <= 0)
                    || (cDataOffset >= currDataOffset)
                    || (cDataSize > TBaseConstants.META_MAX_MESSAGEG_DATA_SIZE + 1024)
                    || (cDataOffset + cDataSize > currDataOffset)) {
                readedOff = usedPos + DataStoreUtils.STORE_INDEX_HEAD_LEN;
                continue;
            }
            if ((cPartitionId != partitionId)
                    || (isFilterConsume && (!filterKeySet.contains(cKeyCode)))) {
                readedOff = usedPos + DataStoreUtils.STORE_INDEX_HEAD_LEN;
                continue;
            }
            //　read data file.
            lastDataRdOff = cDataPos + cDataSize;
            readedOff = usedPos + DataStoreUtils.STORE_INDEX_HEAD_LEN;
            byte[] tmpArray = new byte[cDataSize];
            final ByteBuffer buffer = ByteBuffer.wrap(tmpArray);
            tmpDataRdBuf.position(cDataOffset);
            tmpDataRdBuf.get(tmpArray);
            buffer.rewind();
            cacheMsgList.add(buffer);
            totalReadSize += cDataSize;
            // break when exceed the max transfer size.
            if (totalReadSize >= maxReadSize) {
                break;
            }
        }
        // return result
        return new GetCacheMsgResult(true, 0, "Ok1",
                lastOffset, readedOff, lastDataRdOff, totalReadSize, cacheMsgList);
    }

    /***
     * Flush memory to disk.
     *
     * @param msgFileStore
     * @param strBuffer
     * @return
     * @throws IOException
     */
    public boolean flush(MsgFileStore msgFileStore, final StringBuilder strBuffer) throws IOException {
        if (this.curMessageCount.get() == 0) {
            return true;
        }
        int count = 0;
        int readPos = 0;
        int cPartitionId = 0;
        int cKeyCode = 0;
        int cDataOffset = 0;
        int cDataSize = 0;
        long cTimeRecv = 0;
        long cDataPos = 0;
        ByteBuffer tmpBuffer = this.cachedIndexSegment.asReadOnlyBuffer();
        final ByteBuffer tmpReadBuf = this.cacheDataSegment.asReadOnlyBuffer();
        // flush one by one.
        while (count++ < this.curMessageCount.get()) {
            tmpBuffer.position(readPos);
            cPartitionId = tmpBuffer.getInt();
            cKeyCode = tmpBuffer.getInt();
            cDataOffset = tmpBuffer.getInt();
            cDataSize = tmpBuffer.getInt();
            cTimeRecv = tmpBuffer.getLong();
            cDataPos = tmpBuffer.getLong();
            readPos += this.indexUnitLength;
            if (cDataOffset >= this.cacheDataOffset.get()
                    || cDataSize > this.cacheDataSize.get()
                    || cDataSize > TBaseConstants.META_MAX_MESSAGEG_DATA_SIZE + 1024
                    || cDataOffset + cDataSize > this.cacheDataOffset.get()) {
                logger.error(strBuffer
                        .append("[Mem Cache] flush Message found error data: cDataOffset=")
                        .append(cDataOffset).append(",cDataSize=").append(cDataSize)
                        .append(",cacheDataOffset=").append(this.cacheDataOffset.get())
                        .append(",cacheDataSize=").append(this.cacheDataSize.get()).toString());
                strBuffer.delete(0, strBuffer.length());
                continue;
            }
            byte[] tmpArray = new byte[cDataSize];
            final ByteBuffer buffer = ByteBuffer.wrap(tmpArray);
            tmpReadBuf.position(cDataOffset);
            tmpReadBuf.get(tmpArray);
            msgFileStore.appendMsg(cPartitionId, cKeyCode,
                    cTimeRecv, cDataPos, cDataSize, buffer, strBuffer);
        }
        return true;
    }

    public int getCurMsgCount() {
        return this.curMessageCount.get();
    }

    public int getCurDataCacheSize() {
        return this.cacheDataSize.get();
    }

    public int getIndexCacheSize() {
        return this.cacheIndexSize.get();
    }

    public int getMaxDataCacheSize() {
        return this.maxDataCacheSize;
    }

    public int getMaxAllowedMsgCount() {
        return this.maxAllowedMsgCount;
    }

    public int isOffsetInHold(long requestOffset) {
        if (requestOffset < this.writeIndexStartPos) {
            return -1;
        } else if (requestOffset >= this.writeIndexStartPos + this.cacheIndexSize.get()) {
            return 1;
        }
        return 0;
    }

    public long getDataLastWritePos() {
        return this.writeDataStartPos + this.cacheDataSize.get();
    }

    public long getIndexLastWritePos() {
        return this.writeIndexStartPos + this.cacheIndexSize.get();
    }

    public void clear() {
        this.writeDataStartPos = -1;
        this.writeIndexStartPos = -1;
        this.cacheDataSize.set(0);
        this.cacheDataOffset.set(0);
        this.cacheIndexSize.set(0);
        this.cacheIndexOffset.set(0);
        this.curMessageCount.set(0);
        this.queuesMap.clear();
        this.keysMap.clear();
    }

    @Override
    public void close() {
        this.clear();
        ((DirectBuffer) this.cacheDataSegment).cleaner().clean();
        ((DirectBuffer) this.cachedIndexSegment).cleaner().clean();
    }

}

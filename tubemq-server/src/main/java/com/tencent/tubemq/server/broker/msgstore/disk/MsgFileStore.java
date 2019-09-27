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

package com.tencent.tubemq.server.broker.msgstore.disk;

import com.tencent.tubemq.corebase.TErrCodeConstants;
import com.tencent.tubemq.corebase.protobuf.generated.ClientBroker;
import com.tencent.tubemq.corebase.utils.ServiceStatusHolder;
import com.tencent.tubemq.server.broker.BrokerConfig;
import com.tencent.tubemq.server.broker.msgstore.MessageStore;
import com.tencent.tubemq.server.broker.msgstore.ssd.SSDSegFound;
import com.tencent.tubemq.server.broker.stats.CountItem;
import com.tencent.tubemq.server.broker.utils.DataStoreUtils;
import com.tencent.tubemq.server.common.TServerConstants;
import com.tencent.tubemq.server.common.utils.FileUtil;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Message file's storage. Contains data file and index file.
 */
public class MsgFileStore implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(MsgFileStore.class);
    private static final int MAX_META_REFRESH_DUR = 1000 * 60 * 60;
    // storage ID
    private final String storeKey;
    // data file storage directory
    private final File dataDir;
    // index file storage directory
    private final File indexDir;
    // disk flush parameters: current unflushed message count
    private final AtomicInteger curUnflushed = new AtomicInteger(0);
    // current unflushed message size
    private final AtomicLong curUnflushSize = new AtomicLong(0);
    // time of data's last flush operation
    private final AtomicLong lastFlushTime = new AtomicLong(System.currentTimeMillis());
    // time of meta's last flush operation
    private final AtomicLong lastMetaFlushTime = new AtomicLong(0);
    private final BrokerConfig tubeConfig;
    // lock used for append message to storage
    private final ReentrantLock writeLock = new ReentrantLock();
    private final ByteBuffer byteBufferIndex =
            ByteBuffer.allocate(DataStoreUtils.STORE_INDEX_HEAD_LEN);
    // message storage
    private final MessageStore messageStore;
    // data file segment list
    private final SegmentList dataSegments;
    // index file segment list
    private final SegmentList indexSegments;
    // close status
    private final AtomicBoolean closed = new AtomicBoolean(false);


    public MsgFileStore(final MessageStore messageStore,
                        final BrokerConfig tubeConfig,
                        final String baseStorePath,
                        final long offsetIfCreate) throws IOException {
        final StringBuilder sBuilder = new StringBuilder(512);
        this.tubeConfig = tubeConfig;
        this.messageStore = messageStore;
        this.storeKey = messageStore.getStoreKey();
        this.dataDir = new File(sBuilder.append(baseStorePath)
                .append(File.separator).append(this.storeKey).toString());
        sBuilder.delete(0, sBuilder.length());
        this.indexDir = new File(sBuilder.append(baseStorePath)
                .append(File.separator).append(this.storeKey)
                .append(File.separator).append("index").toString());
        sBuilder.delete(0, sBuilder.length());
        FileUtil.checkDir(this.dataDir);
        FileUtil.checkDir(this.indexDir);
        this.dataSegments =
                new FileSegmentList(this.dataDir,
                        SegmentType.DATA, true, offsetIfCreate, Long.MAX_VALUE, sBuilder);
        this.indexSegments =
                new FileSegmentList(this.indexDir,
                        SegmentType.INDEX, true, offsetIfCreate, Long.MAX_VALUE, sBuilder);
        this.lastFlushTime.set(System.currentTimeMillis());
    }


    public void appendMsg(final int partitionId, final int keyCode, final long timeRecv,
                          final long inDataOffset, final int msgSize, final ByteBuffer buffer,
                          final StringBuilder sb) throws IOException {
        //　append message, put in data file first, then index file.
        if (this.closed.get()) {
            throw new IllegalStateException(new StringBuilder(512)
                    .append("Closed MessageStore for storeKey ")
                    .append(this.storeKey).toString());
        }
        boolean isDataFlushed = false;
        boolean isIndexFlushed = false;
        boolean isMsgCntFlushed = false;
        boolean isMsgTimeFushed = false;
        final MsgFileStatisInfo msgFileStatisInfo =
                messageStore.getFileMsgSizeStatisInfo();
        this.writeLock.lock();
        try {
            final long inIndexOffset =
                    buffer.getLong(DataStoreUtils.STORE_HEADER_POS_QUEUE_LOGICOFF);
            final Segment curDataSeg = this.dataSegments.last();
            this.curUnflushSize.addAndGet(msgSize);
            final long dataOffset = curDataSeg.append(buffer);
            // judge whether need to create a new data segment.
            if (curDataSeg.getCachedSize() >= this.tubeConfig.getMaxSegmentSize()) {
                isDataFlushed = true;
                final long newDataOffset = curDataSeg.flush(true);
                final File newDataFile =
                        new File(this.dataDir,
                                DataStoreUtils.nameFromOffset(newDataOffset, DataStoreUtils.DATA_FILE_SUFFIX));
                curDataSeg.setMutable(false);
                logger.info(sb.append("[File Store] Created data segment ")
                        .append(newDataFile.getAbsolutePath()).toString());
                sb.delete(0, sb.length());
                this.dataSegments.append(new FileSegment(newDataOffset, newDataFile, SegmentType.DATA));
            }
            // filling index data.
            this.byteBufferIndex.clear();
            this.byteBufferIndex.putInt(partitionId);
            this.byteBufferIndex.putLong(dataOffset);
            this.byteBufferIndex.putInt(msgSize);
            this.byteBufferIndex.putInt(keyCode);
            this.byteBufferIndex.putLong(timeRecv);
            this.byteBufferIndex.flip();
            final Segment curIndexSeg = this.indexSegments.last();
            final long indexOffset = curIndexSeg.append(this.byteBufferIndex);
            // judge whether need to create a new index segment.
            if (curIndexSeg.getCachedSize()
                    >= this.tubeConfig.getMaxIndexSegmentSize()) {
                isIndexFlushed = true;
                final long newIndexOffset = curIndexSeg.flush(true);
                final File newIndexFile =
                        new File(this.indexDir,
                                DataStoreUtils.nameFromOffset(newIndexOffset, DataStoreUtils.INDEX_FILE_SUFFIX));
                curIndexSeg.setMutable(false);
                logger.info(sb.append("[File Store] Created index segment ")
                        .append(newIndexFile.getAbsolutePath()).toString());
                sb.delete(0, sb.length());
                this.indexSegments.append(new FileSegment(newIndexOffset,
                        newIndexFile, SegmentType.INDEX));
            }
            // check whether need to flush to disk.
            long currTime = System.currentTimeMillis();
            if ((isMsgCntFlushed = this.curUnflushed.addAndGet(1)
                    >= messageStore.getUnflushThreshold())
                    || (isMsgTimeFushed = currTime - this.lastFlushTime.get()
                    >= messageStore.getUnflushInterval())
                    || isDataFlushed || isIndexFlushed) {
                boolean forceMetadata = (isDataFlushed
                        || isIndexFlushed
                        || (currTime - this.lastMetaFlushTime.get() > MAX_META_REFRESH_DUR));
                if (!isDataFlushed) {
                    curDataSeg.flush(forceMetadata);
                }
                if (!isIndexFlushed) {
                    curIndexSeg.flush(forceMetadata);
                }
                // add statistics.
                msgFileStatisInfo.addFullTypeCount(currTime,
                        isDataFlushed, isIndexFlushed, isMsgCntFlushed, isMsgTimeFushed,
                        this.curUnflushSize.get(), this.curUnflushed.get());
                this.curUnflushSize.set(0);
                this.curUnflushed.set(0);
                this.lastFlushTime.set(System.currentTimeMillis());
                if (forceMetadata) {
                    this.lastMetaFlushTime.set(System.currentTimeMillis());
                }
            }
            if (inIndexOffset != indexOffset || inDataOffset != dataOffset) {
                logger.error(sb.append("[File Store]: appendMsg data Error, storekey=")
                        .append(this.storeKey).append(",msgSize=").append(msgSize)
                        .append(",bufferSize=").append(buffer.array().length)
                        .append(", inIndexOffset=").append(inIndexOffset)
                        .append(", indexOffset=").append(indexOffset)
                        .append(",inDataOffset=").append(inDataOffset)
                        .append(",dataOffset=").append(dataOffset).toString());
                sb.delete(0, sb.length());
            }
        } catch (final IOException e) {
            ServiceStatusHolder.addWriteIOErrCnt();
            logger.error("[File Store] Append message in file failed ", e);
            throw e;
        } finally {
            this.writeLock.unlock();
        }
    }

    /***
     * Get message from index and data files.
     *
     * @param partitionId
     * @param lastRdOffset
     * @param reqOffset
     * @param indexBuffer
     * @param isFilterConsume
     * @param filterKeySet
     * @param statisKeyBase
     * @param maxMsgTransferSize
     * @return
     */
    public GetMessageResult getMessages(final int partitionId, final long lastRdOffset,
                                        final long reqOffset, final ByteBuffer indexBuffer,
                                        final boolean isFilterConsume,
                                        final Set<Integer> filterKeySet,
                                        final String statisKeyBase,
                                        final int maxMsgTransferSize) {
        // #lizard forgives
        //　Orderly read from index file, then random read from data file.
        int retCode = 0;
        int totalSize = 0;
        String errInfo = "Ok";
        boolean result = true;
        int dataRealLimit = 0;
        int curIndexOffset = 0;
        int readedOffset = 0;
        RecordView recordView = null;
        int curIndexPartitionId = 0;
        long curIndexDataOffset = 0L;
        int curIndexDataSize = 0;
        int curIndexKeyCode = 0;
        long recvTimeInMillsec = 0L;
        long maxDataLimitOffset = 0L;
        long lastRdDataOffset = 0L;
        final StringBuilder sBuilder = new StringBuilder(512);
        final long curDataMaxOffset = getDataMaxOffset();
        final long curDataMinOffset = getDataMinOffset();
        HashMap<String, CountItem> countMap = new HashMap<String, CountItem>();
        ByteBuffer dataBuffer =
                ByteBuffer.allocate(TServerConstants.CFG_STORE_DEFAULT_MSG_READ_UNIT);
        List<ClientBroker.TransferedMessage> transferedMessageList =
                new ArrayList<ClientBroker.TransferedMessage>();
        int maxSegmentSize =
                tubeConfig.getMaxSegmentSize() + DataStoreUtils.MAX_READ_BUFFER_ADJUST;
        // read data file by index.
        for (curIndexOffset = 0; curIndexOffset < indexBuffer.remaining();
             curIndexOffset += DataStoreUtils.STORE_INDEX_HEAD_LEN) {
            curIndexPartitionId = indexBuffer.getInt();
            curIndexDataOffset = indexBuffer.getLong();
            curIndexDataSize = indexBuffer.getInt();
            curIndexKeyCode = indexBuffer.getInt();
            recvTimeInMillsec = indexBuffer.getLong();
            maxDataLimitOffset = curIndexDataOffset + curIndexDataSize;
            // skip when mismatch condition
            if (curIndexDataOffset < 0
                    || curIndexDataSize <= 0
                    || curIndexDataSize > DataStoreUtils.STORE_MAX_MESSAGE_STORE_LEN
                    || curIndexDataOffset < curDataMinOffset) {
                readedOffset = curIndexOffset + DataStoreUtils.STORE_INDEX_HEAD_LEN;
                continue;
            }
            // read finish, then return.
            if (curIndexDataOffset >= curDataMaxOffset
                    || maxDataLimitOffset > curDataMaxOffset) {
                lastRdDataOffset = curIndexDataOffset;
                break;
            }
            // conduct filter operation.
            if (curIndexPartitionId != partitionId
                    || (isFilterConsume
                    && !filterKeySet.contains(curIndexKeyCode))) {
                lastRdDataOffset = maxDataLimitOffset;
                readedOffset = curIndexOffset + DataStoreUtils.STORE_INDEX_HEAD_LEN;
                continue;
            }
            try {
                // get data from data file by index one by one.
                if (recordView == null
                        || !((curIndexDataOffset >= recordView.getStartOffset())
                        && (maxDataLimitOffset <= recordView.getStartOffset() + recordView.getCommitSize()))) {
                    if (recordView != null) {
                        dataSegments.relRecordView(recordView);
                        recordView = null;
                    }
                    recordView = dataSegments.getRecordView(curIndexDataOffset, maxSegmentSize);
                    if (recordView == null) {
                        continue;
                    }
                }
                if (dataBuffer.capacity() < curIndexDataSize) {
                    dataBuffer = ByteBuffer.allocate(curIndexDataSize);
                }
                dataBuffer.clear();
                dataBuffer.limit(curIndexDataSize);
                recordView.read(dataBuffer, curIndexDataOffset - recordView.getStartOffset());
                dataBuffer.flip();
                dataRealLimit = dataBuffer.limit();
                if (dataRealLimit < curIndexDataSize) {
                    lastRdDataOffset = curIndexDataOffset;
                    readedOffset = curIndexOffset + DataStoreUtils.STORE_INDEX_HEAD_LEN;
                    continue;
                }
            } catch (Throwable e2) {
                if (e2 instanceof IOException) {
                    ServiceStatusHolder.addReadIOErrCnt();
                }
                logger.warn(sBuilder.append("[File Store] Get message from file failure,storeKey=")
                        .append(messageStore.getStoreKey()).append(", partitionId=")
                        .append(partitionId).toString(), e2);
                retCode = TErrCodeConstants.INTERNAL_SERVER_ERROR;
                sBuilder.delete(0, sBuilder.length());
                errInfo = sBuilder.append("Get message from file failure : ")
                        .append(e2.getCause()).toString();
                sBuilder.delete(0, sBuilder.length());
                result = false;
                break;
            }
            // build query result.
            readedOffset = curIndexOffset + DataStoreUtils.STORE_INDEX_HEAD_LEN;
            lastRdDataOffset = maxDataLimitOffset;
            ClientBroker.TransferedMessage transferedMessage =
                    DataStoreUtils.getTransferMsg(dataBuffer,
                            curIndexDataSize, countMap, statisKeyBase, sBuilder);
            if (transferedMessage == null) {
                continue;
            }
            transferedMessageList.add(transferedMessage);
            totalSize += curIndexDataSize;
            // break when exceed the max transfer size.
            if (totalSize >= maxMsgTransferSize) {
                break;
            }
        }
        // release resource
        if (recordView != null) {
            dataSegments.relRecordView(recordView);
        }
        if (retCode != 0) {
            if (!transferedMessageList.isEmpty()) {
                retCode = 0;
                errInfo = "Ok";
            }
        }
        if (lastRdDataOffset <= 0L) {
            lastRdDataOffset = lastRdOffset;
        }
        // return result.
        return new GetMessageResult(result, retCode, errInfo,
                reqOffset, readedOffset, lastRdDataOffset,
                totalSize, countMap, transferedMessageList);
    }

    @Override
    public void close() throws IOException {
        if (this.closed.compareAndSet(false, true)) {
            this.writeLock.lock();
            try {
                this.indexSegments.close();
                this.dataSegments.close();
            } finally {
                this.writeLock.unlock();
            }
        }
    }

    /***
     * Clean expired data files and index files.
     *
     * @param onlyCheck
     * @return
     */
    public boolean runClearupPolicy(boolean onlyCheck) {
        final StringBuilder sBuilder = new StringBuilder(512);
        final long start = System.currentTimeMillis();
        boolean hasExpiredDataSegs =
                dataSegments.checkExpiredSegments(start, messageStore.getMaxFileValidDurMs());
        boolean hasExpiredIndexSegs =
                indexSegments.checkExpiredSegments(start, messageStore.getMaxFileValidDurMs());
        if (onlyCheck) {
            return (hasExpiredDataSegs || hasExpiredIndexSegs);
        }
        if (hasExpiredDataSegs) {
            dataSegments.delExpiredSegments(sBuilder);
        }
        if (hasExpiredIndexSegs) {
            indexSegments.delExpiredSegments(sBuilder);
        }
        return (hasExpiredDataSegs || hasExpiredIndexSegs);
    }

    /***
     * Flush data to disk at interval.
     *
     * @throws IOException
     */
    public void flushDiskFile() throws IOException {
        long checkTimestamp = System.currentTimeMillis();
        if ((curUnflushed.get() > 0)
                && (checkTimestamp - lastFlushTime.get() >= messageStore.getUnflushInterval())) {
            final MsgFileStatisInfo msgFileStatisInfo = messageStore.getFileMsgSizeStatisInfo();
            this.writeLock.lock();
            try {
                checkTimestamp = System.currentTimeMillis();
                if (curUnflushed.get() >= 0
                        && checkTimestamp - lastFlushTime.get() >= messageStore.getUnflushInterval()) {
                    boolean forceMetadata =
                            checkTimestamp - lastMetaFlushTime.get() > MAX_META_REFRESH_DUR;
                    dataSegments.flushLast(forceMetadata);
                    indexSegments.flushLast(forceMetadata);
                    if (forceMetadata) {
                        this.lastMetaFlushTime.set(checkTimestamp);
                    }
                    msgFileStatisInfo.addFullTypeCount(checkTimestamp, false, false,
                            false, false, curUnflushSize.get(), curUnflushed.get());
                    curUnflushSize.set(0);
                    curUnflushed.set(0);
                    lastFlushTime.set(checkTimestamp);
                }
            } finally {
                this.writeLock.unlock();
            }
        }
        return;
    }

    public long getDataSizeInBytes() {
        return dataSegments.getSizeInBytes();
    }

    public long getIndexSizeInBytes() {
        return indexSegments.getSizeInBytes();
    }

    public long getDataMaxOffset() {
        return dataSegments.getMaxOffset();
    }

    public long getDataHighMaxOffset() {
        return dataSegments.getCommitMaxOffset();
    }

    public long getDataMinOffset() {
        return dataSegments.getMinOffset();
    }

    public long getIndexMaxOffset() {
        return this.indexSegments.getMaxOffset();
    }

    public long getIndexMaxHighOffset() {
        return this.indexSegments.getCommitMaxOffset();
    }

    public long getIndexMinOffset() {
        return this.indexSegments.getMinOffset();
    }

    /***
     * Read from ssd file.
     *
     * @param offset
     * @param rate
     * @return
     * @throws IOException
     */
    public SSDSegFound getSourceSegment(final long offset, final int rate) throws IOException {

        final Segment segment = this.dataSegments.findSegment(offset);
        if (segment == null) {
            return new SSDSegFound(false, -1, null);
        }
        long dataSize = segment.getCachedSize();
        if ((dataSize - offset + segment.getStart()) < dataSize * rate / 100) {
            return new SSDSegFound(false, -2, null,
                    segment.getStart(), segment.getStart() + segment.getCachedSize());
        }
        return new SSDSegFound(true, 0, segment.getFile(),
                segment.getStart(), segment.getStart() + segment.getCachedSize());
    }

    public RecordView indexSlice(final long offset, final int maxSize) throws IOException {
        return indexSegments.getRecordView(offset, maxSize);
    }

}

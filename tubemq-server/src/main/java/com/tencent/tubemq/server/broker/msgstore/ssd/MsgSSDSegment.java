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

package com.tencent.tubemq.server.broker.msgstore.ssd;

import com.tencent.tubemq.corebase.TErrCodeConstants;
import com.tencent.tubemq.corebase.protobuf.generated.ClientBroker;
import com.tencent.tubemq.server.broker.msgstore.disk.FileSegment;
import com.tencent.tubemq.server.broker.msgstore.disk.GetMessageResult;
import com.tencent.tubemq.server.broker.msgstore.disk.RecordView;
import com.tencent.tubemq.server.broker.msgstore.disk.Segment;
import com.tencent.tubemq.server.broker.msgstore.disk.SegmentType;
import com.tencent.tubemq.server.broker.stats.CountItem;
import com.tencent.tubemq.server.broker.utils.DataStoreUtils;
import com.tencent.tubemq.server.common.TServerConstants;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Messages in ssd format, it same to messages in disk.
 */
public class MsgSSDSegment implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(MsgSSDSegment.class);
    private final Segment dataSegment;
    private final ConcurrentHashMap<String, SSDVisitInfo> visitMap =
            new ConcurrentHashMap<String, SSDVisitInfo>();
    private final String topic;
    private final String storeKey;
    private AtomicLong lastReadTime = new AtomicLong(System.currentTimeMillis());
    private AtomicInteger curStatus = new AtomicInteger(0);   // 0:无效  1：有效; 2:老化中
    private AtomicLong expiredWait = new AtomicLong(System.currentTimeMillis());


    public MsgSSDSegment(final String storeKey, final String topic,
                         final long start, final File file) throws IOException {
        this.topic = topic;
        this.storeKey = storeKey;
        this.dataSegment = new FileSegment(start, file, false, SegmentType.DATA);
        this.curStatus.set(1);
        this.lastReadTime.set(System.currentTimeMillis());
    }

    /***
     * Get messages from ssd.
     *
     * @param partStr
     * @param partitionId
     * @param reqOffset
     * @param lastRDOffset
     * @param indexBuffer
     * @param isFilterConsume
     * @param filterKeys
     * @param statisKeyBase
     * @param maxMsgTransferSize
     * @param strBuffer
     * @return
     */
    public GetMessageResult getMessages(final String partStr, final int partitionId,
                                        final long reqOffset, final long lastRDOffset,
                                        final ByteBuffer indexBuffer,
                                        final boolean isFilterConsume, final List<Integer> filterKeys,
                                        final String statisKeyBase, final int maxMsgTransferSize,
                                        final StringBuilder strBuffer) {
        // #lizard forgives
        int retCode = 0;
        int totalSize = 0;
        String errInfo = "Ok";
        boolean result = true;
        int dataRealLimit = 0;
        int curIndexPartitionId = 0;
        long curIndexDataOffset = 0L;
        int curIndexDataSize = 0;
        int curIndexKeyCode = 0;
        int readedOffset = 0;
        long recvTimeInMillsec = 0L;
        long maxDataLimitOffset = 0L;
        long lastRdDataOffset = -1L;
        final long curDataMaxOffset = getDataMaxOffset();
        final long curDataMinOffset = getDataMinOffset();
        HashMap<String, CountItem> countMap = new HashMap<String, CountItem>();
        ByteBuffer dataBuffer =
                ByteBuffer.allocate(TServerConstants.CFG_STORE_DEFAULT_MSG_READ_UNIT);
        List<ClientBroker.TransferedMessage> transferedMessageList =
                new ArrayList<ClientBroker.TransferedMessage>();
        RecordView dataSet = this.dataSegment.getViewRef();
        if (dataSet == null) {
            logger.error(strBuffer.append("[SSD Store] Found SSD fileRecordView is null! storeKey=")
                    .append(this.storeKey).toString());
            strBuffer.delete(0, strBuffer.length());
            return new GetMessageResult(false, TErrCodeConstants.INTERNAL_SERVER_ERROR_MSGSET_NULL,
                    reqOffset, 0, "SSD fileRecordView is null!");
        }
        SSDVisitInfo ssdVisitInfo = visitMap.get(partStr);
        if (ssdVisitInfo == null) {
            SSDVisitInfo ssdVisitInfo1 = new SSDVisitInfo(partStr, lastRDOffset);
            ssdVisitInfo =
                    visitMap.putIfAbsent(partStr, ssdVisitInfo1);
            if (ssdVisitInfo == null) {
                ssdVisitInfo = ssdVisitInfo1;
            }
        }
        lastReadTime.set(System.currentTimeMillis());
        ssdVisitInfo.requestVisit(lastRDOffset);
        // read data by index.
        for (int curIndexOffset = 0;
             curIndexOffset < indexBuffer.remaining();
             curIndexOffset += DataStoreUtils.STORE_INDEX_HEAD_LEN) {
            curIndexPartitionId = indexBuffer.getInt();
            curIndexDataOffset = indexBuffer.getLong();
            curIndexDataSize = indexBuffer.getInt();
            curIndexKeyCode = indexBuffer.getInt();
            recvTimeInMillsec = indexBuffer.getLong();
            maxDataLimitOffset = curIndexDataOffset + curIndexDataSize;
            if (curIndexDataOffset < 0
                    || curIndexPartitionId < 0
                    || curIndexDataSize <= 0
                    || curIndexDataSize > DataStoreUtils.STORE_MAX_MESSAGE_STORE_LEN) {
                readedOffset = curIndexOffset + DataStoreUtils.STORE_INDEX_HEAD_LEN;
                continue;
            }
            if (curIndexDataOffset < curDataMinOffset
                    || curIndexDataOffset >= curDataMaxOffset) {
                lastRdDataOffset = curIndexDataOffset;
                break;
            }
            if (curIndexPartitionId != partitionId
                    || maxDataLimitOffset > curDataMaxOffset
                    || (isFilterConsume && !filterKeys.contains(curIndexKeyCode))) {
                lastRdDataOffset = maxDataLimitOffset;
                readedOffset = curIndexOffset + DataStoreUtils.STORE_INDEX_HEAD_LEN;
                continue;
            }
            try {
                if (dataBuffer.capacity() < curIndexDataSize) {
                    dataBuffer = ByteBuffer.allocate(curIndexDataSize);
                }
                dataBuffer.clear();
                dataBuffer.limit(curIndexDataSize);
                dataSet.read(dataBuffer, curIndexDataOffset - dataSet.getStartOffset());
                dataBuffer.flip();
                dataRealLimit = dataBuffer.limit();
                if (dataRealLimit < curIndexDataSize) {
                    lastRdDataOffset = curIndexDataOffset;
                    readedOffset = curIndexOffset + DataStoreUtils.STORE_INDEX_HEAD_LEN;
                    continue;
                }
            } catch (Throwable e2) {
                strBuffer.delete(0, strBuffer.length());
                logger.warn(strBuffer
                        .append("[SSD Store] Get message from file failure,storeKey=")
                        .append(this.storeKey).append(", partitionId=")
                        .append(partitionId).toString(), e2);
                retCode = TErrCodeConstants.INTERNAL_SERVER_ERROR;
                strBuffer.delete(0, strBuffer.length());
                errInfo = strBuffer.append("Get message from file failure : ")
                        .append(e2.getCause()).toString();
                strBuffer.delete(0, strBuffer.length());
                result = false;
                break;
            }
            lastRdDataOffset = maxDataLimitOffset;
            readedOffset = curIndexOffset + DataStoreUtils.STORE_INDEX_HEAD_LEN;
            ClientBroker.TransferedMessage transferedMessage =
                    DataStoreUtils.getTransferMsg(dataBuffer,
                            curIndexDataSize, countMap, statisKeyBase, strBuffer);
            if (transferedMessage == null) {
                continue;
            }
            transferedMessageList.add(transferedMessage);
            totalSize += curIndexDataSize;
            if (totalSize >= maxMsgTransferSize) {
                break;
            }
        }
        if (retCode != 0) {
            if (!transferedMessageList.isEmpty()) {
                retCode = 0;
                errInfo = "Ok";
            }
        }
        lastReadTime.set(System.currentTimeMillis());
        ssdVisitInfo.responseVisit(lastRdDataOffset);
        if (lastRdDataOffset <= 0L) {
            lastRdDataOffset = lastRDOffset;
        }
        return new GetMessageResult(result, retCode, errInfo,
                reqOffset, readedOffset, lastRdDataOffset,
                totalSize, countMap, transferedMessageList, true);
    }

    public void setInUse() {
        this.curStatus.set(1);
        this.lastReadTime.set(System.currentTimeMillis());
        this.expiredWait.set(System.currentTimeMillis());
    }

    public Segment getDataSegment() {
        return dataSegment;
    }

    public ConcurrentHashMap<String, SSDVisitInfo> getVisitMap() {
        return visitMap;
    }

    @Override
    public void close() throws IOException {
        this.curStatus.set(0);
        try {
            if (this.dataSegment != null) {
                this.dataSegment.close();
            }
        } catch (Throwable e1) {
            logger.error(new StringBuilder(512).append("[SSD Store] Close ")
                    .append(this.dataSegment.getFile().getAbsolutePath())
                    .append("'s file failure").toString(), e1);
        }
    }

    public long getDataMaxOffset() {
        return dataSegment.getLast();
    }

    public long getDataMinOffset() {
        return this.dataSegment.getStart();
    }

    public int getOffsetPos(long dataOffset) {
        if (dataOffset < dataSegment.getStart()) {
            return -1;
        } else if (dataOffset >= dataSegment.getStart() + dataSegment.getCachedSize()) {
            return 1;
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MsgSSDSegment)) {
            return false;
        }
        MsgSSDSegment that = (MsgSSDSegment) o;
        if (expiredWait.get() != that.expiredWait.get()) {
            return false;
        }
        if (!curStatus.equals(that.curStatus)) {
            return false;
        }
        return storeKey.equals(that.storeKey);

    }

    @Override
    public int hashCode() {
        int result = curStatus.hashCode();
        result = 31 * result + storeKey.hashCode();
        result = 31 * result + (int) (expiredWait.get() ^ (expiredWait.get() >>> 32));
        return result;
    }

    public void closeConsumeRel(final String partStr) {
        this.visitMap.remove(partStr);
    }

    public String getStoreKey() {
        return this.storeKey;
    }

    public long getDataSizeInBytes() {
        return dataSegment.getCachedSize();
    }

    public boolean isSegmentValid() {
        long curTime = System.currentTimeMillis();
        return !(this.curStatus.get() > 1
                || (this.curStatus.get() > 0
                && curTime - this.lastReadTime.get() >= 15 * 1000));
    }

    public int getCurStatus() {
        return curStatus.get();
    }

    public boolean setAndGetSegmentExpired() {
        long curTime = System.currentTimeMillis();
        if (this.curStatus.get() > 0
                && this.visitMap.isEmpty()
                && (curTime - this.lastReadTime.get() >= 15 * 1000)) {
            if (this.curStatus.get() == 1) {
                this.curStatus.set(2);
                this.expiredWait.set(curTime);
                return false;
            } else if (curTime - this.expiredWait.get() >= 15 * 1000) {
                return true;
            }
            return false;
        }
        return false;
    }

}

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
import com.tencent.tubemq.corebase.utils.ConcurrentHashSet;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.corebase.utils.ThreadUtils;
import com.tencent.tubemq.server.broker.BrokerConfig;
import com.tencent.tubemq.server.broker.exception.StartupException;
import com.tencent.tubemq.server.broker.metadata.MetadataManage;
import com.tencent.tubemq.server.broker.metadata.TopicMetadata;
import com.tencent.tubemq.server.broker.msgstore.MessageStoreManager;
import com.tencent.tubemq.server.broker.msgstore.disk.GetMessageResult;
import com.tencent.tubemq.server.broker.nodeinfo.ConsumerNodeInfo;
import com.tencent.tubemq.server.broker.utils.DataStoreUtils;
import com.tencent.tubemq.server.common.utils.FileUtil;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Message on ssd storage management. It mainly store message on ssd, and copy to disk.
 * Query messages in copied file, copy files if has not copied.
 * Expired files will be delete when exceed storage quota.
 */
public class MsgSSDStoreManager implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(MsgSSDStoreManager.class);
    // file suffix
    private static final String DATA_FILE_SUFFIX = ".tube";
    // tube config
    private final BrokerConfig tubeConfig;
    private final String primStorePath;
    private final String secdStorePath;
    // ssd data directory
    private final File ssdBaseDataDir;
    private final BlockingQueue<SSDSegEvent> reqSSDEvents
            = new ArrayBlockingQueue<SSDSegEvent>(60);
    private final ExecutorService statusCheckExecutor = Executors.newSingleThreadExecutor();
    private final ExecutorService reqExecutor = Executors.newSingleThreadExecutor();
    // total ssd files size
    private final AtomicLong totalSsdFileSize = new AtomicLong(0L);
    // total ssd file count
    private final AtomicInteger totalSsdFileCnt = new AtomicInteger(0);
    private final MessageStoreManager msgStoreMgr;
    private volatile boolean closed = false;
    private volatile boolean isStart = true;
    private AtomicInteger firstChecked = new AtomicInteger(3);
    // ssd segments
    private ConcurrentHashMap<String, ConcurrentHashMap<Long, MsgSSDSegment>> ssdSegmentsMap =
            new ConcurrentHashMap<String, ConcurrentHashMap<Long, MsgSSDSegment>>(60);
    private ConcurrentHashMap<String, ConcurrentHashSet<SSDSegIndex>> ssdPartStrMap =
            new ConcurrentHashMap<String, ConcurrentHashSet<SSDSegIndex>>(60);
    private long startTime = System.currentTimeMillis();
    private long lastCheckTime = System.currentTimeMillis();


    public MsgSSDStoreManager(final MessageStoreManager msgStoreMgr,
                              final BrokerConfig tubeConfig) throws IOException {
        this.tubeConfig = tubeConfig;
        this.msgStoreMgr = msgStoreMgr;
        this.primStorePath = this.tubeConfig.getPrimaryPath();
        this.secdStorePath = TStringUtils.isBlank(this.tubeConfig.getSecondDataPath())
                ? null : this.tubeConfig.getSecondDataPath();
        if (TStringUtils.isBlank(this.secdStorePath)
                || this.primStorePath.equals(this.secdStorePath)) {
            this.isStart = false;
            this.ssdBaseDataDir = null;
            logger.info("[SSD Manager] The tube not configure SSD directory, SSD-Store function not start!");
            return;
        }
        this.ssdBaseDataDir = new File(this.secdStorePath);
        FileUtil.checkDir(this.ssdBaseDataDir);
        startTime = System.currentTimeMillis();
    }

    /***
     * Load ssd stores.
     *
     * @throws IOException
     */
    public void loadSSDStores() throws IOException {
        try {
            this.loadDataDir(tubeConfig);
            this.statusCheckExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    final StringBuilder strBuffer = new StringBuilder(512);
                    while (!closed) {
                        try {
                            flushCurrSegments(strBuffer);
                        } catch (Throwable e) {
                            logger.error("[SSD Manager] Error during SSD File status check", e);
                        }
                        ThreadUtils.sleep(30000);
                    }
                    logger.warn("[SSD Manager] SSD File scan thread is out!");
                }
            });

            this.reqExecutor.execute(new SsdStoreRunner());
        } catch (final IOException e) {
            logger.error("[SSD Manager] load SSD data files failed", e);
            throw new StartupException("Initialize SSD data files failed", e);
        } catch (Throwable e) {
            Thread.currentThread().interrupt();
        }
    }

    public boolean isSsdServiceInUse() {
        return (this.isStart && !this.closed);
    }

    /***
     * Request ssd transfer to disk.
     *
     * @param partStr
     * @param storeKey
     * @param startOffset
     * @param segCnt
     * @return
     */
    public boolean requestSsdTransfer(final String partStr, final String storeKey,
                                      final long startOffset, final int segCnt) {
        if (this.isStart && !this.closed) {
            try {
                this.reqSSDEvents.put(new SSDSegEvent(partStr, storeKey, startOffset, segCnt));
                return true;
            } catch (Throwable e1) {
                logger.warn("[SSD Store] request SSD Event failure : ", e1);
            }
        }
        return false;
    }

    /***
     * Get messages from ssd.
     *
     * @param storeKey
     * @param partStr
     * @param startDataOffset
     * @param lastRDOffset
     * @param partitionId
     * @param reqOffset
     * @param indexBuffer
     * @param msgDataSizeLimit
     * @param statisKeyBase
     * @return
     */
    public GetMessageResult getMessages(final String storeKey, final String partStr,
                                        final long startDataOffset, final long lastRDOffset,
                                        final int partitionId, final long reqOffset,
                                        final ByteBuffer indexBuffer, int msgDataSizeLimit,
                                        final String statisKeyBase) {
        final StringBuilder strBuffer = new StringBuilder(512);
        ConcurrentHashMap<Long, MsgSSDSegment> msgSSDSegmentMap =
                ssdSegmentsMap.get(storeKey);
        Map<String, ConsumerNodeInfo> consumerNodeInfoMap =
                msgStoreMgr.getTubeBroker().getBrokerServiceServer().getConsumerRegisterMap();
        ConsumerNodeInfo consumerNodeInfo = consumerNodeInfoMap.get(partStr);
        if (consumerNodeInfo == null) {
            return new GetMessageResult(false, TErrCodeConstants.INTERNAL_SERVER_ERROR,
                    reqOffset, 0, "Consumer unregistered!");
        }
        if (msgSSDSegmentMap == null) {
            logger.warn(strBuffer.append("[SSD Store] Found consumer visit but file expired! storeKey=")
                    .append(storeKey).toString());
            consumerNodeInfo.resetSSDProcSeg(false);
            return new GetMessageResult(false,
                    TErrCodeConstants.INTERNAL_SERVER_ERROR, reqOffset, 0, "SSD file has expired!");
        }
        MsgSSDSegment msgSsdSegment =
                msgSSDSegmentMap.get(startDataOffset);
        if (msgSsdSegment == null) {
            consumerNodeInfo.resetSSDProcSeg(false);
            logger.warn(strBuffer
                    .append("[SSD Store] Found consumer visit but file expired! storeKey=")
                    .append(storeKey).append(",startDataOffset=")
                    .append(startDataOffset).toString());
            return new GetMessageResult(false,
                    TErrCodeConstants.INTERNAL_SERVER_ERROR, reqOffset, 0, "SSD file has expired!");
        }
        GetMessageResult getMessageResult =
                msgSsdSegment.getMessages(partStr, partitionId, reqOffset, lastRDOffset,
                        indexBuffer, false, null, statisKeyBase, msgDataSizeLimit, strBuffer);
        if (getMessageResult.retCode == TErrCodeConstants.INTERNAL_SERVER_ERROR_MSGSET_NULL) {
            consumerNodeInfo.resetSSDProcSeg(false);
            try {
                msgSsdSegment.close();
                msgSSDSegmentMap.remove(startDataOffset);
                logger.warn(strBuffer
                        .append("[SSD Store] Found SSD file's fileRecordView is null release! storeKey=")
                        .append(storeKey).append(",startDataOffset=")
                        .append(startDataOffset).toString());
            } catch (Throwable ee) {
                strBuffer.delete(0, strBuffer.length());
                logger.warn(strBuffer.append("[SSD Store] release SSD FileSegment failure, storeKey=")
                        .append(storeKey).append(",startDataOffset=")
                        .append(startDataOffset).toString(), ee);
            }
        }
        if (consumerNodeInfo.getLastDataRdOffset() >= msgSsdSegment.getDataMaxOffset()) {
            msgSsdSegment.closeConsumeRel(partStr);
            consumerNodeInfo.resetSSDProcSeg(false);
        }
        return getMessageResult;
    }

    @Override
    public void close() {
        this.closed = true;
        try {
            this.reqExecutor.shutdown();
            this.statusCheckExecutor.shutdown();
            for (Map<Long, MsgSSDSegment> msgSSDSegmentMap : ssdSegmentsMap.values()) {
                if (msgSSDSegmentMap != null) {
                    for (MsgSSDSegment msgSsdSegment : msgSSDSegmentMap.values()) {
                        if (msgSsdSegment != null) {
                            msgSsdSegment.close();
                        }
                    }
                }
            }
        } catch (Throwable e1) {
            logger.warn("[SSD Store] Close SSD Store manager failure", e1);
        }
    }

    private boolean flushCurrSegments(final StringBuilder sb) throws IOException {
        int totalCnt = 0;
        long currTime = System.currentTimeMillis();
        if ((firstChecked.get() > 0 && currTime - startTime <= 2 * 60 * 1000)
                || (firstChecked.get() == 0
                && totalSsdFileCnt.get() < (int) (tubeConfig.getMaxSSDTotalFileCnt() * 0.7)
                && totalSsdFileSize.get() < (long) (tubeConfig.getMaxSSDTotalFileSizes() * 0.7))) {
            return true;
        }
        Set<SSDSegIndex> msgSsdIndexSet = new HashSet<SSDSegIndex>();
        for (Map.Entry<String, ConcurrentHashMap<Long, MsgSSDSegment>> entry : ssdSegmentsMap.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            ConcurrentHashMap<Long, MsgSSDSegment> msgSegmentMap = entry.getValue();
            for (Map.Entry<Long, MsgSSDSegment> subEntry : msgSegmentMap.entrySet()) {
                if (subEntry.getValue() != null && !subEntry.getValue().isSegmentValid()) {
                    msgSsdIndexSet.add(new SSDSegIndex(entry.getKey(),
                            subEntry.getValue().getDataSegment().getStart(),
                            subEntry.getValue().getDataMaxOffset()));
                }
            }
        }
        if (msgSsdIndexSet.isEmpty()) {
            if (firstChecked.get() > 0) {
                firstChecked.set(0);
            }
            return true;
        }
        for (SSDSegIndex ssdSegIndex : msgSsdIndexSet) {
            ConcurrentHashMap<Long, MsgSSDSegment> tmpMsgSSdSegmentMap =
                    ssdSegmentsMap.get(ssdSegIndex.storeKey);
            if (tmpMsgSSdSegmentMap == null) {
                continue;
            }
            MsgSSDSegment msgSsdSegment =
                    tmpMsgSSdSegmentMap.get(ssdSegIndex.startOffset);
            if (msgSsdSegment == null) {
                continue;
            }
            if (msgSsdSegment.setAndGetSegmentExpired()) {
                inProcessExpiredFile(msgSsdSegment, ssdSegIndex, tmpMsgSSdSegmentMap, sb);
            } else {
                inProcessInUseFile(msgSsdSegment);
            }
        }
        if (firstChecked.get() > 0) {
            totalCnt = firstChecked.get();
            firstChecked.compareAndSet(totalCnt, totalCnt - 1);
        }
        return true;
    }

    private void inProcessInUseFile(MsgSSDSegment msgSsdSegment) {
        if (msgSsdSegment.getCurStatus() != 1) {
            return;
        }
        try {
            Map<String, ConsumerNodeInfo> consumerNodeInfoMap =
                    msgStoreMgr.getTubeBroker().getBrokerServiceServer().getConsumerRegisterMap();
            ConcurrentHashMap<String, SSDVisitInfo> ssdVisitInfoMap =
                    msgSsdSegment.getVisitMap();
            if (ssdVisitInfoMap != null) {
                List<String> rmvPartStrList = new ArrayList<String>();
                for (String partStr : ssdVisitInfoMap.keySet()) {
                    ConsumerNodeInfo consumerNodeInfo =
                            consumerNodeInfoMap.get(partStr);
                    if (consumerNodeInfo == null) {
                        rmvPartStrList.add(partStr);
                        continue;
                    }
                    if (msgSsdSegment
                            .getOffsetPos(consumerNodeInfo.getLastDataRdOffset()) > 0) {
                        consumerNodeInfo.resetSSDProcSeg(true);
                        rmvPartStrList.add(partStr);
                    }
                }
                if (!rmvPartStrList.isEmpty()) {
                    for (String partStr : rmvPartStrList) {
                        msgSsdSegment.closeConsumeRel(partStr);
                    }
                }
            }
        } catch (Throwable e2) {
            logger.warn("[SSD Manager] Check consumerInfo and SSD file relation failure", e2);
        }
    }

    /***
     * Delete expired files.
     *
     * @param msgSsdSegment
     * @param ssdSegIndex
     * @param msgSsdSegmentMap
     * @param strBuffer
     */
    private void inProcessExpiredFile(MsgSSDSegment msgSsdSegment, SSDSegIndex ssdSegIndex,
                                      Map<Long, MsgSSDSegment> msgSsdSegmentMap, StringBuilder strBuffer) {
        try {
            File file = msgSsdSegment.getDataSegment().getFile();
            logger.info(strBuffer.append("[SSD Manager] delete ssd segment : ")
                    .append(file.getAbsolutePath()).toString());
            strBuffer.delete(0, strBuffer.length());
            final long msgSize = msgSsdSegment.getDataSegment().getCachedSize();
            for (Map.Entry<String, ConcurrentHashSet<SSDSegIndex>> entry : ssdPartStrMap.entrySet()) {
                if (entry.getValue() != null) {
                    entry.getValue().remove(ssdSegIndex);
                }
            }
            msgSsdSegmentMap.remove(ssdSegIndex.startOffset);
            msgSsdSegment.close();
            file.delete();
            int exptCnt = totalSsdFileCnt.get();
            while (!totalSsdFileCnt.compareAndSet(exptCnt, exptCnt - 1)) {
                exptCnt = totalSsdFileCnt.get();
            }
            long exptSize = totalSsdFileSize.get();
            while (!totalSsdFileSize.compareAndSet(exptSize, exptSize - msgSize)) {
                exptSize = totalSsdFileSize.get();
            }
            logger.info(strBuffer.append("[SSD Manager] rmv SSD FileSegment finished, totalSsdFileCnt=")
                    .append(totalSsdFileCnt.get()).append(",totalSsdFileSize=")
                    .append(totalSsdFileSize.get()).toString());
            strBuffer.delete(0, strBuffer.length());
        } catch (Throwable e1) {
            logger.warn("[SSD Manager] Delete SSD expired file failure", e1);
        }
    }

    /***
     * Load data from directory.
     *
     * @param tubeConfig
     * @throws IOException
     */
    private void loadDataDir(final BrokerConfig tubeConfig) throws IOException {
        StringBuilder strBuffer = new StringBuilder(512);
        final long startTime = System.currentTimeMillis();
        logger.info(strBuffer.append("[SSD Manager] Begin to scan data path:")
                .append(this.ssdBaseDataDir.getAbsolutePath()).toString());
        strBuffer.delete(0, strBuffer.length());
        MetadataManage metadataManage = msgStoreMgr.getMetadataManage();
        final File[] ls = this.ssdBaseDataDir.listFiles();
        if (ls != null) {
            for (final File subDir : ls) {
                if (subDir == null) {
                    continue;
                }
                if (!subDir.isDirectory()) {
                    logger.warn(strBuffer.append("[SSD Manager] Ignore not directory path:")
                            .append(subDir.getAbsolutePath()).toString());
                    strBuffer.delete(0, strBuffer.length());
                } else {
                    final String name = subDir.getName();
                    final int index = name.lastIndexOf('-');
                    if (index < 0) {
                        logger.warn(strBuffer.append("[SSD Manager] Ignore invalid directory:")
                                .append(subDir.getAbsolutePath()).toString());
                        strBuffer.delete(0, strBuffer.length());
                        continue;
                    }
                    final String topic = name.substring(0, index);
                    TopicMetadata topicMetadata = metadataManage.getTopicMetadata(topic);
                    if (topicMetadata == null) {
                        logger.warn(strBuffer
                                .append("[SSD Manager] No valid topic config for topic data directories:")
                                .append(topic).toString());
                        strBuffer.delete(0, strBuffer.length());
                        continue;
                    }
                    final int storeId = Integer.parseInt(name.substring(index + 1));
                    final String storeKey =
                            strBuffer.append(topic).append("-").append(storeId).toString();
                    strBuffer.delete(0, strBuffer.length());
                    loadSsdFile(storeKey, topic, strBuffer);
                }
            }
        }
        logger.info(strBuffer.append("[SSD Manager] End to scan SSD data path in ")
                .append((System.currentTimeMillis() - startTime) / 1000)
                .append(" secs").toString());
    }

    /***
     * Copy files on disk to ssd.
     *
     * @param storeKey
     * @param topic
     * @param partStr
     * @param fromFile
     * @param startOffset
     * @param endOffset
     * @param sb
     * @return
     */
    private boolean copyFileToSSD(final String storeKey, final String topic,
                                  final String partStr, File fromFile,
                                  final long startOffset, final long endOffset,
                                  final StringBuilder sb) {
        // #lizard forgives
        logger.info(sb.append("[SSD Manager] Begin to copy file to SSD, source data path:")
                .append(fromFile.getAbsolutePath()).toString());
        sb.delete(0, sb.length());
        if (!fromFile.exists()) {
            logger.warn(sb.append("[SSD Manager] source file not exists, path:")
                    .append(fromFile.getAbsolutePath()).toString());
            sb.delete(0, sb.length());
            return false;
        }
        if (!fromFile.isFile()) {
            logger.warn(sb.append("[SSD Manager] source file not File, path:")
                    .append(fromFile.getAbsolutePath()).toString());
            sb.delete(0, sb.length());
            return false;
        }
        if (!fromFile.canRead()) {
            logger.warn(sb.append("[SSD Manager] source file not canRead, path:")
                    .append(fromFile.getAbsolutePath()).toString());
            sb.delete(0, sb.length());
            return false;
        }
        boolean isNew = false;
        final String filename = fromFile.getName();
        final long start =
                Long.parseLong(filename.substring(0, filename.length() - DATA_FILE_SUFFIX.length()));
        final File toFileDir =
                new File(sb.append(secdStorePath)
                        .append(File.separator).append(storeKey).toString());
        sb.delete(0, sb.length());
        FileUtil.checkDir(toFileDir);
        ConcurrentHashMap<Long, MsgSSDSegment> msgSsdSegMap =
                ssdSegmentsMap.get(storeKey);
        if (msgSsdSegMap == null) {
            ConcurrentHashMap<Long, MsgSSDSegment> tmpMsgSsdSegMap =
                    new ConcurrentHashMap<Long, MsgSSDSegment>();
            msgSsdSegMap = ssdSegmentsMap.putIfAbsent(storeKey, tmpMsgSsdSegMap);
            if (msgSsdSegMap == null) {
                msgSsdSegMap = tmpMsgSsdSegMap;
            }
        }
        MsgSSDSegment msgSsdSegment1 = msgSsdSegMap.get(start);
        if (msgSsdSegment1 == null) {
            boolean isSuccess = false;
            FileInputStream fosfrom = null;
            FileOutputStream fosto = null;
            final File targetFile =
                    new File(toFileDir, DataStoreUtils.nameFromOffset(start, DATA_FILE_SUFFIX));
            try {
                fosfrom = new FileInputStream(fromFile);
                fosto = new FileOutputStream(targetFile);
                byte[] bt = new byte[65536];
                int c;
                while ((c = fosfrom.read(bt)) > 0) {
                    fosto.write(bt, 0, c);
                }
                isSuccess = true;
                isNew = true;
            } catch (FileNotFoundException e) {
                logger.warn("[SSD Manager] copy File to SSD FileNotFoundException failure ", e);
            } catch (IOException e) {
                logger.warn("[SSD Manager] copy File to SSD IOException failure ", e);
            } finally {
                if (fosfrom != null) {
                    try {
                        fosfrom.close();
                    } catch (Throwable e2) {
                        logger.warn("[SSD Manager] Close FileInputStream failure! ", e2);
                    }
                }
                if (fosto != null) {
                    try {
                        fosto.close();
                    } catch (Throwable e2) {
                        logger.warn("[SSD Manager] Close FileOutputStream failure! ", e2);
                    }
                }
            }
            if (!isSuccess) {
                try {
                    targetFile.delete();
                } catch (Throwable e2) {
                    logger.warn("[SSD Manager] remove SSD target file failure ", e2);
                }
                return false;
            }
            try {
                MsgSSDSegment tmpMsgSsdSegment =
                        new MsgSSDSegment(storeKey, topic, start, targetFile);
                if (tmpMsgSsdSegment.getDataMinOffset() != startOffset
                        || tmpMsgSsdSegment.getDataMaxOffset() != endOffset) {
                    logger.warn(sb
                            .append("[SSD Manager] created SSD file getCachedSize not equal source: sourceFile=")
                            .append(fromFile.getAbsolutePath()).append(",srcStartOffset=")
                            .append(startOffset).append(",srcEndOffset=").append(endOffset)
                            .append(",dstStartOffset=").append(tmpMsgSsdSegment.getDataMinOffset())
                            .append(",dstEndOffset=").append(tmpMsgSsdSegment.getDataMaxOffset()).toString());
                    sb.delete(0, sb.length());
                    tmpMsgSsdSegment.close();
                    return false;
                }
                msgSsdSegment1 = msgSsdSegMap.putIfAbsent(start, tmpMsgSsdSegment);
                if (msgSsdSegment1 == null) {
                    msgSsdSegment1 = tmpMsgSsdSegment;
                    this.totalSsdFileSize.addAndGet(tmpMsgSsdSegment.getDataSizeInBytes());
                    this.totalSsdFileCnt.incrementAndGet();
                } else {
                    tmpMsgSsdSegment.close();
                }
            } catch (Throwable e1) {
                logger.warn("[SSD Manager] create SSD FileSegment failure", e1);
                return false;
            }
        }
        ConcurrentHashSet<SSDSegIndex> ssdSegIndices = this.ssdPartStrMap.get(partStr);
        if (ssdSegIndices == null) {
            ConcurrentHashSet<SSDSegIndex> tmpSsdSegIndices = new ConcurrentHashSet<SSDSegIndex>();
            ssdSegIndices = this.ssdPartStrMap.putIfAbsent(partStr, tmpSsdSegIndices);
            if (ssdSegIndices == null) {
                ssdSegIndices = tmpSsdSegIndices;
            }
        }
        SSDSegIndex ssdSegIndex =
                new SSDSegIndex(storeKey, start, msgSsdSegment1.getDataMaxOffset());
        if (!ssdSegIndices.contains(ssdSegIndex)) {
            ssdSegIndices.add(ssdSegIndex);
        }
        logger.info(sb.append("[SSD Manager] copy file to SSD success, data segment ")
                .append(msgSsdSegment1.getStoreKey()).append(",start=").append(start)
                .append(", isNew=").append(isNew).toString());
        sb.delete(0, sb.length());
        return true;
    }

    private void checkSegmentTransferStatus(final String topicName, final SSDSegEvent ssdSegEvent,
                                            final SSDSegFound ssdSegFound, final ConsumerNodeInfo consumerNodeInfo,
                                            final StringBuilder strBuffer) {
        consumerNodeInfo.setSSDProcing();
        ConcurrentHashMap<Long, MsgSSDSegment> msgSegmentMap =
                ssdSegmentsMap.get(ssdSegEvent.storeKey);
        if (msgSegmentMap == null) {
            ConcurrentHashMap<Long, MsgSSDSegment> tmpMsgSsdSegMap =
                    new ConcurrentHashMap<Long, MsgSSDSegment>();
            msgSegmentMap =
                    ssdSegmentsMap.putIfAbsent(ssdSegEvent.storeKey, tmpMsgSsdSegMap);
            if (msgSegmentMap == null) {
                msgSegmentMap = tmpMsgSsdSegMap;
            }
        }
        MsgSSDSegment curMsgSsdSegment =
                msgSegmentMap.get(ssdSegFound.startOffset);
        if (curMsgSsdSegment != null) {
            curMsgSsdSegment.setInUse();
            ConcurrentHashSet<SSDSegIndex> ssdSegIndices =
                    ssdPartStrMap.get(ssdSegEvent.partStr);
            if (ssdSegIndices == null) {
                ConcurrentHashSet<SSDSegIndex> tmpSsdSegIndices =
                        new ConcurrentHashSet<SSDSegIndex>();
                ssdSegIndices =
                        ssdPartStrMap.putIfAbsent(ssdSegEvent.partStr, tmpSsdSegIndices);
                if (ssdSegIndices == null) {
                    ssdSegIndices = tmpSsdSegIndices;
                }
            }
            ssdSegIndices.add(new SSDSegIndex(ssdSegEvent.storeKey,
                    ssdSegFound.startOffset, ssdSegFound.endOffset));
            consumerNodeInfo.setSSDTransferFinished(true,
                    ssdSegFound.startOffset, ssdSegFound.endOffset);
        } else {
            if (totalSsdFileCnt.get() >= tubeConfig.getMaxSSDTotalFileCnt()
                    || totalSsdFileSize.get() >= tubeConfig.getMaxSSDTotalFileSizes()) {
                consumerNodeInfo.setSSDTransferFinished(false, -2, -2);
                return;
            }
            boolean result = copyFileToSSD(ssdSegEvent.storeKey, topicName,
                    ssdSegEvent.partStr, ssdSegFound.sourceFile,
                    ssdSegFound.startOffset, ssdSegFound.endOffset, strBuffer);
            if (result) {
                consumerNodeInfo.setSSDTransferFinished(true,
                        ssdSegFound.startOffset, ssdSegFound.endOffset);
            } else {
                consumerNodeInfo.setSSDTransferFinished(false,
                        ssdSegFound.startOffset, ssdSegFound.endOffset);
            }
        }
    }

    private void loadSsdFile(final String storeKey, final String topicName,
                             final StringBuilder strBuffer) throws IOException {
        final File storeKeydataDir =
                new File(strBuffer.append(this.secdStorePath)
                        .append(File.separator).append(storeKey).toString());
        strBuffer.delete(0, strBuffer.length());
        final File[] lsData = storeKeydataDir.listFiles();
        if (lsData == null) {
            return;
        }
        for (final File fileData : lsData) {
            if (fileData != null
                    && fileData.isFile()
                    && fileData.toString().endsWith(DATA_FILE_SUFFIX)) {
                if (!fileData.canRead()) {
                    throw new IOException(new StringBuilder(512)
                            .append("[SSD Manager] Could not read data file ")
                            .append(fileData).toString());
                }
                final String filename = fileData.getName();
                final long start =
                        Long.parseLong(filename.substring(0,
                                filename.length() - DATA_FILE_SUFFIX.length()));
                ConcurrentHashMap<Long, MsgSSDSegment> msgSsdSegMap =
                        this.ssdSegmentsMap.get(storeKey);
                if (msgSsdSegMap == null) {
                    ConcurrentHashMap<Long, MsgSSDSegment> tmpMsgSsdSegMap =
                            new ConcurrentHashMap<Long, MsgSSDSegment>();
                    msgSsdSegMap =
                            ssdSegmentsMap.putIfAbsent(storeKey, tmpMsgSsdSegMap);
                    if (msgSsdSegMap == null) {
                        msgSsdSegMap = tmpMsgSsdSegMap;
                    }
                }
                MsgSSDSegment msgSsdSegment1 = msgSsdSegMap.get(start);
                if (msgSsdSegment1 == null) {
                    MsgSSDSegment tmpMsgSsdSegment =
                            new MsgSSDSegment(storeKey, topicName, start, fileData);
                    msgSsdSegment1 =
                            msgSsdSegMap.putIfAbsent(start, tmpMsgSsdSegment);
                    if (msgSsdSegment1 == null) {
                        totalSsdFileSize
                                .addAndGet(tmpMsgSsdSegment.getDataSizeInBytes());
                        totalSsdFileCnt.incrementAndGet();
                    } else {
                        tmpMsgSsdSegment.close();
                    }
                }
                logger.info(strBuffer.append("[SSD Manager] Loaded data segment ")
                        .append(fileData.getAbsolutePath()).toString());
                strBuffer.delete(0, strBuffer.length());
            }
        }
    }

    private class SsdStoreRunner implements Runnable {

        public SsdStoreRunner() {
            //
        }

        @Override
        public void run() {
            final StringBuilder strBuffer = new StringBuilder(512);
            logger.info("[SSD Manager] start process SSD  transfer requests");
            try {
                MetadataManage metadataManage = msgStoreMgr.getMetadataManage();
                while (!closed) {
                    try {
                        SSDSegEvent ssdSegEvent =
                                reqSSDEvents.poll(500, TimeUnit.MILLISECONDS);
                        if (ssdSegEvent == null) {
                            ThreadUtils.sleep(1000);
                            continue;
                        }
                        Map<String, ConsumerNodeInfo> consumerNodeInfoMap =
                                msgStoreMgr.getTubeBroker().getBrokerServiceServer().getConsumerRegisterMap();
                        ConsumerNodeInfo consumerNodeInfo =
                                consumerNodeInfoMap.get(ssdSegEvent.partStr);
                        if (consumerNodeInfo == null) {
                            continue;
                        }
                        final int index = ssdSegEvent.storeKey.lastIndexOf('-');
                        if (index < 0) {
                            logger.warn(strBuffer.append("[SSD Manager] Ignore invalid storeKey=")
                                    .append(ssdSegEvent.storeKey).toString());
                            strBuffer.delete(0, strBuffer.length());
                            continue;
                        }
                        final String topic = ssdSegEvent.storeKey.substring(0, index);
                        TopicMetadata topicMetadata =
                                metadataManage.getTopicMetadata(topic);
                        if (topicMetadata == null) {
                            logger.warn(strBuffer
                                    .append("[SSD Manager] No valid topic config for storeKey=")
                                    .append(ssdSegEvent.storeKey).toString());
                            strBuffer.delete(0, strBuffer.length());
                            continue;
                        }
                        final int storeId =
                                Integer.parseInt(ssdSegEvent.storeKey.substring(index + 1));
                        SSDSegFound ssdSegFound =
                                msgStoreMgr.getSourceSegment(topic, storeId, ssdSegEvent.startOffset, 15);
                        if (!ssdSegFound.isSuccess) {
                            if (ssdSegFound.reason > 0) {
                                logger.warn(strBuffer
                                        .append("[SSD Manager] Found source store error, storeKey=")
                                        .append(ssdSegEvent.storeKey).append(", reason=")
                                        .append(ssdSegFound.reason).toString());
                                strBuffer.delete(0, strBuffer.length());
                            }
                            consumerNodeInfo.setSSDTransferFinished(false,
                                    ssdSegFound.startOffset, ssdSegFound.endOffset);
                            continue;
                        }
                        checkSegmentTransferStatus(topic, ssdSegEvent,
                                ssdSegFound, consumerNodeInfo, strBuffer);
                    } catch (InterruptedException e) {
                        break;
                    } catch (Throwable e2) {
                        logger.error("[SSD Manager] process SSD  transfer request failure", e2);
                    }
                }
                logger.info("[SSD Manager] stopped process SSD  transfer requests");
            } catch (Throwable e) {
                logger.error("[SSD Manager] Error during process SSD transfer requests", e);
            }
        }
    }

}

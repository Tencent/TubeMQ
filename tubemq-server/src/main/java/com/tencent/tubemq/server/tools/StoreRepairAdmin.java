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

package com.tencent.tubemq.server.tools;

import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.server.broker.msgstore.disk.FileSegment;
import com.tencent.tubemq.server.broker.msgstore.disk.FileSegmentList;
import com.tencent.tubemq.server.broker.msgstore.disk.RecordView;
import com.tencent.tubemq.server.broker.msgstore.disk.Segment;
import com.tencent.tubemq.server.broker.msgstore.disk.SegmentList;
import com.tencent.tubemq.server.broker.msgstore.disk.SegmentType;
import com.tencent.tubemq.server.broker.utils.DataStoreUtils;
import com.tencent.tubemq.server.common.utils.FileUtil;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StoreRepairAdmin {
    private static final Logger logger =
            LoggerFactory.getLogger(StoreRepairAdmin.class);

    public static void main(final String[] args) throws Exception {
        if (args == null || args.length < 1) {
            System.out.println(
                    "[Data Repair] Please input 1 params : storePath [ topicA,topicB,....]");
            return;
        }
        List<String> topicList = null;
        final String storePath = args[0];
        if (args.length > 1) {
            if (args[1] != null && TStringUtils.isNotBlank(args[1])) {
                topicList = Arrays.asList(args[1].split(","));
            }
        }
        final File storeDir = new File(storePath);
        if (!storeDir.exists()) {
            throw new RuntimeException(new StringBuilder(512)
                    .append("[Data Repair] store path is not existed, path is ")
                    .append(storePath).toString());
        }
        if (!storeDir.isDirectory()) {
            throw new RuntimeException(new StringBuilder(512)
                    .append("[Data Repair]  store path is not a directory, path is ")
                    .append(storePath).toString());
        }
        final long start = System.currentTimeMillis();
        final File[] ls = storeDir.listFiles();
        if (topicList == null || topicList.isEmpty()) {
            logger.warn(new StringBuilder(512)
                    .append("[Data Repair] Begin to scan store path: ")
                    .append(storePath).toString());
        } else {
            logger.warn(new StringBuilder(512)
                    .append("[Data Repair] Begin to scan store path: ")
                    .append(storePath).append(",topicList:")
                    .append(topicList.toString()).toString());
        }
        int count = 0;
        ExecutorService executor =
                Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);
        List<Callable<IndexReparStore>> tasks = new ArrayList<Callable<IndexReparStore>>();
        if (ls != null) {
            for (final File subDir : ls) {
                if (subDir == null) {
                    continue;
                }
                if (subDir.isDirectory()) {
                    final String name = subDir.getName();
                    final int index = name.lastIndexOf('-');
                    if (index < 0) {
                        continue;
                    }
                    final String topic = name.substring(0, index);
                    if (topicList != null && !topicList.isEmpty()) {
                        if (!topicList.contains(topic)) {
                            continue;
                        }
                    }
                    final int storeId = Integer.parseInt(name.substring(index + 1));
                    tasks.add(new Callable<IndexReparStore>() {
                        @Override
                        public IndexReparStore call() throws Exception {
                            StringBuilder sBuilder = new StringBuilder(512);
                            logger.info(sBuilder.append("[Data Repair] Loading data directory:")
                                    .append(subDir.getAbsolutePath()).append("...").toString());
                            sBuilder.delete(0, sBuilder.length());
                            final IndexReparStore messageStore =
                                    new IndexReparStore(storePath, topic, storeId);
                            messageStore.reCreateIndexFiles();
                            logger.info(sBuilder.append("[Data Repair] Finished data index recreation :")
                                    .append(subDir.getAbsolutePath()).toString());
                            return messageStore;
                        }
                    });
                    count++;
                }
            }
        }
        if (count > 0) {
            CompletionService<IndexReparStore> completionService =
                    new ExecutorCompletionService<IndexReparStore>(executor);
            for (Callable<IndexReparStore> task : tasks) {
                completionService.submit(task);
            }
            for (int i = 0; i < tasks.size(); i++) {
                IndexReparStore messageStore =
                        completionService.take().get();
                if (messageStore != null) {
                    messageStore.close();
                }
            }
            tasks.clear();
        }
        executor.shutdown();
        try {
            executor.awaitTermination(30 * 1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            //
        }
        logger.warn(new StringBuilder(512)
                .append("[Data Repair] End to scan data path in ")
                .append((System.currentTimeMillis() - start) / 1000)
                .append(" secs").toString());
        System.exit(0);
    }

    private static class IndexReparStore implements Closeable {
        private static final String DATA_SUFFIX = ".tube";
        private static final String INDEX_SUFFIX = ".index";
        private static final int ONE_M_BYTES = 10 * 1024 * 1024;
        private final String topic;
        private final int storeId;
        private final String basePath;
        private final String topicKey;
        private final String storePath;
        private final String indexPath;
        private final File topicDir;
        private final File indexDir;
        private int maxIndexSegmentSize =
                500000 * DataStoreUtils.STORE_INDEX_HEAD_LEN;
        private SegmentList segments;

        public IndexReparStore(final String basePath,
                               final String topic,
                               final int storeId) {
            this.basePath = basePath;
            this.topic = topic;
            this.storeId = storeId;
            StringBuilder sBuilder = new StringBuilder(512);
            this.topicKey =
                    sBuilder.append(this.topic).append("-").append(this.storeId).toString();
            sBuilder.delete(0, sBuilder.length());
            this.storePath = sBuilder.append(this.basePath)
                    .append(File.separator).append(this.topicKey).toString();
            sBuilder.delete(0, sBuilder.length());
            this.indexPath = sBuilder.append(this.basePath)
                    .append(File.separator).append(this.topicKey)
                    .append(File.separator).append("index").toString();
            sBuilder.delete(0, sBuilder.length());
            this.topicDir = new File(storePath);
            this.indexDir = new File(indexPath);
        }

        public void reCreateIndexFiles() {
            final StringBuilder sBuilder = new StringBuilder(512);
            try {
                loadDataSegments(sBuilder);
                deleteIndexFiles(sBuilder);
                createIndexFiles();
            } catch (Throwable ee) {
                sBuilder.delete(0, sBuilder.length());
                logger.error(sBuilder.append("ReCreate Index File of ")
                        .append(this.topicKey).append(" error ").toString(), ee);
            }
        }

        private void loadDataSegments(final StringBuilder sBuilder) throws IOException {
            if (!topicDir.exists()) {
                throw new RuntimeException(sBuilder
                        .append("[Data Repair] Topic data path is not existed, path is ")
                        .append(storePath).toString());
            }
            if (!topicDir.isDirectory()) {
                throw new RuntimeException(sBuilder
                        .append("[Data Repair]  Topic data path is not a directory, path is ")
                        .append(storePath).toString());
            }
            segments = new FileSegmentList(topicDir,
                    SegmentType.DATA, false, -1, Long.MAX_VALUE, sBuilder);
        }

        @Override
        public void close() throws IOException {
            try {
                if (this.segments != null) {
                    for (final Segment segment : this.segments.getView()) {
                        segment.close();
                    }
                }
            } catch (Throwable e3) {
                logger.error("[Data Repair] Close data segments error", e3);
            }
        }

        private void deleteIndexFiles(final StringBuilder sBuilder) {
            if (indexDir.exists()) {
                if (!indexDir.isDirectory()) {
                    throw new RuntimeException(sBuilder
                            .append("[Data Repair] Topic index path is not a directory, path is ")
                            .append(indexPath).toString());
                }
                try {
                    FileUtil.fullyDeleteContents(indexDir);
                } catch (Throwable er) {
                    logger.error(sBuilder
                            .append("[Data Repair] delete index files error, path is ")
                            .append(indexPath).toString(), er);
                }
            } else {
                if (!indexDir.mkdirs()) {
                    throw new RuntimeException(sBuilder
                            .append("[Data Repair] Could not make index directory ")
                            .append(indexPath).toString());
                }
                if (!indexDir.isDirectory() || !indexDir.canRead()) {
                    throw new RuntimeException(sBuilder.append("[Data Repair] Index path ")
                            .append(indexPath).append(" is not a readable directory").toString());
                }
            }
        }

        private void createIndexFiles() {
            final List<Segment> segments = this.segments.getView();
            if (segments == null || segments.isEmpty()) {
                return;
            }
            long gQueueOffset = -1;
            Segment curPartSeg = null;
            final ByteBuffer dataBuffer = ByteBuffer.allocate(ONE_M_BYTES);
            final ByteBuffer indexBuffer =
                    ByteBuffer.allocate(DataStoreUtils.STORE_INDEX_HEAD_LEN);
            for (int index = 0; index < segments.size(); index++) {
                Segment curSegment = segments.get(index);
                if (curSegment == null) {
                    continue;
                }
                RecordView recordView = null;
                try {
                    long curOffset = 0L;
                    recordView =
                            curSegment.getViewRef(curSegment.getStart(),
                                    0, curSegment.getCachedSize());
                    while (curOffset < curSegment.getCachedSize()) {
                        dataBuffer.clear();
                        recordView.read(dataBuffer, curOffset);
                        dataBuffer.flip();
                        int dataStart = 0;
                        int dataRealLimit = dataBuffer.limit();
                        for (dataStart = 0; dataStart < dataRealLimit; ) {
                            if (dataRealLimit - dataStart < DataStoreUtils.STORE_DATA_HEADER_LEN) {
                                dataStart += DataStoreUtils.STORE_DATA_HEADER_LEN;
                                break;
                            }
                            final int msgLen =
                                    dataBuffer.getInt(dataStart + DataStoreUtils.STORE_HEADER_POS_LENGTH);
                            final int msgToken =
                                    dataBuffer.getInt(dataStart + DataStoreUtils.STORE_HEADER_POS_DATATYPE);
                            if (msgToken != DataStoreUtils.STORE_DATA_TOKER_BEGIN_VALUE) {
                                dataStart += 1;
                                continue;
                            }
                            final int msgSize = msgLen + 4;
                            final long msgOffset =
                                    curSegment.getStart() + curOffset + dataStart;
                            final long queueOffset =
                                    dataBuffer.getLong(dataStart + DataStoreUtils.STORE_HEADER_POS_QUEUE_LOGICOFF);
                            final int partitionId =
                                    dataBuffer.getInt(dataStart + DataStoreUtils.STORE_HEADER_POS_QUEUEID);
                            final int keyCode =
                                    dataBuffer.getInt(dataStart + DataStoreUtils.STORE_HEADER_POS_KEYCODE);
                            final long timeRecv =
                                    dataBuffer.getLong(dataStart + DataStoreUtils.STORE_HEADER_POS_RECEIVEDTIME);
                            dataStart += msgSize;
                            indexBuffer.clear();
                            indexBuffer.putInt(partitionId);
                            indexBuffer.putLong(msgOffset);
                            indexBuffer.putInt(msgSize);
                            indexBuffer.putInt(keyCode);
                            indexBuffer.putLong(timeRecv);
                            indexBuffer.flip();
                            if (curPartSeg == null) {
                                if (gQueueOffset < 0) {
                                    gQueueOffset = queueOffset;
                                }
                                File newFile =
                                        new File(this.indexDir,
                                                DataStoreUtils.nameFromOffset(gQueueOffset, INDEX_SUFFIX));
                                curPartSeg =
                                        new FileSegment(queueOffset, newFile, SegmentType.INDEX);
                            }
                            curPartSeg.append(indexBuffer);
                            gQueueOffset += DataStoreUtils.STORE_INDEX_HEAD_LEN;
                            if (curPartSeg.getCachedSize() >= maxIndexSegmentSize) {
                                curPartSeg.flush(true);
                                curPartSeg.close();
                                curPartSeg = null;
                            }
                        }
                        curOffset += dataStart;
                    }
                    if (curPartSeg != null) {
                        curPartSeg.flush(true);
                    }
                } catch (Throwable ee) {
                    logger.error("Create Index file error ", ee);
                } finally {
                    if (recordView != null) {
                        recordView.getSegment().relViewRef();
                    }
                }
            }
            try {
                if (curPartSeg != null) {
                    curPartSeg.flush(true);
                    curPartSeg.close();
                    curPartSeg = null;
                }
            } catch (Throwable e2) {
                logger.error("Close Index file error ", e2);
            }
        }

    }

}

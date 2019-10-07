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

package com.tencent.tubemq.server.broker.msgstore;

import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.TErrCodeConstants;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.corebase.utils.ThreadUtils;
import com.tencent.tubemq.server.broker.BrokerConfig;
import com.tencent.tubemq.server.broker.TubeBroker;
import com.tencent.tubemq.server.broker.exception.StartupException;
import com.tencent.tubemq.server.broker.metadata.MetadataManage;
import com.tencent.tubemq.server.broker.metadata.TopicMetadata;
import com.tencent.tubemq.server.broker.msgstore.disk.GetMessageResult;
import com.tencent.tubemq.server.broker.msgstore.ssd.MsgSSDStoreManager;
import com.tencent.tubemq.server.broker.msgstore.ssd.SSDSegFound;
import com.tencent.tubemq.server.broker.nodeinfo.ConsumerNodeInfo;
import com.tencent.tubemq.server.broker.utils.DataStoreUtils;
import com.tencent.tubemq.server.common.TStatusConstants;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Message storage management. It contains all topics on broker. In charge of store, expire, and flush operation,
 */
public class MessageStoreManager implements StoreService {
    private static final Logger logger = LoggerFactory.getLogger(MessageStoreManager.class);
    private final BrokerConfig tubeConfig;
    private final TubeBroker tubeBroker;
    // metadata manager, get metadata from master.
    private final MetadataManage metadataManage;
    // storeId to store on each topic.
    private final ConcurrentHashMap<String/* topic */,
            ConcurrentHashMap<Integer/* storeId */, MessageStore>> dataStores =
            new ConcurrentHashMap<String, ConcurrentHashMap<Integer, MessageStore>>();
    // ssd store manager
    private final MsgSSDStoreManager msgSsdStoreManager;
    // store service status
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    // data expire operation scheduler.
    private final ScheduledExecutorService logClearScheduler;
    // flush operation scheduler.
    private final ScheduledExecutorService unFlushDiskScheduler;
    // message on memory sink to disk operation scheduler.
    private final ScheduledExecutorService unFlushMemkScheduler;
    // max transfer size.
    private int maxMsgTransferSize;
    // the status that is deleting topic.
    private AtomicBoolean isRemovingTopic = new AtomicBoolean(false);


    public MessageStoreManager(final TubeBroker tubeBroker,
                               final BrokerConfig tubeConfig) throws IOException {
        super();
        this.tubeConfig = tubeConfig;
        this.tubeBroker = tubeBroker;
        this.metadataManage = this.tubeBroker.getMetadataManage();
        this.isRemovingTopic.set(false);
        this.maxMsgTransferSize =
                tubeConfig.getTransferSize() > DataStoreUtils.MAX_MSG_TRANSFER_SIZE
                        ? DataStoreUtils.MAX_MSG_TRANSFER_SIZE : tubeConfig.getTransferSize();
        this.msgSsdStoreManager = new MsgSSDStoreManager(this, this.tubeConfig);
        this.metadataManage.addPropertyChangeListener("topicConfigMap", new PropertyChangeListener() {
            @Override
            public void propertyChange(final PropertyChangeEvent evt) {
                Map<String, TopicMetadata> oldTopicConfigMap
                        = (Map<String, TopicMetadata>) evt.getOldValue();
                Map<String, TopicMetadata> newTopicConfigMap
                        = (Map<String, TopicMetadata>) evt.getNewValue();
                MessageStoreManager.this.refreshMessageStoresHoldVals(oldTopicConfigMap, newTopicConfigMap);

            }
        });
        this.logClearScheduler =
                Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "Broker Log Clear Thread");
                    }
                });
        this.unFlushDiskScheduler =
                Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "Broker Log Disk Flush Thread");
                    }
                });
        this.unFlushMemkScheduler =
                Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "Broker Log Mem Flush Thread");
                    }
                });

    }

    @Override
    public void start() {
        try {
            this.msgSsdStoreManager.loadSSDStores();
            this.loadMessageStores(this.tubeConfig);
        } catch (final IOException e) {
            logger.error("[Store Manager] load message stores failed", e);
            throw new StartupException("Initialize message store manager failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        this.logClearScheduler.scheduleWithFixedDelay(new LogClearRunner(),
                tubeConfig.getLogClearupDurationMs() / 2,
                tubeConfig.getLogClearupDurationMs(),
                TimeUnit.MILLISECONDS);

        this.unFlushDiskScheduler.scheduleWithFixedDelay(new DiskUnFlushRunner(),
                tubeConfig.getLogFlushDiskDurMs(),
                tubeConfig.getLogFlushDiskDurMs(),
                TimeUnit.MILLISECONDS);

        this.unFlushMemkScheduler.scheduleWithFixedDelay(new MemUnFlushRunner(),
                tubeConfig.getLogFlushMemDurMs(),
                tubeConfig.getLogFlushMemDurMs(),
                TimeUnit.MILLISECONDS);

    }

    @Override
    public void close() {
        if (this.stopped.get()) {
            return;
        }
        if (this.stopped.compareAndSet(false, true)) {
            logger.info("[Store Manager] begin close store manager......");
            this.logClearScheduler.shutdownNow();
            this.unFlushDiskScheduler.shutdownNow();
            this.unFlushMemkScheduler.shutdownNow();
            this.msgSsdStoreManager.close();
            for (Map.Entry<String, ConcurrentHashMap<Integer, MessageStore>> entry :
                    this.dataStores.entrySet()) {
                if (entry.getValue() != null) {
                    ConcurrentHashMap<Integer, MessageStore> subMap = entry.getValue();
                    for (Map.Entry<Integer, MessageStore> subEntry : subMap.entrySet()) {
                        if (subEntry.getValue() != null) {
                            try {
                                subEntry.getValue().close();
                            } catch (final Throwable e) {
                                logger.error(new StringBuilder(512)
                                        .append("[Store Manager] Try to run close  ")
                                        .append(subEntry.getValue().getStoreKey()).append(" failed").toString(), e);
                            }
                        }
                    }
                }
            }
            this.dataStores.clear();
            logger.info("[Store Manager] Store Manager stopped!");
        }
    }

    @Override
    public List<String> removeTopicStore() {
        if (isRemovingTopic.get()) {
            return null;
        }
        if (!isRemovingTopic.compareAndSet(false, true)) {
            return null;
        }
        try {
            List<String> removedTopics =
                    new ArrayList<String>();
            Map<String, TopicMetadata> removedTopicMap =
                    this.metadataManage.getRemovedTopicConfigMap();
            if (removedTopicMap.isEmpty()) {
                return removedTopics;
            }
            Set<String> targetTopics = new HashSet<String>();
            for (Map.Entry<String, TopicMetadata> entry : removedTopicMap.entrySet()) {
                if (entry.getKey() == null || entry.getValue() == null) {
                    continue;
                }
                if (entry.getValue().getStatusId() == TStatusConstants.STATUS_TOPIC_SOFT_REMOVE) {
                    targetTopics.add(entry.getKey());
                }
            }
            if (targetTopics.isEmpty()) {
                return removedTopics;
            }
            for (String tmpTopic : targetTopics) {
                ConcurrentHashMap<Integer, MessageStore> topicStores =
                        dataStores.get(tmpTopic);
                if (topicStores != null) {
                    Set<Integer> storeIds = topicStores.keySet();
                    for (Integer storeId : storeIds) {
                        try {
                            MessageStore tmpStore = topicStores.remove(storeId);
                            tmpStore.close();
                            if (topicStores.isEmpty()) {
                                this.dataStores.remove(tmpTopic);
                            }
                        } catch (Throwable ee) {
                            logger.error(new StringBuilder(512)
                                    .append("[Remove Topic] Close removed store failure, storeKey=")
                                    .append(tmpTopic).append("-").append(storeId).toString(), ee);
                        }
                    }
                }
                TopicMetadata tmpTopicConf = removedTopicMap.get(tmpTopic);
                if (tmpTopicConf != null) {
                    StringBuilder sBuilder = new StringBuilder(512);
                    for (int storeId = 0; storeId < tmpTopicConf.getNumTopicStores(); storeId++) {
                        String storeDir = sBuilder.append(tmpTopicConf.getDataPath())
                                .append(File.separator).append(tmpTopic).append("-")
                                .append(storeId).toString();
                        sBuilder.delete(0, sBuilder.length());
                        try {
                            delTopicFiles(storeDir);
                        } catch (Throwable e) {
                            logger.error("[Remove Topic] Remove topic data error : ", e);
                        }
                        ThreadUtils.sleep(50);
                    }
                    tmpTopicConf.setStatusId(TStatusConstants.STATUS_TOPIC_HARD_REMOVE);
                    removedTopics.add(tmpTopic);
                }
                ThreadUtils.sleep(100);
            }
            return removedTopics;
        } finally {
            this.isRemovingTopic.set(false);
        }
    }

    /***
     * Get message store by topic.
     *
     * @param topic
     * @return
     */
    @Override
    public Collection<MessageStore> getMessageStoresByTopic(final String topic) {
        final ConcurrentHashMap<Integer, MessageStore> map
                = this.dataStores.get(topic);
        if (map == null) {
            return Collections.emptyList();
        }
        return map.values();
    }

    /***
     * Get or create message store.
     *
     * @param topic
     * @param partition
     * @return
     * @throws IOException
     */
    @Override
    public MessageStore getOrCreateMessageStore(final String topic,
                                                final int partition) throws IOException {
        StringBuilder sBuilder = new StringBuilder(512);
        final int storeId = partition < TBaseConstants.META_STORE_INS_BASE
                ? 0 : partition / TBaseConstants.META_STORE_INS_BASE;
        int realPartition = partition < TBaseConstants.META_STORE_INS_BASE
                ? partition : partition % TBaseConstants.META_STORE_INS_BASE;
        final String dataStoreToken = sBuilder.append("tube_store_manager_").append(topic).toString();
        sBuilder.delete(0, sBuilder.length());
        if (realPartition < 0 || realPartition >= this.metadataManage.getNumPartitions(topic)) {
            throw new IllegalArgumentException(sBuilder.append("Wrong partition value ")
                    .append(partition).append(",valid partitions in (0,")
                    .append(this.metadataManage.getNumPartitions(topic) - 1)
                    .append(")").toString());
        }
        ConcurrentHashMap<Integer, MessageStore> dataMap = dataStores.get(topic);
        if (dataMap == null) {
            ConcurrentHashMap<Integer, MessageStore> tmpTopicMap =
                    new ConcurrentHashMap<Integer, MessageStore>();
            dataMap = this.dataStores.putIfAbsent(topic, tmpTopicMap);
            if (dataMap == null) {
                dataMap = tmpTopicMap;
            }
        }
        MessageStore messageStore = dataMap.get(storeId);
        if (messageStore == null) {
            synchronized (dataStoreToken.intern()) {
                messageStore = dataMap.get(storeId);
                if (messageStore == null) {
                    TopicMetadata topicMetadata =
                            metadataManage.getTopicMetadata(topic);
                    MessageStore tmpMessageStore =
                            new MessageStore(this, topicMetadata, storeId,
                                    tubeConfig, 0, maxMsgTransferSize);
                    messageStore = dataMap.putIfAbsent(storeId, tmpMessageStore);
                    if (messageStore == null) {
                        messageStore = tmpMessageStore;
                        logger.info(sBuilder
                                .append("[Store Manager] Created a new message storage, storeKey=")
                                .append(topic).append("-").append(storeId).toString());
                    } else {
                        tmpMessageStore.close();
                    }
                }
            }
        }
        return messageStore;
    }

    public TubeBroker getTubeBroker() {
        return this.tubeBroker;
    }

    public boolean isSsdServiceStart() {
        return this.msgSsdStoreManager.isSsdServiceInUse();
    }

    public boolean putSsdTransferReq(final String partStr,
                                     final String storeKey, final long startOffset) {
        if (this.msgSsdStoreManager.isSsdServiceInUse()) {
            return this.msgSsdStoreManager.requestSsdTransfer(partStr, storeKey, startOffset, 1);
        }
        return false;
    }

    /***
     * Get messages from ssd.
     *
     * @param storeKey
     * @param partStr
     * @param ssdStartDataOffset
     * @param lastRDOffset
     * @param partitionId
     * @param reqOffset
     * @param indexBuffer
     * @param msgDataSizeLimit
     * @param statisKeyBase
     * @return
     * @throws IOException
     */
    public GetMessageResult getSsdMesssage(final String storeKey,
                                           final String partStr,
                                           final long ssdStartDataOffset,
                                           final long lastRDOffset,
                                           final int partitionId,
                                           final long reqOffset,
                                           final ByteBuffer indexBuffer,
                                           int msgDataSizeLimit,
                                           final String statisKeyBase) throws IOException {
        if (this.msgSsdStoreManager.isSsdServiceInUse()) {
            return msgSsdStoreManager.getMessages(storeKey, partStr,
                    ssdStartDataOffset, lastRDOffset,
                    partitionId, reqOffset, indexBuffer,
                    msgDataSizeLimit, statisKeyBase);
        }
        return new GetMessageResult(false, TErrCodeConstants.INTERNAL_SERVER_ERROR,
                reqOffset, 0, "SSD StoreService not in use!");
    }

    /***
     * Get message from store.
     *
     * @param msgStore
     * @param topic
     * @param partitionId
     * @param msgCount
     * @param filterCondSet
     * @return
     * @throws IOException
     */
    public GetMessageResult getMessages(final MessageStore msgStore,
                                        final String topic,
                                        final int partitionId,
                                        final int msgCount,
                                        final Set<String> filterCondSet) throws IOException {
        long requestOffset = 0L;
        try {
            final long maxOffset = msgStore.getIndexMaxOffset();
            ConsumerNodeInfo consumerNodeInfo =
                    new ConsumerNodeInfo(tubeBroker.getStoreManager(),
                            "visit", filterCondSet, "", System.currentTimeMillis(), "");
            int maxIndexReadSize = (msgCount + 1)
                    * DataStoreUtils.STORE_INDEX_HEAD_LEN * msgStore.getPartitionNum();
            if (filterCondSet != null && !filterCondSet.isEmpty()) {
                maxIndexReadSize *= 5;
            }
            requestOffset = maxOffset - maxIndexReadSize < 0 ? 0L : maxOffset - maxIndexReadSize;
            return msgStore.getMessages(303, requestOffset, partitionId,
                    consumerNodeInfo, topic, this.maxMsgTransferSize);
        } catch (Throwable e1) {
            return new GetMessageResult(false, TErrCodeConstants.INTERNAL_SERVER_ERROR,
                    requestOffset, 0, "Get message failure, errMsg=" + e1.getMessage());
        }
    }

    public SSDSegFound getSourceSegment(final String topic, final int storeId,
                                        final long offset, final int rate) throws IOException {
        ConcurrentHashMap<Integer, MessageStore> map = this.dataStores.get(topic);
        if (map == null) {
            return new SSDSegFound(false, 1, null);
        }
        MessageStore messageStore = map.get(storeId);
        if (messageStore == null) {
            return new SSDSegFound(false, 2, null);
        }
        return messageStore.getSourceSegment(offset, rate);
    }

    public MetadataManage getMetadataManage() {
        return tubeBroker.getMetadataManage();
    }

    public int getMaxMsgTransferSize() {
        return maxMsgTransferSize;
    }

    public Map<String, ConcurrentHashMap<Integer, MessageStore>> getMessageStores() {
        return Collections.unmodifiableMap(this.dataStores);
    }

    private Set<File> getLogDirSet(final BrokerConfig tubeConfig) throws IOException {
        TopicMetadata topicMetadata = null;
        final Set<String> paths = new HashSet<String>();
        paths.add(tubeConfig.getPrimaryPath());
        for (final String topic : metadataManage.getTopics()) {
            topicMetadata = metadataManage.getTopicMetadata(topic);
            if (topicMetadata != null
                    && TStringUtils.isNotBlank(topicMetadata.getDataPath())) {
                paths.add(topicMetadata.getDataPath());
            }
        }
        final Set<File> fileSet = new HashSet<File>();
        for (final String path : paths) {
            final File dir = new File(path);
            if (!dir.exists() && !dir.mkdirs()) {
                throw new IOException(new StringBuilder(512)
                        .append("Could not make Log directory ")
                        .append(dir.getAbsolutePath()).toString());
            }
            if (!dir.isDirectory() || !dir.canRead()) {
                throw new IOException(new StringBuilder(512).append("Log path ")
                        .append(dir.getAbsolutePath())
                        .append(" is not a readable directory").toString());
            }
            fileSet.add(dir);
        }
        return fileSet;
    }

    /***
     * Load stores sequential.
     *
     * @param tubeConfig
     * @throws IOException
     * @throws InterruptedException
     */
    private void loadMessageStores(final BrokerConfig tubeConfig)
            throws IOException, InterruptedException {
        int totalCnt = 0;
        StringBuilder sBuilder = new StringBuilder(512);
        logger.info(sBuilder.append("[Store Manager] Begin to load message stores from path ")
                .append(tubeConfig.getPrimaryPath()).toString());
        sBuilder.delete(0, sBuilder.length());
        final long start = System.currentTimeMillis();
        final AtomicInteger errCnt = new AtomicInteger(0);
        final AtomicInteger finishCnt = new AtomicInteger(0);
        ExecutorService executor =
                Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);
        List<Callable<MessageStore>> tasks = new ArrayList<Callable<MessageStore>>();
        for (final File dir : this.getLogDirSet(tubeConfig)) {
            if (dir == null) {
                continue;
            }
            final File[] ls = dir.listFiles();
            if (ls == null) {
                continue;
            }
            for (final File subDir : ls) {
                if (subDir == null) {
                    continue;
                }
                if (!subDir.isDirectory()) {
                    continue;
                }
                final String name = subDir.getName();
                final int index = name.lastIndexOf('-');
                if (index < 0) {
                    logger.warn(sBuilder.append("[Store Manager] Ignore invlaid directory:")
                            .append(subDir.getAbsolutePath()).toString());
                    sBuilder.delete(0, sBuilder.length());
                    continue;
                }
                final String topic = name.substring(0, index);
                final TopicMetadata topicMetadata = metadataManage.getTopicMetadata(topic);
                if (topicMetadata == null) {
                    logger.warn(sBuilder
                            .append("[Store Manager] No valid topic config for topic data directories:")
                            .append(topic).toString());
                    sBuilder.delete(0, sBuilder.length());
                    continue;
                }
                final int storeId = Integer.parseInt(name.substring(index + 1));
                final MessageStoreManager messageStoreManager = this;
                tasks.add(new Callable<MessageStore>() {
                    @Override
                    public MessageStore call() throws Exception {
                        try {
                            return new MessageStore(messageStoreManager,
                                    topicMetadata, storeId, tubeConfig, maxMsgTransferSize);
                        } catch (Throwable e2) {
                            errCnt.incrementAndGet();
                            logger.error(new StringBuilder(512).append("[Store Manager] Loaded ")
                                    .append(subDir.getAbsolutePath())
                                    .append("message store failure:").toString(), e2);
                            return null;
                        } finally {
                            finishCnt.incrementAndGet();
                        }
                    }
                });
                totalCnt++;
            }
        }
        if (totalCnt > 0) {
            this.loadStoresInParallel(executor, tasks);
            tasks.clear();
            if (errCnt.get() > 0) {
                throw new RuntimeException(
                        "[Store Manager] failure to load message stores, please check load logger and fix first!");
            }
        }
        try {
            Thread.sleep(200);
        } catch (Throwable e) {
            //
        }
        executor.shutdownNow();
        logger.info(sBuilder.append("[Store Manager] End to load message stores in ")
                .append((System.currentTimeMillis() - start) / 1000).append(" secs").toString());
    }

    /***
     * Load stores in parallel.
     *
     * @param executor
     * @param tasks
     * @throws InterruptedException
     */
    private void loadStoresInParallel(ExecutorService executor,
                                      List<Callable<MessageStore>> tasks) throws InterruptedException {
        CompletionService<MessageStore> completionService =
                new ExecutorCompletionService<MessageStore>(executor);
        for (Callable<MessageStore> task : tasks) {
            completionService.submit(task);
        }
        for (int i = 0; i < tasks.size(); i++) {
            try {
                MessageStore messageStore = completionService.take().get();
                if (messageStore == null) {
                    continue;
                }
                ConcurrentHashMap<Integer, MessageStore> map =
                        this.dataStores.get(messageStore.getTopic());
                if (map == null) {
                    map = new ConcurrentHashMap<Integer, MessageStore>();
                    ConcurrentHashMap<Integer, MessageStore> oldmap =
                            this.dataStores.putIfAbsent(messageStore.getTopic(), map);
                    if (oldmap != null) {
                        map = oldmap;
                    }
                }
                MessageStore oldMsgStore = map.putIfAbsent(messageStore.getStoreId(), messageStore);
                if (oldMsgStore != null) {
                    try {
                        logger.info(new StringBuilder(512)
                                .append("[Store Manager] Close duplicated messageStore ")
                                .append(messageStore.getStoreKey()).toString());
                        messageStore.close();
                    } catch (Throwable e2) {
                        //
                        logger.info("[Store Manager] Close duplicated messageStore failure", e2);
                    }
                }
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void delTopicFiles(String filepath) throws IOException {
        File targetFile = new File(filepath);
        if (targetFile.exists()) {
            if (targetFile.isFile()) {
                targetFile.delete();
            } else if (targetFile.isDirectory()) {
                File[] files = targetFile.listFiles();
                if (files != null) {
                    for (int i = 0; i < files.length; i++) {
                        this.delTopicFiles(files[i].getAbsolutePath());
                    }
                }
            }
            targetFile.delete();
        }
    }


    public void refreshMessageStoresHoldVals(Map<String, TopicMetadata> oldTopicConfigMap,
                                             Map<String, TopicMetadata> newTopicConfigMap) {
        if (((newTopicConfigMap == null) || newTopicConfigMap.isEmpty())
                || ((oldTopicConfigMap == null) || oldTopicConfigMap.isEmpty())) {
            return;
        }
        StringBuilder sBuilder = new StringBuilder(512);
        for (TopicMetadata newTopicMetadata : newTopicConfigMap.values()) {
            TopicMetadata oldTopicMetadata = oldTopicConfigMap.get(newTopicMetadata.getTopic());
            if ((oldTopicMetadata == null) || oldTopicMetadata.isPropertyEquals(newTopicMetadata)) {
                continue;
            }
            ConcurrentHashMap<Integer, MessageStore> messageStores =
                    MessageStoreManager.this.dataStores.get(newTopicMetadata.getTopic());
            if ((messageStores == null) || messageStores.isEmpty()) {
                continue;
            }
            for (Map.Entry<Integer, MessageStore> entry : messageStores.entrySet()) {
                if (entry.getValue() != null) {
                    try {
                        entry.getValue().refreshUnflushThreshold(newTopicMetadata);
                    } catch (Throwable ee) {
                        logger.error(sBuilder.append("[Store Manager] refress ")
                                .append(entry.getValue().getStoreKey())
                                .append("'s parameter error,").toString(), ee);
                        sBuilder.delete(0, sBuilder.length());
                    }
                }
            }
        }
    }

    private class LogClearRunner implements Runnable {

        public LogClearRunner() {
            //
        }

        @Override
        public void run() {
            StringBuilder sBuilder = new StringBuilder(256);
            long startTime = System.currentTimeMillis();
            Set<String> expiredTopic = getExpiredTopicSet(sBuilder);
            if (!expiredTopic.isEmpty()) {
                logger.info(sBuilder.append("Found ").append(expiredTopic.size())
                        .append(" files expired, start delete files!").toString());
                sBuilder.delete(0, sBuilder.length());
                for (String topicName : expiredTopic) {
                    if (topicName == null) {
                        continue;
                    }
                    Map<Integer, MessageStore> storeMap = dataStores.get(topicName);
                    if (storeMap == null || storeMap.isEmpty()) {
                        continue;
                    }
                    for (Map.Entry<Integer, MessageStore> entry : storeMap.entrySet()) {
                        if (entry.getValue() == null) {
                            continue;
                        }
                        try {
                            entry.getValue().runClearupPolicy(false);
                        } catch (final Throwable e) {
                            logger.error(sBuilder.append("Try to run delete policy with ")
                                    .append(entry.getValue().getStoreKey())
                                    .append("'s log file  failed").toString(), e);
                            sBuilder.delete(0, sBuilder.length());
                        }
                    }
                }
            }
            long dltTime = System.currentTimeMillis() - startTime;
            if (dltTime >= tubeConfig.getLogClearupDurationMs()) {
                logger.warn(sBuilder.append("Log Clear up task continue over the clearup duration, ")
                        .append("used ").append(dltTime).append(", configure value is ")
                        .append(tubeConfig.getLogClearupDurationMs()).toString());
                sBuilder.delete(0, sBuilder.length());
            }
            if (!expiredTopic.isEmpty()) {
                logger.info("Log Clear Scheduler finished file delete!");
            }
        }

        private Set<String> getExpiredTopicSet(final StringBuilder sb) {
            Set<String> expiredTopic = new HashSet<String>();
            for (Map<Integer, MessageStore> storeMap : dataStores.values()) {
                if (storeMap == null || storeMap.isEmpty()) {
                    continue;
                }
                for (MessageStore msgStore : storeMap.values()) {
                    if (msgStore == null) {
                        continue;
                    }
                    try {
                        if (msgStore.runClearupPolicy(true)) {
                            expiredTopic.add(msgStore.getTopic());
                        }
                    } catch (final Throwable e) {
                        logger.error(sb.append("Try to run delete policy with ")
                                .append(msgStore.getStoreKey())
                                .append("'s log file  failed").toString(), e);
                        sb.delete(0, sb.length());
                    }
                }
            }
            return expiredTopic;
        }
    }

    private class DiskUnFlushRunner implements Runnable {

        public DiskUnFlushRunner() {
            //
        }

        @Override
        public void run() {
            StringBuilder sBuilder = new StringBuilder(256);
            for (Map<Integer, MessageStore> storeMap : dataStores.values()) {
                if (storeMap == null || storeMap.isEmpty()) {
                    continue;
                }
                for (MessageStore msgStore : storeMap.values()) {
                    if (msgStore == null) {
                        continue;
                    }
                    try {
                        msgStore.flushFile();
                    } catch (final Throwable e) {
                        logger.error(sBuilder.append("[Store Manager] Try to flush ")
                                .append(msgStore.getStoreKey())
                                .append("'s file-store failed1 : ").toString(), e);
                        sBuilder.delete(0, sBuilder.length());
                    }
                }
            }
        }
    }

    private class MemUnFlushRunner implements Runnable {

        public MemUnFlushRunner() {
            //
        }

        @Override
        public void run() {
            StringBuilder sBuilder = new StringBuilder(256);
            for (Map<Integer, MessageStore> storeMap : dataStores.values()) {
                if (storeMap == null || storeMap.isEmpty()) {
                    continue;
                }
                for (MessageStore msgStore : storeMap.values()) {
                    if (msgStore == null) {
                        continue;
                    }
                    try {
                        msgStore.flushMemCacheData();
                    } catch (final Throwable e) {
                        logger.error(sBuilder.append("[Store Manager] Try to flush ")
                                .append(msgStore.getStoreKey())
                                .append("'s mem-store failed1 : ").toString(), e);
                        sBuilder.delete(0, sBuilder.length());
                    }
                }
            }
        }
    }

}

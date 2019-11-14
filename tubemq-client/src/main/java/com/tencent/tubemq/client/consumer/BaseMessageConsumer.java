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

package com.tencent.tubemq.client.consumer;

import com.tencent.tubemq.client.common.TubeClientVersion;
import com.tencent.tubemq.client.config.ConsumerConfig;
import com.tencent.tubemq.client.exception.TubeClientException;
import com.tencent.tubemq.client.factory.InnerSessionFactory;
import com.tencent.tubemq.corebase.Message;
import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.TErrCodeConstants;
import com.tencent.tubemq.corebase.TokenConstants;
import com.tencent.tubemq.corebase.aaaclient.ClientAuthenticateHandler;
import com.tencent.tubemq.corebase.aaaclient.SimpleClientAuthenticateHandler;
import com.tencent.tubemq.corebase.balance.ConsumerEvent;
import com.tencent.tubemq.corebase.balance.EventStatus;
import com.tencent.tubemq.corebase.balance.EventType;
import com.tencent.tubemq.corebase.cluster.BrokerInfo;
import com.tencent.tubemq.corebase.cluster.Partition;
import com.tencent.tubemq.corebase.cluster.SubscribeInfo;
import com.tencent.tubemq.corebase.policies.FlowCtrlRuleHandler;
import com.tencent.tubemq.corebase.protobuf.generated.ClientBroker;
import com.tencent.tubemq.corebase.protobuf.generated.ClientMaster;
import com.tencent.tubemq.corebase.utils.AddressUtils;
import com.tencent.tubemq.corebase.utils.DataConverterUtil;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.corebase.utils.ThreadUtils;
import com.tencent.tubemq.corerpc.RpcConfig;
import com.tencent.tubemq.corerpc.RpcConstants;
import com.tencent.tubemq.corerpc.RpcServiceFactory;
import com.tencent.tubemq.corerpc.service.BrokerReadService;
import com.tencent.tubemq.corerpc.service.MasterService;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of MessageConsumer.
 */
public class BaseMessageConsumer implements MessageConsumer {
    private static final Logger logger =
            LoggerFactory.getLogger(BaseMessageConsumer.class);
    private static final int REBALANCE_QUEUE_SIZE = 5000;
    private static final AtomicInteger consumerCounter = new AtomicInteger(0);
    protected final String consumerId;
    protected final ConsumerConfig consumerConfig;
    protected final RmtDataCache rmtDataCache;
    protected final ClientSubInfo consumeSubInfo = new ClientSubInfo();
    private final boolean isPullConsume;
    private final InnerSessionFactory sessionFactory;
    private final RpcServiceFactory rpcServiceFactory;
    private final MasterService masterService;
    private final ScheduledExecutorService heartService2Master;
    private final Thread rebalanceThread;
    private final BlockingQueue<ConsumerEvent> rebalanceEvents =
            new ArrayBlockingQueue<ConsumerEvent>(REBALANCE_QUEUE_SIZE);
    private final BlockingQueue<ConsumerEvent> rebalanceResults =
            new ArrayBlockingQueue<ConsumerEvent>(REBALANCE_QUEUE_SIZE);
    // flowctrl
    private boolean isCurGroupCtrl = false;
    private AtomicLong lastCheckTime = new AtomicLong(0);
    private final FlowCtrlRuleHandler groupFlowCtrlRuleHandler =
            new FlowCtrlRuleHandler(false);
    private final FlowCtrlRuleHandler defFlowCtrlRuleHandler =
            new FlowCtrlRuleHandler(true);
    private final ConsumerSamplePrint samplePrintCtrl =
            new ConsumerSamplePrint();
    private final RpcConfig rpcConfig = new RpcConfig();
    private AtomicLong visitToken = new AtomicLong(TBaseConstants.META_VALUE_UNDEFINED);
    private AtomicReference<String> authAuthorizedTokenRef =
            new AtomicReference<String>("");
    private ClientAuthenticateHandler authenticateHandler =
            new SimpleClientAuthenticateHandler();
    private Thread heartBeatThread2Broker;
    private AtomicBoolean isShutdown = new AtomicBoolean(false);
    private AtomicBoolean isRebalanceStopped = new AtomicBoolean(false);
    private AtomicBoolean isFirst = new AtomicBoolean(true);
    private int heartbeatRetryTimes = 0;
    // Status:
    // -1: Unsubscribed
    // 0: In Process
    // 1: Subscribed
    private AtomicInteger subStatus = new AtomicInteger(-1);
    // rebalance
    private int reportIntervalTimes = 0;
    private int rebalanceRetryTimes = 0;
    private long lastHeartbeatTime2Master = 0;
    private long lastHeartbeatTime2Broker = 0;
    private AtomicBoolean nextWithAuthInfo2M = new AtomicBoolean(false);
    private AtomicBoolean nextWithAuthInfo2B = new AtomicBoolean(false);

    /**
     * Construct a BaseMessageConsumer object.
     *
     * @param sessionFactory session factory
     * @param consumerConfig consumer configuration
     * @param isPullConsume  if this is a pull consumer
     * @throws TubeClientException
     */
    public BaseMessageConsumer(final InnerSessionFactory sessionFactory,
                               final ConsumerConfig consumerConfig,
                               boolean isPullConsume) throws TubeClientException {
        java.security.Security.setProperty("networkaddress.cache.ttl", "3");
        java.security.Security.setProperty("networkaddress.cache.negative.ttl", "1");
        if (sessionFactory == null || consumerConfig == null) {
            throw new TubeClientException(
                    "Illegal parameter: messageSessionFactory or consumerConfig is null!");
        }
        this.sessionFactory = sessionFactory;
        this.consumerConfig = consumerConfig;
        this.isPullConsume = isPullConsume;
        try {
            this.consumerId = generateConsumerID();
        } catch (Exception e) {
            throw new TubeClientException("Get consumer id failed!", e);
        }
        this.rmtDataCache =
                new RmtDataCache(this.defFlowCtrlRuleHandler,
                        this.groupFlowCtrlRuleHandler, null);
        this.rpcServiceFactory =
                this.sessionFactory.getRpcServiceFactory();
        this.rpcConfig.put(RpcConstants.CONNECT_TIMEOUT, 3000);
        this.rpcConfig.put(RpcConstants.REQUEST_TIMEOUT,
                this.consumerConfig.getRpcTimeoutMs());
        this.rpcConfig.put(RpcConstants.WORKER_THREAD_NAME,
                "tube_consumer_netty_worker-");
        this.rpcConfig.put(RpcConstants.CALLBACK_WORKER_COUNT,
                this.consumerConfig.getRpcRspCallBackThreadCnt());
        this.masterService =
                rpcServiceFactory.getFailoverService(MasterService.class,
                        this.consumerConfig.getMasterInfo(), this.rpcConfig);
        this.heartService2Master =
                Executors.newScheduledThreadPool(1, new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, new StringBuilder(512)
                                .append("Master-Heartbeat-Thread-")
                                .append(consumerId).toString());
                        t.setPriority(Thread.MAX_PRIORITY);
                        return t;
                    }
                });
        this.rebalanceThread = new Thread(new Runnable() {
            @Override
            public void run() {
                StringBuilder strBuffer = new StringBuilder(256);
                while ((!isRebalanceStopped()) || (!isShutdown())) {
                    try {
                        ConsumerEvent event = rebalanceEvents.take();
                        rebalanceEvents.clear();
                        if ((isRebalanceStopped()) || (isShutdown())) {
                            break;
                        }
                        switch (event.getType()) {
                            case DISCONNECT:
                            case ONLY_DISCONNECT:
                                disconnectFromBroker(event);
                                rebalanceResults.put(event);
                                break;
                            case CONNECT:
                            case ONLY_CONNECT:
                                connect2Broker(event);
                                rebalanceResults.put(event);
                                break;
                            case REPORT:
                                reportSubscribeInfo();
                                break;
                            case STOPREBALANCE:
                                break;
                            default:
                                throw new TubeClientException(strBuffer
                                        .append("Invalid rebalance opCode:")
                                        .append(event.getType()).toString());
                        }
                        rebalanceRetryTimes = 0;
                    } catch (InterruptedException e) {
                        return;
                    } catch (Throwable e) {
                        rebalanceRetryTimes++;
                        if (!isShutdown()) {
                            strBuffer.delete(0, strBuffer.length());
                            logger.error(strBuffer.append("Rebalance retry ")
                                    .append(rebalanceRetryTimes).append(" failed.").toString(), e);
                            strBuffer.delete(0, strBuffer.length());
                        }
                    }
                }
            }
        }, new StringBuilder(512).append("Rebalance-Thread-")
                .append(this.consumerId).toString());
        this.rebalanceThread.setPriority(Thread.MAX_PRIORITY);
    }

    /**
     * Subscribe a topic.
     *
     * @param topic           topic name
     * @param filterConds     filter conditions
     * @param messageListener message listener
     * @return message consumer
     * @throws TubeClientException
     */
    protected MessageConsumer subscribe(String topic,
                                        TreeSet<String> filterConds,
                                        MessageListener messageListener) throws TubeClientException {
        this.checkClientRunning();
        if (TStringUtils.isBlank(topic)) {
            throw new TubeClientException("Parameter error: topic is Blank!");
        }
        if ((filterConds != null) && (!filterConds.isEmpty())) {
            if (filterConds.size() > TBaseConstants.CFG_FLT_MAX_FILTER_ITEM_COUNT) {
                throw new TubeClientException(new StringBuilder(256)
                        .append("Parameter error: Over max allowed filter count, allowed count is ")
                        .append(TBaseConstants.CFG_FLT_MAX_FILTER_ITEM_COUNT).toString());
            }
            for (String filter : filterConds) {
                if (TStringUtils.isBlank(filter)) {
                    throw new TubeClientException(
                            "Parameter error: blank filter value in parameter filterConds!");
                }
                if (filter.length() > TBaseConstants.CFG_FLT_MAX_FILTER_ITEM_LENGTH) {
                    throw new TubeClientException(new StringBuilder(256)
                            .append("Parameter error: over max allowed filter length, allowed length is ")
                            .append(TBaseConstants.CFG_FLT_MAX_FILTER_ITEM_LENGTH).toString());
                }
            }
        }
        if ((messageListener == null) && (!this.isPullConsume)) {
            throw new IllegalArgumentException("Parameter error: null messageListener");
        }
        TopicProcessor topicProcessor = this.consumeSubInfo.getTopicProcesser(topic);
        if (topicProcessor == null) {
            final TopicProcessor oldProcessor =
                    this.consumeSubInfo.putIfAbsentTopicProcessor(topic,
                            new TopicProcessor(messageListener, filterConds));
            if (oldProcessor != null) {
                throw new TubeClientException(new StringBuilder(256).append("Topic=")
                        .append(topic).append(" has been subscribered").toString());
            }
            return this;
        } else {
            throw new TubeClientException(new StringBuilder(256).append("Topic=")
                    .append(topic).append(" has been subscribered").toString());
        }
    }

    /**
     * Complete subscription.
     *
     * @throws TubeClientException
     */
    @Override
    public void completeSubscribe() throws TubeClientException {
        this.checkClientRunning();
        if (this.consumeSubInfo.isSubscribedTopicEmpty()) {
            throw new TubeClientException("Not subscribe any topic, please subscribe first!");
        }
        if (this.subStatus.get() >= 0) {
            if (this.subStatus.get() == 0) {
                throw new TubeClientException("Duplicated completeSubscribe call!");
            } else {
                throw new TubeClientException("Subscribe has finished!");
            }
        }
        if (!subStatus.compareAndSet(-1, 0)) {
            throw new TubeClientException("Duplicated completeSubscribe call!");
        }
        this.consumeSubInfo.setNotRequireBound();
        this.startMasterAndBrokerThreads();
        this.subStatus.set(1);
    }

    @Override
    public void completeSubscribe(final String sessionKey,
                                  final int sourceCount,
                                  final boolean isSelectBig,
                                  final Map<String, Long> partOffsetMap) throws TubeClientException {
        this.checkClientRunning();
        if (consumeSubInfo.isSubscribedTopicEmpty()) {
            throw new TubeClientException("Not subscribe any topic, please subscribe first!");
        }
        if (partOffsetMap != null) {
            if (TStringUtils.isBlank(sessionKey)) {
                throw new TubeClientException("Parameter error: sessionKey is Blank!");
            }
            if (sourceCount <= 0) {
                throw new TubeClientException("Parameter error: sourceCount must over zero!");
            }
            for (Map.Entry<String, Long> entry : partOffsetMap.entrySet()) {
                if (entry.getKey() != null) {
                    String[] partitionKeyItems = entry.getKey().split(TokenConstants.ATTR_SEP);
                    if (partitionKeyItems.length != 3) {
                        throw new TubeClientException(new StringBuilder(256)
                                .append("Parameter error: partOffsetMap's key ")
                                .append(entry.getKey())
                                .append(" format error: value must be aaaa:bbbb:cccc !").toString());
                    }
                    if (!consumeSubInfo.isSubscribedTopicContain(partitionKeyItems[1].trim())) {
                        throw new TubeClientException(new StringBuilder(256)
                                .append("Parameter error: not included in subcribed topic list: ")
                                .append("partOffsetMap's key is ")
                                .append(entry.getKey()).append(", subscribed topics are ")
                                .append(consumeSubInfo.getSubscribedTopics().toString()).toString());
                    }
                    if (entry.getKey().contains(TokenConstants.ARRAY_SEP)) {
                        throw new TubeClientException(new StringBuilder(256)
                                .append("Parameter error: illegal format error of ")
                                .append(entry.getKey()).append(" : value must not include ',' char!").toString());
                    }
                    if (entry.getValue() != null) {
                        if (entry.getValue() < 0) {
                            throw new TubeClientException(new StringBuilder(256)
                                    .append("Parameter error: Offset must over or equal zero of partOffsetMap  key ")
                                    .append(entry.getKey()).append(", value is ").append(entry.getValue()).toString());
                        }
                    }
                }
            }
        }
        if (this.subStatus.get() >= 0) {
            if (this.subStatus.get() == 0) {
                throw new TubeClientException("Duplicated completeSubscribe call!");
            } else {
                throw new TubeClientException("Subscribe has finished!");
            }
        }
        if (!subStatus.compareAndSet(-1, 0)) {
            throw new TubeClientException("Duplicated completeSubscribe call!");
        }
        if (partOffsetMap == null) {
            this.consumeSubInfo.setNotRequireBound();
        } else {
            this.consumeSubInfo.setRequireBound(sessionKey,
                    sourceCount, isSelectBig, partOffsetMap);
        }
        this.startMasterAndBrokerThreads();
        this.subStatus.set(1);
    }

    @Override
    public boolean isFilterConsume(String topic) {
        return this.consumeSubInfo.isFilterConsume(topic);
    }

    /**
     * Chekc if current consumer is shutdown.
     *
     * @return consumer status
     */
    @Override
    public boolean isShutdown() {
        return this.isShutdown.get();
    }

    /**
     * Get the consumer id.
     *
     * @return consumer id
     */
    @Override
    public String getConsumerId() {
        return this.consumerId;
    }

    /**
     * Get the client version.
     *
     * @return client version
     */
    @Override
    public String getClientVersion() {
        return TubeClientVersion.CONSUMER_VERSION;
    }

    /**
     * Shutdown current consumer.
     *
     * @throws Throwable
     */
    @Override
    public void shutdown() throws Throwable {
        StringBuilder strBuffer = new StringBuilder(256);
        if (this.isShutdown()) {
            logger.info(strBuffer.append("[SHUTDOWN_CONSUMER] ")
                    .append(this.consumerId)
                    .append(" was already shutdown, do nothing...").toString());
            return;
        }
        if (this.isRebalanceStopped()) {
            logger.info(strBuffer.append("[SHUTDOWN_CONSUMER] ")
                    .append(this.consumerId)
                    .append(" is shutting down, do nothing...").toString());
            return;
        }
        logger.info(strBuffer.append("[SHUTDOWN_CONSUMER] Shutting down consumer:")
                .append(this.consumerId).toString());
        strBuffer.delete(0, strBuffer.length());
        if (!this.isRebalanceStopped.compareAndSet(false, true)) {
            return;
        }
        rebalanceEvents.put(new ConsumerEvent(-2,
                EventType.STOPREBALANCE, null, EventStatus.TODO));
        long startWaitTime = System.currentTimeMillis();
        do {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                break;
            }
        } while ((rmtDataCache.isRebProcessing())
                && (System.currentTimeMillis() - startWaitTime < consumerConfig.getShutDownRebalanceWaitPeriodMs()));
        if (this.rebalanceThread != null) {
            try {
                this.rebalanceThread.interrupt();
            } catch (Throwable e) {
                //
            }
        }
        logger.info(strBuffer
                .append("[SHUTDOWN_CONSUMER] Partition rebalance stopped, consumer:")
                .append(this.consumerId).toString());
        strBuffer.delete(0, strBuffer.length());
        //
        this.rmtDataCache.close();
        Map<BrokerInfo, List<PartitionSelectResult>> unRegisterInfoMap =
                rmtDataCache.getAllPartitionListWithStatus();
        unregisterPartitions(unRegisterInfoMap);
        this.isShutdown.set(true);
        this.sessionFactory.removeClient(this);
        if (this.heartService2Master != null) {
            try {
                this.heartService2Master.shutdownNow();
            } catch (Throwable ee) {
                //
            }
        }
        if (this.heartBeatThread2Broker != null) {
            try {
                this.heartBeatThread2Broker.interrupt();
            } catch (Throwable ee) {
                //
            }
        }
        logger.info(strBuffer
                .append("[SHUTDOWN_CONSUMER] Partitions unregistered,  consumer :")
                .append(this.consumerId).toString());
        strBuffer.delete(0, strBuffer.length());
        try {
            masterService.consumerCloseClientC2M(createMasterCloseRequest(),
                    AddressUtils.getLocalAddress(), consumerConfig.isTlsEnable());
        } catch (Throwable e) {
            strBuffer.delete(0, strBuffer.length());
            logger.warn(strBuffer
                    .append("[SHUTDOWN_CONSUMER] call closeRequest failure, error is ")
                    .append(e.getMessage()).toString());
            strBuffer.delete(0, strBuffer.length());
        }
        logger.info(strBuffer.append("[SHUTDOWN_CONSUMER] Client closed, consumer : ")
                .append(this.consumerId).toString());

    }

    /**
     * Get consumer configuration.
     *
     * @return consumer configuration
     */
    @Override
    public ConsumerConfig getConsumerConfig() {
        return this.consumerConfig;
    }

    /**
     * Get partition information of the consumer.
     *
     * @return consumer partition information
     */
    @Override
    public Map<String, ConsumeOffsetInfo> getCurConsumedPartitions() {
        return this.rmtDataCache.getCurPartitionInfoMap();
    }

    /**
     * Check if the rebalance is stopped.
     *
     * @return reblance status
     */
    private boolean isRebalanceStopped() {
        return isRebalanceStopped.get();
    }

    /**
     * Generate consumer id.
     *
     * @return consumer id
     * @throws Exception
     */
    private synchronized String generateConsumerID() throws Exception {
        String pidName = ManagementFactory.getRuntimeMXBean().getName();
        if (pidName != null && pidName.contains("@")) {
            pidName = pidName.split("@")[0];
        }
        StringBuilder strBuffer = new StringBuilder(256)
                .append(this.consumerConfig.getConsumerGroup())
                .append("_").append(AddressUtils.getLocalAddress())
                .append("-").append(pidName)
                .append("-").append(System.currentTimeMillis())
                .append("-").append(consumerCounter.incrementAndGet());
        if (this.isPullConsume) {
            strBuffer.append("-Pull-");
        } else {
            strBuffer.append("-Push-");
        }
        return strBuffer.append(TubeClientVersion.CONSUMER_VERSION).toString();
    }

    /**
     * Start heartbeat thread and rebalance thread.
     *
     * @throws TubeClientException
     */
    private void startMasterAndBrokerThreads() throws TubeClientException {
        int registerRetryTimes = 0;
        StringBuilder strBuffer = new StringBuilder(256);
        while (registerRetryTimes < consumerConfig.getMaxRegisterRetryTimes()) {
            try {
                ClientMaster.RegisterResponseM2C response =
                        masterService.consumerRegisterC2M(createMasterRegisterRequest(),
                                AddressUtils.getLocalAddress(), consumerConfig.isTlsEnable());
                if (response.getSuccess()) {
                    processRegisterAllocAndRspFlowRules(response);
                    processRegAuthorizedToken(response);
                    break;
                }
                if (response.getErrCode() == TErrCodeConstants.CONSUME_GROUP_FORBIDDEN) {
                    logger.error(strBuffer
                            .append("[Register Failed] ConsumeGroup forbidden, register to master failed! ")
                            .append(response.getErrMsg()).toString());
                    strBuffer.delete(0, strBuffer.length());
                    throw new TubeClientException(strBuffer
                            .append("Register to master failed! ConsumeGroup forbidden, ")
                            .append(response.getErrMsg()).toString());
                } else if (response.getErrCode() == TErrCodeConstants.CONSUME_CONTENT_FORBIDDEN) {
                    logger.error(strBuffer
                            .append("[Register Failed] Restricted consume content, register to master failed! ")
                            .append(response.getErrMsg()).toString());
                    strBuffer.delete(0, strBuffer.length());
                    throw new TubeClientException(strBuffer
                            .append("Register to master failed! Restricted consume content, ")
                            .append(response.getErrMsg()).toString());
                } else {
                    logger.error(strBuffer.append("[Register Failed] ")
                            .append(response.getErrMsg()).toString());
                    strBuffer.delete(0, strBuffer.length());
                }
            } catch (Throwable e) {
                logger.error("completeSubscribe throwable is ", e);
                logger.error("Register to master failed.", e);
                ThreadUtils.sleep(this.consumerConfig.getRegFailWaitPeriodMs());
            }
            if (++registerRetryTimes >= consumerConfig.getMaxRegisterRetryTimes()) {
                subStatus.compareAndSet(0, -1);
                logger.error(
                        "Register to master failed! please check and retry later.");
                throw new TubeClientException(
                        "Register to master failed! please check and retry later.");
            }
        }
        // to master heartbeat
        this.lastHeartbeatTime2Master = System.currentTimeMillis();
        this.heartService2Master.scheduleWithFixedDelay(new HeartTask2MasterWorker(),
                0, consumerConfig.getHeartbeatPeriodMs(), TimeUnit.MILLISECONDS);
        // to broker
        this.lastHeartbeatTime2Broker = System.currentTimeMillis();
        this.heartBeatThread2Broker = new Thread(new HeartTask2BrokerWorker());
        heartBeatThread2Broker
                .setName(strBuffer.append("Broker-Heartbeat-Thread-").append(consumerId).toString());
        heartBeatThread2Broker.setPriority(Thread.MAX_PRIORITY);
        heartBeatThread2Broker.start();
        rebalanceThread.start();
    }

    private void disconnectFromBroker(ConsumerEvent event) throws InterruptedException {
        List<String> partKeys = new ArrayList<String>();
        HashMap<BrokerInfo, List<Partition>> unRegisterInfoMap =
                new HashMap<BrokerInfo, List<Partition>>();
        List<SubscribeInfo> subscribeInfoList = event.getSubscribeInfoList();
        for (SubscribeInfo info : subscribeInfoList) {
            BrokerInfo broker =
                    new BrokerInfo(info.getBrokerId(), info.getHost(), info.getPort());
            Partition partition =
                    new Partition(broker, info.getTopic(), info.getPartitionId());
            List<Partition> unRegisterPartitionList =
                    unRegisterInfoMap.get(broker);
            if (unRegisterPartitionList == null) {
                unRegisterPartitionList = new ArrayList<Partition>();
                unRegisterInfoMap.put(broker, unRegisterPartitionList);
            }
            if (!unRegisterPartitionList.contains(partition)) {
                unRegisterPartitionList.add(partition);
                partKeys.add(partition.getPartitionKey());
            }
        }
        if (isShutdown() || isRebalanceStopped()) {
            return;
        }
        Map<BrokerInfo, List<PartitionSelectResult>> unNewRegisterInfoMap =
                new HashMap<BrokerInfo, List<PartitionSelectResult>>();
        try {
            if (this.isPullConsume) {
                unNewRegisterInfoMap =
                        rmtDataCache.removeAndGetPartition(unRegisterInfoMap, partKeys,
                                this.consumerConfig.getPullRebConfirmWaitPeriodMs(),
                                this.consumerConfig.isPullRebConfirmTimeoutRollBack());

            } else {
                unNewRegisterInfoMap =
                        rmtDataCache.removeAndGetPartition(unRegisterInfoMap, partKeys,
                                this.consumerConfig.getPushListenerWaitPeriodMs(),
                                this.consumerConfig.isPushListenerWaitTimeoutRollBack());
            }
        } finally {
            unregisterPartitions(unNewRegisterInfoMap);
            event.setStatus(EventStatus.DONE);
        }
    }

    private void connect2Broker(ConsumerEvent event) throws InterruptedException {
        Map<BrokerInfo, List<Partition>> registerInfoMap =
                new HashMap<BrokerInfo, List<Partition>>();
        List<SubscribeInfo> subscribeInfoList = event.getSubscribeInfoList();
        for (SubscribeInfo info : subscribeInfoList) {
            BrokerInfo broker = new BrokerInfo(info.getBrokerId(), info.getHost(), info.getPort());
            Partition partition = new Partition(broker, info.getTopic(), info.getPartitionId());
            List<Partition> curPartList = registerInfoMap.get(broker);
            if (curPartList == null) {
                curPartList = new ArrayList<Partition>();
                registerInfoMap.put(broker, curPartList);
            }
            if (!curPartList.contains(partition)) {
                curPartList.add(partition);
            }
        }
        if ((isRebalanceStopped()) || (isShutdown())) {
            return;
        }
        List<Partition> unfinishedPartitions = new ArrayList<Partition>();
        rmtDataCache.filterCachedPartitionInfo(registerInfoMap, unfinishedPartitions);
        registerPartitions(registerInfoMap, unfinishedPartitions);
        if (this.isFirst.get()) {
            this.isFirst.set(false);
        }
        event.setStatus(EventStatus.DONE);
    }

    private void reportSubscribeInfo() {
        // TODO
    }

    /**
     * Remove partition from the data cache.
     *
     * @param part partition to be removed
     * @return status
     */
    protected boolean removePartition(Partition part) {
        rmtDataCache.removePartition(part);
        return true;
    }

    /**
     * Publish the selected partitions
     *
     * @return publish result
     */
    protected PartitionSelectResult pushSelectPartition() {
        return rmtDataCache.pushSelect();
    }

    protected void pushReqReleasePartiton(String partitionKey,
                                          long usedTime,
                                          boolean isLastPackConsumed) {
        rmtDataCache.errReqRelease(partitionKey, usedTime, isLastPackConsumed);
    }

    /**
     * Construct a get message request.
     *
     * @param partition      message partition
     * @param isLastConsumed if the last package consumed
     * @return message request
     */
    protected ClientBroker.GetMessageRequestC2B createBrokerGetMessageRequest(
            Partition partition, boolean isLastConsumed) {
        ClientBroker.GetMessageRequestC2B.Builder builder =
                ClientBroker.GetMessageRequestC2B.newBuilder();
        builder.setClientId(this.consumerId);
        builder.setGroupName(this.consumerConfig.getConsumerGroup());
        builder.setTopicName(partition.getTopic());
        builder.setEscFlowCtrl(isCurGroupCtrl());
        builder.setPartitionId(partition.getPartitionId());
        builder.setLastPackConsumed(isLastConsumed);
        builder.setManualCommitOffset(false);
        return builder.build();
    }

    /**
     * Create a commit request.
     *
     * @param partition  partition to be commit
     * @param isConsumed if the last package consumed
     * @return commit request
     */
    protected ClientBroker.CommitOffsetRequestC2B createBrokerCommitRequest(
            Partition partition, boolean isConsumed) {
        ClientBroker.CommitOffsetRequestC2B.Builder builder =
                ClientBroker.CommitOffsetRequestC2B.newBuilder();
        builder.setClientId(this.consumerId);
        builder.setGroupName(this.consumerConfig.getConsumerGroup());
        builder.setTopicName(partition.getTopic());
        builder.setPartitionId(partition.getPartitionId());
        builder.setLastPackConsumed(isConsumed);
        return builder.build();
    }

    /**
     * Register partitions.
     *
     * @param registerInfoMap broker partition mapping
     * @param unRegPartitions unregister partition list
     * @throws InterruptedException
     */
    private void registerPartitions(Map<BrokerInfo, List<Partition>> registerInfoMap,
                                    List<Partition> unRegPartitions) throws InterruptedException {
        int retryTimesRegister2Broker = 0;
        StringBuilder strBuffer = new StringBuilder(512);
        while ((!unRegPartitions.isEmpty())
                && (retryTimesRegister2Broker < this.consumerConfig.getMaxRegisterRetryTimes())) {
            for (Map.Entry<BrokerInfo, List<Partition>> entry : registerInfoMap.entrySet()) {
                ConcurrentLinkedQueue<Partition> regedPartitions =
                        rmtDataCache.getPartitionByBroker(entry.getKey());
                for (Partition partition : entry.getValue()) {
                    if ((isRebalanceStopped()) || (isShutdown())) {
                        return;
                    }
                    try {
                        if (regedPartitions != null && regedPartitions.contains(partition)) {
                            unRegPartitions.remove(partition);
                            continue;
                        }
                        ClientBroker.RegisterResponseB2C responseB2C =
                                getBrokerService(partition.getBroker())
                                        .consumerRegisterC2B(createBrokerRegisterRequest(partition),
                                                AddressUtils.getLocalAddress(), consumerConfig.isTlsEnable());
                        long currOffset =
                                responseB2C.hasCurrOffset() ? responseB2C.getCurrOffset() : -1L;
                        if (responseB2C.getSuccess()) {
                            rmtDataCache.addPartition(partition, currOffset);
                            unRegPartitions.remove(partition);
                            logger.info(strBuffer.append("Registered partition: consumer is ")
                                    .append(consumerId).append(", partition is:")
                                    .append(partition.toString()).toString());
                            strBuffer.delete(0, strBuffer.length());
                        } else {
                            if (responseB2C.getErrCode() == TErrCodeConstants.PARTITION_OCCUPIED
                                    || responseB2C.getErrCode() == TErrCodeConstants.CERTIFICATE_FAILURE) {
                                unRegPartitions.remove(partition);
                                if (responseB2C.getErrCode() == TErrCodeConstants.PARTITION_OCCUPIED) {
                                    if (logger.isDebugEnabled()) {
                                        logger.debug(strBuffer
                                                .append("[Partition occupied], curr consumerId: ")
                                                .append(consumerId).append(", returned message : ")
                                                .append(responseB2C.getErrMsg()).toString());
                                    }
                                } else {
                                    logger.warn(strBuffer
                                            .append("[Certificate failure], curr consumerId: ")
                                            .append(consumerId).append(", returned message : ")
                                            .append(responseB2C.getErrMsg()).toString());
                                }
                            } else {
                                logger.warn(strBuffer.append("register2broker error! ")
                                    .append(retryTimesRegister2Broker).append(" register ")
                                    .append(partition.toString()).append(" return ")
                                    .append(responseB2C.getErrMsg()).toString());
                            }
                            strBuffer.delete(0, strBuffer.length());
                        }
                    } catch (IOException e) {
                        strBuffer.delete(0, strBuffer.length());
                        logger.warn(strBuffer.append("register2broker error1 ! ")
                                .append(retryTimesRegister2Broker).append(" ")
                                .append(partition.toString()).toString(), e);
                        strBuffer.delete(0, strBuffer.length());
                    } catch (Throwable ee) {
                        strBuffer.delete(0, strBuffer.length());
                        logger.warn(strBuffer.append("register2broker error2 ! ")
                                .append(retryTimesRegister2Broker).append(" ")
                                .append(partition.toString()).toString(), ee);
                        strBuffer.delete(0, strBuffer.length());
                    }
                }
            }
            retryTimesRegister2Broker++;
            Thread.sleep(1000);
        }
        for (Partition partition : unRegPartitions) {
            boolean result = removePartition(partition);
            logger.info(strBuffer.append("[Remove Partition] ")
                    .append(partition.toString()).append(" ")
                    .append(result).toString());
            strBuffer.delete(0, strBuffer.length());
        }
    }

    /**
     * Unregister partitions.
     *
     * @param unRegisterInfoMap partitions to be unregister
     */
    private void unregisterPartitions(
            Map<BrokerInfo, List<PartitionSelectResult>> unRegisterInfoMap) {
        StringBuilder strBuffer = new StringBuilder(512);
        strBuffer.append("Unregister info:");
        for (Map.Entry<BrokerInfo, List<PartitionSelectResult>> entry
                : unRegisterInfoMap.entrySet()) {
            for (PartitionSelectResult partResult : entry.getValue()) {
                try {
                    getBrokerService(partResult.getPartition().getBroker())
                            .consumerRegisterC2B(createBrokerUnregisterRequest(partResult.getPartition(),
                                    partResult.isLastPackConsumed()),
                                    AddressUtils.getLocalAddress(), consumerConfig.isTlsEnable());
                } catch (Throwable e) {
                    logger.error(new StringBuilder(512)
                            .append("Disconnect to Broker error! broker:")
                            .append(partResult.getPartition().getBroker().toString()).toString(), e);
                }
                strBuffer.append(partResult.getPartition().toString());
                strBuffer.append("\n");
            }
        }
        logger.info(strBuffer.toString());
    }

    private ClientMaster.RegisterRequestC2M createMasterRegisterRequest() throws Exception {
        ClientMaster.RegisterRequestC2M.Builder builder =
                ClientMaster.RegisterRequestC2M.newBuilder();
        builder.setClientId(consumerId);
        builder.setHostName(AddressUtils.getLocalAddress());
        builder.setRequireBound(this.consumeSubInfo.isRequireBound());
        builder.setGroupName(this.consumerConfig.getConsumerGroup());
        builder.setSessionTime(this.consumeSubInfo.getSubscribedTime());
        builder.addAllTopicList(this.consumeSubInfo.getSubscribedTopics());
        builder.setDefFlowCheckId(defFlowCtrlRuleHandler.getFlowCtrlId());
        builder.setSsdStoreId(groupFlowCtrlRuleHandler.getSsdTranslateId());
        builder.setGroupFlowCheckId(groupFlowCtrlRuleHandler.getFlowCtrlId());
        builder.setQryPriorityId(groupFlowCtrlRuleHandler.getQryPriorityId());
        List<SubscribeInfo> subInfoList =
                this.rmtDataCache.getSubscribeInfoList(consumerId,
                        this.consumerConfig.getConsumerGroup());
        if (subInfoList != null) {
            builder.addAllSubscribeInfo(DataConverterUtil.formatSubInfo(subInfoList));
        }
        builder.addAllTopicCondition(formatTopicCondInfo(consumeSubInfo.getTopicCondRegistry()));
        if (this.consumeSubInfo.isRequireBound()) {
            builder.setSessionKey(this.consumeSubInfo.getSessionKey());
            builder.setSelectBig(this.consumeSubInfo.isSelectBig());
            builder.setTotalCount(this.consumeSubInfo.getSourceCount());
            builder.setRequiredPartition(this.consumeSubInfo.getRequiredPartition());
            builder.setNotAllocated(this.consumeSubInfo.getIsNotAllocated());
        }
        ClientMaster.MasterCertificateInfo.Builder authInfoBuilder = genMasterCertificateInfo(true);
        if (authInfoBuilder != null) {
            builder.setAuthInfo(authInfoBuilder.build());
        }
        return builder.build();
    }

    private List<String> formatTopicCondInfo(
            final ConcurrentHashMap<String, TopicProcessor> topicCondMap) {
        final StringBuilder strBuffer = new StringBuilder(512);
        List<String> strTopicCondList = new ArrayList<String>();
        if ((topicCondMap != null) && (!topicCondMap.isEmpty())) {
            for (Map.Entry<String, TopicProcessor> entry : topicCondMap.entrySet()) {
                if (entry.getKey() == null || entry.getValue() == null) {
                    continue;
                }
                Set<String> condSet = entry.getValue().getFilterConds();
                if (condSet != null && !condSet.isEmpty()) {
                    int i = 0;
                    strBuffer.append(entry.getKey()).append(TokenConstants.SEGMENT_SEP);
                    for (String condStr : condSet) {
                        if (i++ > 0) {
                            strBuffer.append(TokenConstants.ARRAY_SEP);
                        }
                        strBuffer.append(condStr);
                    }
                    strTopicCondList.add(strBuffer.toString());
                    strBuffer.delete(0, strBuffer.length());
                }
            }
        }
        return strTopicCondList;
    }

    private ClientMaster.HeartRequestC2M createMasterHeartbeatRequest(ConsumerEvent event,
                                                                      List<SubscribeInfo> subInfoList,
                                                                      boolean reportSubscribeInfo) throws Exception {
        ClientMaster.HeartRequestC2M.Builder builder = ClientMaster.HeartRequestC2M.newBuilder();
        builder.setClientId(this.consumerId);
        builder.setGroupName(this.consumerConfig.getConsumerGroup());
        builder.setReportSubscribeInfo(reportSubscribeInfo);
        builder.setDefFlowCheckId(defFlowCtrlRuleHandler.getFlowCtrlId());
        builder.setSsdStoreId(groupFlowCtrlRuleHandler.getSsdTranslateId());
        builder.setQryPriorityId(groupFlowCtrlRuleHandler.getQryPriorityId());
        builder.setGroupFlowCheckId(groupFlowCtrlRuleHandler.getFlowCtrlId());
        if (event != null) {
            ClientMaster.EventProto.Builder eventProtoBuilder =
                    ClientMaster.EventProto.newBuilder();
            eventProtoBuilder.setRebalanceId(event.getRebalanceId());
            eventProtoBuilder.setOpType(event.getType().getValue());
            eventProtoBuilder.setStatus(event.getStatus().getValue());
            eventProtoBuilder.addAllSubscribeInfo(
                    DataConverterUtil.formatSubInfo(event.getSubscribeInfoList()));
            ClientMaster.EventProto eventProto = eventProtoBuilder.build();
            builder.setEvent(eventProto);
        }
        if (subInfoList != null) {
            builder.addAllSubscribeInfo(DataConverterUtil.formatSubInfo(subInfoList));
        }
        ClientMaster.MasterCertificateInfo.Builder authInfoBuilder = genMasterCertificateInfo(true);
        if (authInfoBuilder != null) {
            builder.setAuthInfo(authInfoBuilder.build());
        }
        return builder.build();
    }

    private ClientMaster.CloseRequestC2M createMasterCloseRequest() {
        ClientMaster.CloseRequestC2M.Builder builder =
                ClientMaster.CloseRequestC2M.newBuilder();
        builder.setClientId(this.consumerId);
        builder.setGroupName(this.consumerConfig.getConsumerGroup());
        ClientMaster.MasterCertificateInfo.Builder authInfoBuilder = genMasterCertificateInfo(true);
        if (authInfoBuilder != null) {
            builder.setAuthInfo(authInfoBuilder.build());
        }
        return builder.build();
    }

    private ClientBroker.RegisterRequestC2B createBrokerRegisterRequest(Partition partition) {
        ClientBroker.RegisterRequestC2B.Builder builder =
                ClientBroker.RegisterRequestC2B.newBuilder();
        builder.setClientId(consumerId);
        builder.setGroupName(this.consumerConfig.getConsumerGroup());
        builder.setOpType(RpcConstants.MSG_OPTYPE_REGISTER);
        builder.setTopicName(partition.getTopic());
        builder.setPartitionId(partition.getPartitionId());
        builder.setSsdStoreId(groupFlowCtrlRuleHandler.getSsdTranslateId());
        builder.setQryPriorityId(groupFlowCtrlRuleHandler.getQryPriorityId());
        builder.setReadStatus(getGroupInitReadStatus());
        TopicProcessor topicProcessor =
                this.consumeSubInfo.getTopicProcesser(partition.getTopic());
        if (topicProcessor != null && topicProcessor.getFilterConds() != null) {
            builder.addAllFilterCondStr(topicProcessor.getFilterConds());
        }
        if (this.isFirst.get()
                && consumeSubInfo.isRequireBound()
                && consumeSubInfo.getIsNotAllocated()) {
            Long currOffset = consumeSubInfo.getAssignedPartOffset(partition.getPartitionKey());
            if (currOffset != null && currOffset != -1) {
                builder.setCurrOffset(currOffset);
            }
        }
        ClientBroker.AuthorizedInfo.Builder authInfoBuilder =
                genBrokerAuthenticInfo(true);
        if (authInfoBuilder != null) {
            builder.setAuthInfo(authInfoBuilder.build());
        }
        return builder.build();
    }

    private ClientBroker.RegisterRequestC2B createBrokerUnregisterRequest(Partition partition,
                                                                          boolean isLastConsumered) {
        ClientBroker.RegisterRequestC2B.Builder builder =
                ClientBroker.RegisterRequestC2B.newBuilder();
        builder.setClientId(consumerId);
        builder.setGroupName(this.consumerConfig.getConsumerGroup());
        builder.setOpType(RpcConstants.MSG_OPTYPE_UNREGISTER);
        builder.setTopicName(partition.getTopic());
        builder.setPartitionId(partition.getPartitionId());
        if (isLastConsumered) {
            builder.setReadStatus(0);
        } else {
            builder.setReadStatus(1);
        }
        ClientBroker.AuthorizedInfo.Builder authInfoBuilder =
                genBrokerAuthenticInfo(true);
        if (authInfoBuilder != null) {
            builder.setAuthInfo(authInfoBuilder.build());
        }
        return builder.build();
    }

    private ClientBroker.HeartBeatRequestC2B createBrokerHeartBeatRequest(
            List<String> partitionList) {
        ClientBroker.HeartBeatRequestC2B.Builder builder =
                ClientBroker.HeartBeatRequestC2B.newBuilder();
        builder.setClientId(consumerId);
        builder.setGroupName(this.consumerConfig.getConsumerGroup());
        builder.setReadStatus(getGroupInitReadStatus());
        builder.setSsdStoreId(groupFlowCtrlRuleHandler.getSsdTranslateId());
        builder.setQryPriorityId(groupFlowCtrlRuleHandler.getQryPriorityId());
        builder.addAllPartitionInfo(partitionList);
        ClientBroker.AuthorizedInfo.Builder authInfoBuilder =
                genBrokerAuthenticInfo(true);
        if (authInfoBuilder != null) {
            builder.setAuthInfo(authInfoBuilder.build());
        }
        return builder.build();
    }

    private void processRegisterAllocAndRspFlowRules(ClientMaster.RegisterResponseM2C response) {
        if (response.hasNotAllocated() && !response.getNotAllocated()) {
            consumeSubInfo.compareAndSetIsNotAllocated(true, false);
        }
        if (response.hasGroupFlowCheckId()) {
            final int qryPriorityId = response.hasQryPriorityId()
                    ? response.getQryPriorityId() : groupFlowCtrlRuleHandler.getQryPriorityId();
            if (response.getGroupFlowCheckId() != groupFlowCtrlRuleHandler.getFlowCtrlId()) {
                try {
                    groupFlowCtrlRuleHandler.updateDefFlowCtrlInfo(response.getSsdStoreId(),
                            TBaseConstants.META_VALUE_UNDEFINED,
                            response.getGroupFlowCheckId(), response.getGroupFlowControlInfo());
                } catch (Exception e1) {
                    logger.warn("[Register response] found parse group flowCtrl rules failure", e1);
                }
            }
            if (response.getSsdStoreId() != groupFlowCtrlRuleHandler.getSsdTranslateId()) {
                groupFlowCtrlRuleHandler.setSsdTranslateId(response.getSsdStoreId());
            }
            if (qryPriorityId != groupFlowCtrlRuleHandler.getQryPriorityId()) {
                groupFlowCtrlRuleHandler.setQryPriorityId(qryPriorityId);
            }
        }
    }

    private void processRegAuthorizedToken(ClientMaster.RegisterResponseM2C response) {
        if (response.hasAuthorizedInfo()) {
            processAuthorizedToken(response.getAuthorizedInfo());
        }
    }

    private void procHeartBeatRspAllocAndFlowRules(ClientMaster.HeartResponseM2C response) {
        if (response.hasNotAllocated() && !response.getNotAllocated()) {
            consumeSubInfo.compareAndSetIsNotAllocated(true, false);
        }
        if (response.hasGroupFlowCheckId()) {
            final int qryPriorityId = response.hasQryPriorityId()
                    ? response.getQryPriorityId() : groupFlowCtrlRuleHandler.getQryPriorityId();
            if (response.getGroupFlowCheckId() != groupFlowCtrlRuleHandler.getFlowCtrlId()) {
                try {
                    groupFlowCtrlRuleHandler.updateDefFlowCtrlInfo(response.getSsdStoreId(),
                            TBaseConstants.META_VALUE_UNDEFINED,
                            response.getGroupFlowCheckId(), response.getGroupFlowControlInfo());
                } catch (Exception e1) {
                    logger.warn(
                            "[Heartbeat response] found parse group flowCtrl rules failure", e1);
                }
            }
            if (response.getSsdStoreId() != groupFlowCtrlRuleHandler.getSsdTranslateId()) {
                groupFlowCtrlRuleHandler.setSsdTranslateId(response.getSsdStoreId());
            }
            if (qryPriorityId != groupFlowCtrlRuleHandler.getQryPriorityId()) {
                groupFlowCtrlRuleHandler.setQryPriorityId(qryPriorityId);
            }
        }
    }


    private ClientMaster.MasterCertificateInfo.Builder genMasterCertificateInfo(boolean force) {
        boolean needAdd = false;
        ClientMaster.MasterCertificateInfo.Builder authInfoBuilder = null;
        if (this.consumerConfig.isEnableUserAuthentic()) {
            if (force) {
                needAdd = true;
                nextWithAuthInfo2M.set(false);
            } else if (nextWithAuthInfo2M.get()) {
                if (nextWithAuthInfo2M.compareAndSet(true, false)) {
                    needAdd = true;
                }
            }
        }
        if (needAdd) {
            authInfoBuilder = ClientMaster.MasterCertificateInfo.newBuilder();
            authInfoBuilder.setAuthInfo(authenticateHandler
                    .genMasterAuthenticateToken(consumerConfig.getUsrName(),
                            consumerConfig.getUsrPassWord()).build());
        }
        return authInfoBuilder;
    }

    private ClientBroker.AuthorizedInfo.Builder genBrokerAuthenticInfo(boolean force) {
        ClientBroker.AuthorizedInfo.Builder authInfoBuilder =
                ClientBroker.AuthorizedInfo.newBuilder();
        authInfoBuilder.setVisitAuthorizedToken(visitToken.get());
        if (this.consumerConfig.isEnableUserAuthentic()) {
            boolean needAdd = false;
            if (force) {
                needAdd = true;
                nextWithAuthInfo2B.set(false);
            } else if (nextWithAuthInfo2B.get()) {
                if (nextWithAuthInfo2B.compareAndSet(true, false)) {
                    needAdd = true;
                }
            }
            if (needAdd) {
                authInfoBuilder.setAuthAuthorizedToken(authenticateHandler
                        .genBrokerAuthenticateToken(consumerConfig.getUsrName(),
                                consumerConfig.getUsrPassWord()));
            }
        }
        return authInfoBuilder;
    }

    private void processHeartBeatAuthorizedToken(ClientMaster.HeartResponseM2C response) {
        if (response.hasAuthorizedInfo()) {
            processAuthorizedToken(response.getAuthorizedInfo());
        }
    }

    private void processAuthorizedToken(ClientMaster.MasterAuthorizedInfo inAuthorizedTokenInfo) {
        if (inAuthorizedTokenInfo != null) {
            visitToken.set(inAuthorizedTokenInfo.getVisitAuthorizedToken());
            if (inAuthorizedTokenInfo.hasAuthAuthorizedToken()) {
                String inAuthAuthorizedToken = inAuthorizedTokenInfo.getAuthAuthorizedToken();
                if (TStringUtils.isNotBlank(inAuthAuthorizedToken)) {
                    String curAuthAuthorizedToken = authAuthorizedTokenRef.get();
                    if (!inAuthAuthorizedToken.equals(curAuthAuthorizedToken)) {
                        authAuthorizedTokenRef.set(inAuthAuthorizedToken);
                    }
                }
            }
        }
    }

    private int getGroupInitReadStatus() {
        int readStatus = TBaseConstants.CONSUME_MODEL_READ_NORMAL;
        switch (consumerConfig.getConsumeModel()) {
            case 0: {
                if (this.isFirst.get()) {
                    readStatus = TBaseConstants.CONSUME_MODEL_READ_FROM_MAX;
                    logger.info("[Consume From Max Offset]" + consumerId);
                }
                break;
            }
            case 1: {
                if (this.isFirst.get()) {
                    readStatus = TBaseConstants.CONSUME_MODEL_READ_FROM_MAX_ALWAYS;
                    logger.info("[Consume From Max Offset Always]" + consumerId);
                }
                break;
            }
            default: {
                readStatus = TBaseConstants.CONSUME_MODEL_READ_NORMAL;
            }
        }
        return readStatus;
    }

    // #lizard forgives
    protected FetchContext fetchMessage(PartitionSelectResult partSelectResult,
                                        final StringBuilder strBuffer) {
        // Fetch task context based on selected partition
        FetchContext taskContext =
                new FetchContext(partSelectResult);
        Partition partition = taskContext.getPartition();
        String topic = partition.getTopic();
        String partitionKey = partition.getPartitionKey();
        // Response from broker
        ClientBroker.GetMessageResponseB2C msgRspB2C = null;
        try {
            msgRspB2C =
                    getBrokerService(partition.getBroker())
                            .getMessagesC2B(createBrokerGetMessageRequest(
                                    partition, taskContext.isLastConsumed()),
                                    AddressUtils.getLocalAddress(), consumerConfig.isTlsEnable());
        } catch (Throwable ee) {
            // Process the exception
            rmtDataCache.errReqRelease(partitionKey, taskContext.getUsedToken(), false);
            taskContext.setFailProcessResult(400, strBuffer
                    .append("Get message error, reason is ")
                    .append(ee.toString()).toString());
            strBuffer.delete(0, strBuffer.length());
            return taskContext;
        }
        if (msgRspB2C == null) {
            rmtDataCache.errReqRelease(partitionKey, taskContext.getUsedToken(), false);
            taskContext.setFailProcessResult(500, "Get message null");
            return taskContext;
        }
        try {
            // Process the response based on the return code
            switch (msgRspB2C.getErrCode()) {
                case TErrCodeConstants.SUCCESS: {
                    int msgSize = 0;
                    // Convert the message payload data
                    List<Message> tmpMessageList =
                            DataConverterUtil.convertMessage(topic, msgRspB2C.getMessagesList());
                    boolean isEscLimit =
                            (msgRspB2C.hasEscFlowCtrl() && msgRspB2C.getEscFlowCtrl());
                    // Filter the message based on its content
                    // Calculate the message size and do some flow control
                    boolean needFilter = false;
                    Set<String> topicFilterSet = null;
                    TopicProcessor topicProcessor = consumeSubInfo.getTopicProcesser(topic);
                    if (topicProcessor != null) {
                        topicFilterSet = topicProcessor.getFilterConds();
                        if (topicFilterSet != null && !topicFilterSet.isEmpty()) {
                            needFilter = true;
                        }
                    }
                    List<Message> messageList = new ArrayList<Message>();
                    for (Message message : tmpMessageList) {
                        if (message == null) {
                            continue;
                        }
                        if (needFilter && (TStringUtils.isBlank(message.getMsgType())
                                || !topicFilterSet.contains(message.getMsgType()))) {
                            continue;
                        }
                        messageList.add(message);
                        msgSize += message.getData().length;
                    }
                    // Set the process result of current stage. Process the result based on the response
                    long dataDltVal = msgRspB2C.hasCurrDataDlt()
                            ? msgRspB2C.getCurrDataDlt() : -1;
                    long currOffset = msgRspB2C.hasCurrOffset()
                            ? msgRspB2C.getCurrOffset() : TBaseConstants.META_VALUE_UNDEFINED;
                    boolean isRequireSlow =
                            (msgRspB2C.hasRequireSlow() && msgRspB2C.getRequireSlow());
                    rmtDataCache
                            .setPartitionContextInfo(partitionKey, currOffset, 1,
                                    msgRspB2C.getErrCode(), isEscLimit, msgSize, 0,
                                    dataDltVal, isRequireSlow);
                    taskContext.setSuccessProcessResult(currOffset,
                            strBuffer.append(partitionKey).append(TokenConstants.ATTR_SEP)
                                    .append(taskContext.getUsedToken()).toString(), messageList);
                    strBuffer.delete(0, strBuffer.length());
                    break;
                }
                case TErrCodeConstants.NOT_FOUND:
                case TErrCodeConstants.FORBIDDEN:
                case TErrCodeConstants.MOVED: {
                    // Slow down the request based on the limitation configuration when meet these errors
                    long limitDlt = consumerConfig.getMsgNotFoundWaitPeriodMs();
                    if (msgRspB2C.getErrCode() == TErrCodeConstants.FORBIDDEN) {
                        limitDlt = 2000;
                    } else if (msgRspB2C.getErrCode() == TErrCodeConstants.MOVED) {
                        limitDlt = 200;
                    }
                    rmtDataCache.errRspRelease(partitionKey, topic,
                            taskContext.getUsedToken(), false, -1,
                            0, msgRspB2C.getErrCode(), false, 0,
                            limitDlt, isFilterConsume(topic), -1);
                    taskContext.setFailProcessResult(msgRspB2C.getErrCode(), msgRspB2C.getErrMsg());
                    break;
                }
                case TErrCodeConstants.HB_NO_NODE:
                case TErrCodeConstants.CERTIFICATE_FAILURE:
                case TErrCodeConstants.DUPLICATE_PARTITION: {
                    // Release the partitions when meeting these error codes
                    removePartition(partition);
                    taskContext.setFailProcessResult(msgRspB2C.getErrCode(), msgRspB2C.getErrMsg());
                    break;
                }
                case TErrCodeConstants.SERVER_CONSUME_SPEED_LIMIT: {
                    // Process with server side speed limit
                    long defDltTime =
                            msgRspB2C.hasMinLimitTime()
                                    ? msgRspB2C.getMinLimitTime() : consumerConfig.getMsgNotFoundWaitPeriodMs();
                    rmtDataCache.errRspRelease(partitionKey, topic,
                            taskContext.getUsedToken(), false, -1,
                            0, msgRspB2C.getErrCode(), false, 0,
                            defDltTime, isFilterConsume(topic), -1);
                    taskContext.setFailProcessResult(msgRspB2C.getErrCode(), msgRspB2C.getErrMsg());
                    break;
                }
                default: {
                    // Unknown error
                    rmtDataCache.errRspRelease(partitionKey, topic,
                            taskContext.getUsedToken(), false, -1,
                            0, msgRspB2C.getErrCode(), false, 0,
                            300, isFilterConsume(topic), -1);
                    taskContext.setFailProcessResult(msgRspB2C.getErrCode(), msgRspB2C.getErrMsg());
                    break;
                }
            }
            return taskContext;
        } catch (Throwable ee) {
            ee.printStackTrace();
            rmtDataCache.succRspRelease(partitionKey, topic,
                    taskContext.getUsedToken(), false, isFilterConsume(topic), -1);
            taskContext.setFailProcessResult(500, strBuffer
                    .append("Get message failed,topic=")
                    .append(topic).append(",partition=").append(partition)
                    .append(", throw info is ").append(ee.toString()).toString());
            strBuffer.delete(0, strBuffer.length());
        }
        return taskContext;
    }

    protected void checkClientRunning() throws TubeClientException {
        if (this.isShutdown()) {
            throw new TubeClientException("Status error: consumer has been shutdown");
        }
    }

    private boolean isCurGroupCtrl() {
        long curCheckTime = this.lastCheckTime.get();
        if (System.currentTimeMillis() - curCheckTime >= 10000) {
            if (this.lastCheckTime.compareAndSet(curCheckTime, System.currentTimeMillis())) {
                this.isCurGroupCtrl =
                    this.groupFlowCtrlRuleHandler.getCurDataLimit(Long.MAX_VALUE) != null;
            }
        }
        return this.isCurGroupCtrl;
    }

    /**
     * Stopped the message listeners.
     */
    public void notifyAllMessageListenerStopped() {
        this.consumeSubInfo.notifyAllMessageListenerStopped();
    }

    /**
     * Flush last request on a partition.
     *
     * @param partition partition to do the flush operation
     * @return need to reconsume or not
     */
    protected boolean flushLastRequest(Partition partition) {
        boolean needReConsume = true;
        try {
            ClientBroker.CommitOffsetResponseB2C commitResponse =
                    getBrokerService(partition.getBroker())
                            .consumerCommitC2B(createBrokerCommitRequest(partition, true),
                                    AddressUtils.getLocalAddress(), consumerConfig.isTlsEnable());
            if (commitResponse != null && commitResponse.getSuccess()) {
                needReConsume = false;
            }
        } catch (Throwable e) {
            logger.error(new StringBuilder(256)
                    .append("flushLastRequest, commit ")
                    .append(partition.getTopic()).append("#")
                    .append(partition.getPartitionId())
                    .append(" offset failed.").toString(), e);
        }
        return needReConsume;
    }

    protected boolean isSubscribed() {
        return (this.subStatus.get() > 0);
    }

    /**
     * Get the broker read service.
     *
     * @param brokerInfo broker information
     * @return broker read service
     */
    protected BrokerReadService getBrokerService(BrokerInfo brokerInfo) {
        return rpcServiceFactory.getService(BrokerReadService.class, brokerInfo, rpcConfig);
    }

    // #lizard forgives
    private class HeartTask2MasterWorker implements Runnable {
        // Heartbeat logic between master and worker
        @Override
        public void run() {
            StringBuilder strBuffer = new StringBuilder(256);
            try {
                if (isPullConsume) {
                    // For pull consume, do timeout check on partitions pulled without confirm
                    rmtDataCache.resumeTimeoutConsumePartitions(
                            consumerConfig.getPullProtectConfirmTimeoutMs());
                }
                // Fetch the rebalance result, construct message adn return it.
                ConsumerEvent event = rebalanceResults.poll();
                List<SubscribeInfo> subInfoList = null;
                boolean reportSubscribeInfo = false;
                if ((event != null)
                        || (++reportIntervalTimes >= consumerConfig.getMaxSubInfoReportIntvlTimes())) {
                    subInfoList =
                            rmtDataCache.getSubscribeInfoList(consumerId,
                                    consumerConfig.getConsumerGroup());
                    reportSubscribeInfo = true;
                    reportIntervalTimes = 0;
                }
                // Send heartbeat request to master
                ClientMaster.HeartResponseM2C response =
                        masterService.consumerHeartbeatC2M(
                                createMasterHeartbeatRequest(event, subInfoList, reportSubscribeInfo),
                                AddressUtils.getLocalAddress(), consumerConfig.isTlsEnable());
                // Process unsuccessful response
                if (!response.getSuccess()) {
                    // If master replies that cannot find current consumer node, re-register
                    if (response.getErrCode() == TErrCodeConstants.HB_NO_NODE) {
                        try {
                            ClientMaster.RegisterResponseM2C regResponse =
                                    masterService.consumerRegisterC2M(createMasterRegisterRequest(),
                                            AddressUtils.getLocalAddress(), consumerConfig.isTlsEnable());
                            // Print the log when registration fails
                            if (regResponse == null || !regResponse.getSuccess()) {
                                if (regResponse == null) {
                                    logger.error(strBuffer.append("[Re-Register Failed] ").append(consumerId)
                                            .append(" register to master return null!").toString());
                                } else {
                                    // If the consumer group is forbidden, output the log
                                    if (response.getErrCode() == TErrCodeConstants.CONSUME_GROUP_FORBIDDEN) {
                                        logger.error(strBuffer.append("[Re-Register Failed] ")
                                                .append(consumerId)
                                                .append(" ConsumeGroup forbidden, ")
                                                .append(response.getErrMsg()).toString());
                                    } else {
                                        logger.error(strBuffer.append("[Re-Register Failed] ")
                                                .append(consumerId).append(" ")
                                                .append(response.getErrMsg()).toString());
                                    }
                                }
                                strBuffer.delete(0, strBuffer.length());
                            } else {
                                // Process the successful response. Record the response information,
                                // including control rules and latest auth token.
                                processRegisterAllocAndRspFlowRules(regResponse);
                                processRegAuthorizedToken(regResponse);
                                logger.info(strBuffer.append("[Re-register] ")
                                        .append(consumerId).toString());
                                strBuffer.delete(0, strBuffer.length());
                            }
                        } catch (Throwable e) {
                            strBuffer.delete(0, strBuffer.length());
                            logger.error(strBuffer.append("Register to master failed.")
                                    .append(e.getCause()).toString());
                            ThreadUtils.sleep(1000);
                        }
                        return;
                    }
                    logger.error(strBuffer.append("[Heartbeat Failed] ")
                            .append(response.getErrMsg()).toString());
                    if (response.getErrCode() == TErrCodeConstants.CERTIFICATE_FAILURE) {
                        adjustHeartBeatPeriod("certificate failure", strBuffer);
                    } else {
                        heartbeatRetryTimes++;
                    }
                    return;
                }
                // Process the heartbeat success response
                heartbeatRetryTimes = 0;
                // Get the authorization rules and update the local rules
                procHeartBeatRspAllocAndFlowRules(response);
                // Get the latest authorized token
                processHeartBeatAuthorizedToken(response);
                // Check if master requires to check authorization next time. If so, set the flag
                // and exchange the authorize information next time.
                if (response.hasRequireAuth()) {
                    nextWithAuthInfo2M.set(response.getRequireAuth());
                }
                // Get the latest rebalance task
                ClientMaster.EventProto eventProto = response.getEvent();
                if ((eventProto != null) && (eventProto.getRebalanceId() > 0)) {
                    ConsumerEvent newEvent =
                            new ConsumerEvent(eventProto.getRebalanceId(),
                                    EventType.valueOf(eventProto.getOpType()),
                                    DataConverterUtil.convertSubInfo(eventProto.getSubscribeInfoList()),
                                    EventStatus.TODO);
                    rebalanceEvents.put(newEvent);
                    if (logger.isDebugEnabled()) {
                        strBuffer.append("[Receive Consumer Event]");
                        logger.debug(newEvent.toStrBuilder(strBuffer).toString());
                        strBuffer.delete(0, strBuffer.length());
                    }
                }
                // Warning if heartbeat interval is too long
                long currentTime = System.currentTimeMillis();
                if ((currentTime - lastHeartbeatTime2Master)
                        > consumerConfig.getHeartbeatPeriodMs() * 2) {
                    logger.warn(strBuffer.append(consumerId)
                            .append(" heartbeat interval to master is too long,please check! Total time : ")
                            .append(currentTime - lastHeartbeatTime2Master).toString());
                    strBuffer.delete(0, strBuffer.length());
                }
                lastHeartbeatTime2Master = currentTime;
            } catch (InterruptedException ee) {
                logger.info("To Master Heartbeat thread is interrupted,existed!");
            } catch (Throwable e) {
                // Print the log when meeting heartbeat errors.
                // Reduce the heartbeat request frequency when failure count exceed the threshold
                if (!isShutdown()) {
                    logger.error("Heartbeat failed,retry later.", e);
                }
                adjustHeartBeatPeriod("heartbeat exception", strBuffer);
            }
        }

        private void adjustHeartBeatPeriod(String reason, StringBuilder sBuilder) {
            lastHeartbeatTime2Master = System.currentTimeMillis();
            heartbeatRetryTimes++;
            if (!isShutdown()
                    && heartbeatRetryTimes > consumerConfig.getMaxHeartBeatRetryTimes()) {
                logger.warn(sBuilder.append("Adjust HeartbeatPeriod for ").append(reason)
                        .append(", sleep ").append(consumerConfig.getHeartbeatPeriodAfterFail())
                        .append(" Ms").toString());
                sBuilder.delete(0, sBuilder.length());
                try {
                    Thread.sleep(consumerConfig.getHeartbeatPeriodAfterFail());
                } catch (InterruptedException e1) {
                    //
                }
            }
        }
    }

    // #lizard forgives
    private class HeartTask2BrokerWorker implements Runnable {
        @Override
        public void run() {
            StringBuilder strBuffer = new StringBuilder(256);
            while (!isShutdown()) {
                try {
                    // First check the last heartbeat interval. If it's larger than two periods,
                    // there may be some system hang up(e.g. long time gc, CPU is too busy).
                    // Print the warning message.
                    long currentTime = System.currentTimeMillis();
                    if ((currentTime - lastHeartbeatTime2Broker)
                            > (consumerConfig.getHeartbeatPeriodMs() * 2)) {
                        logger.warn(strBuffer.append(consumerId)
                                .append(" heartbeat interval to broker is too long,please check! Total time : ")
                                .append(currentTime - lastHeartbeatTime2Broker).toString());
                        strBuffer.delete(0, strBuffer.length());
                    }
                    // Send heartbeat request to the broker connect by the client
                    for (BrokerInfo brokerInfo : rmtDataCache.getAllRegisterBrokers()) {
                        List<String> partStrSet = new ArrayList<String>();
                        try {
                            // Handle the heartbeat response for partitions belong to the same broker.
                            List<Partition> partitions =
                                    rmtDataCache.getBrokerPartitionList(brokerInfo);
                            if ((partitions != null) && (!partitions.isEmpty())) {
                                for (Partition partition : partitions) {
                                    partStrSet.add(partition.toString());
                                }
                                ClientBroker.HeartBeatResponseB2C heartBeatResponseV2 =
                                        getBrokerService(brokerInfo).consumerHeartbeatC2B(
                                                createBrokerHeartBeatRequest(partStrSet),
                                                AddressUtils.getLocalAddress(), consumerConfig.isTlsEnable());
                                // When response is success
                                if (heartBeatResponseV2.getSuccess()) {
                                    // If the peer require authentication, set a flag.
                                    // The following request will attach the auth information.
                                    if (heartBeatResponseV2.hasRequireAuth()) {
                                        nextWithAuthInfo2B.set(heartBeatResponseV2.getRequireAuth());
                                    }
                                    // If the heartbeat response report failed partitions, release the
                                    // corresponding local partition and log the operation
                                    if (heartBeatResponseV2.getHasPartFailure()) {
                                        try {
                                            List<String> strFailInfoList =
                                                    heartBeatResponseV2.getFailureInfoList();
                                            for (String strFailInfo : strFailInfoList) {
                                                final int index =
                                                        strFailInfo.indexOf(TokenConstants.ATTR_SEP);
                                                if (index < 0) {
                                                    logger.error(strBuffer
                                                            .append("Parse Heartbeat response error : ")
                                                            .append("invalid response, ")
                                                            .append(strFailInfo).toString());
                                                    strBuffer.delete(0, strBuffer.length());
                                                    continue;
                                                }
                                                int errorCode =
                                                        Integer.parseInt(strFailInfo.substring(0, index));
                                                Partition failPartition =
                                                        new Partition(strFailInfo.substring(index + 1));
                                                removePartition(failPartition);
                                                logger.warn(strBuffer
                                                        .append("[heart2broker error] partition:")
                                                        .append(failPartition.toString())
                                                        .append(" errorCode")
                                                        .append(errorCode).toString());
                                                strBuffer.delete(0, strBuffer.length());
                                            }
                                        } catch (Throwable ee) {
                                            if (!isShutdown()) {
                                                strBuffer.delete(0, strBuffer.length());
                                                logger.error(strBuffer
                                                        .append("Parse Heartbeat response error :")
                                                        .append(ee.getMessage()).toString());
                                                strBuffer.delete(0, strBuffer.length());
                                            }
                                        }
                                    }
                                }
                                if (heartBeatResponseV2.getErrCode() == TErrCodeConstants.CERTIFICATE_FAILURE) {
                                    for (Partition partition : partitions) {
                                        removePartition(partition);
                                    }
                                    logger.warn(strBuffer
                                            .append("[heart2broker error] certificate failure, ")
                                            .append(brokerInfo.getBrokerStrInfo())
                                            .append("'s partitions ared released, ")
                                            .append(heartBeatResponseV2.getErrMsg()).toString());
                                    strBuffer.delete(0, strBuffer.length());
                                }
                            }
                        } catch (Throwable ee) {
                            // If there's error in the heartbeat, collect the log and print out.
                            // Release the log string buffer.
                            if (!isShutdown()) {
                                samplePrintCtrl.printExceptionCaught(ee);
                                if (!partStrSet.isEmpty()) {
                                    strBuffer.delete(0, strBuffer.length());
                                    for (String partionStr : partStrSet) {
                                        Partition tmpPartition = new Partition(partionStr);
                                        removePartition(tmpPartition);
                                        logger.warn(strBuffer
                                                .append("[heart2broker Throwable] release partition:")
                                                .append(partionStr).toString());
                                        strBuffer.delete(0, strBuffer.length());
                                    }
                                }
                            }
                        }
                    }
                    // Wait for next heartbeat
                    lastHeartbeatTime2Broker = System.currentTimeMillis();
                    Thread.sleep(consumerConfig.getHeartbeatPeriodMs());
                } catch (Throwable e) {
                    lastHeartbeatTime2Broker = System.currentTimeMillis();
                    if (!isShutdown()) {
                        logger.error("heartbeat thread error 3 : ", e);
                    }
                }
            }
        }
    }

}

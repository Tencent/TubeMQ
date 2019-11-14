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

package com.tencent.tubemq.client.producer;

import com.tencent.tubemq.client.common.TubeClientVersion;
import com.tencent.tubemq.client.config.TubeClientConfig;
import com.tencent.tubemq.client.exception.TubeClientException;
import com.tencent.tubemq.client.factory.InnerSessionFactory;
import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.TErrCodeConstants;
import com.tencent.tubemq.corebase.aaaclient.ClientAuthenticateHandler;
import com.tencent.tubemq.corebase.aaaclient.SimpleClientAuthenticateHandler;
import com.tencent.tubemq.corebase.cluster.BrokerInfo;
import com.tencent.tubemq.corebase.cluster.Partition;
import com.tencent.tubemq.corebase.cluster.TopicInfo;
import com.tencent.tubemq.corebase.protobuf.generated.ClientBroker;
import com.tencent.tubemq.corebase.protobuf.generated.ClientMaster;
import com.tencent.tubemq.corebase.utils.AddressUtils;
import com.tencent.tubemq.corebase.utils.DataConverterUtil;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.corerpc.RpcConfig;
import com.tencent.tubemq.corerpc.RpcConstants;
import com.tencent.tubemq.corerpc.RpcServiceFactory;
import com.tencent.tubemq.corerpc.exception.ClientClosedException;
import com.tencent.tubemq.corerpc.exception.LocalConnException;
import com.tencent.tubemq.corerpc.service.MasterService;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
 * Produce messages through rpc.
 */
public class ProducerManager {
    private static final Logger logger =
            LoggerFactory.getLogger(ProducerManager.class);
    private static final int BROKER_UPDATED_TIME_AFTER_RETRY_FAIL = 2 * 60 * 60 * 1000;
    private static final AtomicInteger producerCounter =
            new AtomicInteger(0);
    private final String producerId;
    private final int producerAddrId;
    private final TubeClientConfig tubeClientConfig;
    private final InnerSessionFactory sessionFactory;
    private final RpcServiceFactory rpcServiceFactory;
    private final ConcurrentHashMap<String, AtomicInteger> publishTopics =
            new ConcurrentHashMap<String, AtomicInteger>();
    private final RpcConfig rpcConfig = new RpcConfig();
    private final ScheduledExecutorService heartbeatService;
    private final AtomicLong visitToken =
            new AtomicLong(TBaseConstants.META_VALUE_UNDEFINED);
    private final AtomicReference<String> authAuthorizedTokenRef =
            new AtomicReference<String>("");
    private final ClientAuthenticateHandler authenticateHandler =
            new SimpleClientAuthenticateHandler();
    private MasterService masterService;
    private Map<Integer, BrokerInfo> brokersMap = new ConcurrentHashMap<Integer, BrokerInfo>();
    private long brokerInfoCheckSum = -1L;
    private long lastBrokerUpdatedTime = System.currentTimeMillis();
    private long lastEmptyBrokerPrintTIme = 0;
    private long lastEmptyTopicPrintTIme = 0;
    private int heartbeatRetryTimes = 0;
    private AtomicBoolean isStartHeart = new AtomicBoolean(false);
    private AtomicInteger heartBeatStatus = new AtomicInteger(-1);
    private volatile long lastHeartbeatTime = System.currentTimeMillis();
    private AtomicInteger nodeStatus = new AtomicInteger(-1);
    private Map<String, Map<Integer, List<Partition>>> topicPartitionMap =
            new ConcurrentHashMap<String, Map<Integer, List<Partition>>>();
    private AtomicBoolean nextWithAuthInfo2M = new AtomicBoolean(false);


    public ProducerManager(final InnerSessionFactory sessionFactory,
                           final TubeClientConfig tubeClientConfig) throws TubeClientException {
        java.security.Security.setProperty("networkaddress.cache.ttl", "3");
        java.security.Security.setProperty("networkaddress.cache.negative.ttl", "1");
        if (sessionFactory == null
                || tubeClientConfig == null) {
            throw new TubeClientException(
                    "Illegal parameter: messageSessionFactory or tubeClientConfig is null!");
        }
        this.tubeClientConfig = tubeClientConfig;
        this.sessionFactory = sessionFactory;
        try {
            this.producerId = generateProducerID();
            this.producerAddrId = AddressUtils.ipToInt(AddressUtils.getLocalAddress());
        } catch (Exception e) {
            throw new TubeClientException("Generate producer id failed!", e);
        }
        this.rpcServiceFactory =
                this.sessionFactory.getRpcServiceFactory();
        rpcConfig.put(RpcConstants.CONNECT_TIMEOUT, 3000);
        rpcConfig.put(RpcConstants.REQUEST_TIMEOUT, tubeClientConfig.getRpcTimeoutMs());
        rpcConfig.put(RpcConstants.NETTY_WRITE_HIGH_MARK,
                tubeClientConfig.getNettyWriteBufferHighWaterMark());
        rpcConfig.put(RpcConstants.NETTY_WRITE_LOW_MARK,
                tubeClientConfig.getNettyWriteBufferLowWaterMark());
        rpcConfig.put(RpcConstants.WORKER_COUNT, tubeClientConfig.getRpcConnProcesserCnt());
        rpcConfig.put(RpcConstants.WORKER_THREAD_NAME, "tube_netty_worker-");
        rpcConfig.put(RpcConstants.CALLBACK_WORKER_COUNT,
                tubeClientConfig.getRpcRspCallBackThreadCnt());
        heartBeatStatus.set(0);
        this.masterService =
                this.rpcServiceFactory.getFailoverService(MasterService.class,
                        tubeClientConfig.getMasterInfo(), rpcConfig);
        this.heartbeatService =
                Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, new StringBuilder(256)
                                .append("Producer-Heartbeat-Thread-")
                                .append(producerId).toString());
                        t.setPriority(Thread.MAX_PRIORITY);
                        return t;
                    }
                });
    }

    public String getClientVersion() {
        return TubeClientVersion.PRODUCER_VERSION;
    }

    /**
     * Start the producer manager.
     *
     * @throws Throwable
     */
    public void start() throws Throwable {
        if (nodeStatus.get() <= 0) {
            if (nodeStatus.compareAndSet(-1, 0)) {
                register2Master();
                logger.info("[Producer] Producer status from ready to running!");
            }
        }
    }

    /**
     * Publish a topic.
     *
     * @param topic topic name
     * @throws TubeClientException
     */
    public synchronized void publish(final String topic) throws TubeClientException {
        checkServiceStatus();
        StringBuilder sBuilder = new StringBuilder(512);
        try {
            logger.info(sBuilder.append("[Publish begin 1] publish topic ")
                    .append(topic).append(", address = ")
                    .append(this.toString()).toString());
            sBuilder.delete(0, sBuilder.length());
            AtomicInteger curPubCnt = this.publishTopics.get(topic);
            if (curPubCnt == null) {
                AtomicInteger tmpPubCnt = new AtomicInteger(0);
                curPubCnt = this.publishTopics.putIfAbsent(topic, tmpPubCnt);
                if (curPubCnt == null) {
                    curPubCnt = tmpPubCnt;
                }
            }
            if (curPubCnt.incrementAndGet() == 1) {
                long curTime = System.currentTimeMillis();
                new ProducerHeartbeatTask().run();
                logger.info(sBuilder
                        .append("[Publish begin 1] already get meta info, topic: ")
                        .append(topic).append(", wast time ")
                        .append(System.currentTimeMillis() - curTime).append(" Ms").toString());
                sBuilder.delete(0, sBuilder.length());
            }
            if (topicPartitionMap.get(topic) == null) {
                throw new TubeClientException(sBuilder
                        .append("Publish topic failure, make sure the topic ")
                        .append(topic).append(" exist or acceptPublish and try later!").toString());
            }
        } finally {
            if (isStartHeart.compareAndSet(false, true)) {
                heartbeatService.scheduleWithFixedDelay(new ProducerHeartbeatTask(), 5L,
                        tubeClientConfig.getHeartbeatPeriodMs(), TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * Publish a set of topic.
     *
     * @param topicSet a set of topic names
     * @return a set of successful published topic names
     * @throws TubeClientException
     */
    public synchronized Set<String> publish(Set<String> topicSet) throws TubeClientException {
        checkServiceStatus();
        StringBuilder sBuilder = new StringBuilder(512);
        Set<String> failTopicSet = new HashSet<String>();
        Set<String> successTopicSet = new HashSet<String>();
        try {
            logger.info(sBuilder.append("[Publish begin 2] publish topicSet ")
                    .append(topicSet).append(", address = ")
                    .append(this.toString()).toString());
            sBuilder.delete(0, sBuilder.length());
            boolean hasNewTopic = false;
            for (String topicItem : topicSet) {
                AtomicInteger curPubCnt = this.publishTopics.get(topicItem);
                if (curPubCnt == null) {
                    AtomicInteger tmpPubCnt = new AtomicInteger(0);
                    curPubCnt = this.publishTopics.putIfAbsent(topicItem, tmpPubCnt);
                    if (curPubCnt == null) {
                        curPubCnt = tmpPubCnt;
                    }
                }
                if (curPubCnt.incrementAndGet() == 1) {
                    hasNewTopic = true;
                }
            }
            if (hasNewTopic) {
                long curTime = System.currentTimeMillis();
                new ProducerHeartbeatTask().run();
                logger.info(sBuilder
                        .append("[Publish begin 2] already get meta info, topicSet: ")
                        .append(topicSet).append(", wast time ")
                        .append(System.currentTimeMillis() - curTime).append(" Ms").toString());
                sBuilder.delete(0, sBuilder.length());
            }
            for (String topicItem : topicSet) {
                if (topicPartitionMap.containsKey(topicItem)) {
                    successTopicSet.add(topicItem);
                } else {
                    failTopicSet.add(topicItem);
                }
            }
            if (!failTopicSet.isEmpty()) {
                logger.info(sBuilder.append("Publish topic failure, make sure the topics ")
                        .append(failTopicSet).append(" exist or acceptPublish and try later!").toString());
                sBuilder.delete(0, sBuilder.length());
            }
        } finally {
            if (isStartHeart.compareAndSet(false, true)) {
                heartbeatService.scheduleWithFixedDelay(new ProducerHeartbeatTask(), 5L,
                        tubeClientConfig.getHeartbeatPeriodMs(), TimeUnit.MILLISECONDS);
            }
        }
        return successTopicSet;
    }

    /**
     * Shutdown the produce manager.
     *
     * @throws Throwable
     */
    public void shutdown() throws Throwable {
        logger.info("[ShutDown Producer] Shutting down heartbeat...");
        if (this.nodeStatus.get() != 0) {
            if (isShutdown()) {
                logger.info(new StringBuilder(512)
                        .append("[ShutDown Producer] Producer ").append(producerId)
                        .append("has been shutdown,please do not make a duplicated invocation.").toString());
            }
            return;
        }
        if (this.nodeStatus.compareAndSet(0, 1)) {
            this.heartbeatService.shutdownNow();
            this.topicPartitionMap.clear();
            masterService.producerCloseClientP2M(createCloseProducerRequest(),
                    AddressUtils.getLocalAddress(), tubeClientConfig.isTlsEnable());
            logger.info("[SHUTDOWN_TUBE] tube heartbeat was shutdown.");
        }
    }

    /**
     * Get the producer id.
     *
     * @return producer id
     */
    public String getProducerId() {
        return producerId;
    }

    /**
     * Get the producer address id.
     *
     * @return address id
     */
    public int getProducerAddrId() {
        return producerAddrId;
    }

    /**
     * Check if the producer manager is shutdown.
     *
     * @return producer status
     */
    public boolean isShutdown() {
        return (this.nodeStatus.get() > 0);
    }

    /**
     * Set the authorized token information.
     *
     * @param builder message builder
     * @return the passed in builder
     */
    public ClientBroker.SendMessageRequestP2B.Builder setAuthorizedTokenInfo(
            ClientBroker.SendMessageRequestP2B.Builder builder) {
        ClientBroker.AuthorizedInfo.Builder authInfoBuilder =
                ClientBroker.AuthorizedInfo.newBuilder();
        authInfoBuilder.setVisitAuthorizedToken(this.visitToken.get());
        String authAuthorizedToken = this.authAuthorizedTokenRef.get();
        if (TStringUtils.isNotBlank(authAuthorizedToken)) {
            authInfoBuilder.setAuthAuthorizedToken(authAuthorizedToken);
        }
        builder.setAuthInfo(authInfoBuilder.build());
        return builder;
    }

    /**
     * Remove published topics. We will ignore null topics or non-published topics.
     *
     * @param topicSet
     */
    public synchronized void removeTopic(Set<String> topicSet) {
        for (String topic : topicSet) {
            if (topic == null) {
                continue;
            }
            AtomicInteger subCnt = publishTopics.get(topic);
            if (subCnt == null) {
                return;
            }
            if (subCnt.decrementAndGet() == 0) {
                publishTopics.remove(topic);
            }
        }
    }

    /**
     * Get partitions of the given topic.
     *
     * @param topic topic name
     * @return partition map
     */
    public Map<Integer, List<Partition>> getTopicPartition(String topic) {
        return topicPartitionMap.get(topic);
    }

    private void checkServiceStatus() throws TubeClientException {
        if (nodeStatus.get() < 0) {
            throw new TubeClientException("Status error: please call start function first!");
        }
        if (nodeStatus.get() > 0) {
            throw new TubeClientException("Status error: producer service has been shutdown!");
        }
    }

    private void register2Master() throws Throwable {
        int remainingRetry =
                this.tubeClientConfig.getMaxRegisterRetryTimes();
        StringBuilder sBuilder = new StringBuilder(512);
        do {
            if (isShutdown()) {
                logger.error("Producer service has been shutdown, exit register to master!");
                break;
            }
            remainingRetry--;
            try {
                ClientMaster.RegisterResponseM2P response =
                        this.masterService.producerRegisterP2M(createRegisterRequest(),
                                AddressUtils.getLocalAddress(), tubeClientConfig.isTlsEnable());
                if ((response != null) && (response.getSuccess())) {
                    if (response.getBrokerCheckSum() != this.brokerInfoCheckSum) {
                        updateBrokerInfoList(true, response.getBrokerInfosList(),
                                response.getBrokerCheckSum(), sBuilder);
                    }
                    processRegAuthorizedToken(response);
                    return;
                }
                if (remainingRetry <= 0) {
                    if (response == null) {
                        throw new TubeClientException(
                                "Register producer failure, response is null!");
                    } else {
                        throw new TubeClientException(sBuilder
                                .append("Register producer failure, error is ")
                                .append(response.getErrMsg()).toString());
                    }
                }
                Thread.sleep(1000);
            } catch (Throwable e) {
                sBuilder.delete(0, sBuilder.length());
                if (e instanceof LocalConnException) {
                    logger.warn("register2Master error, retry... exception: ", e);
                }
                try {
                    Thread.sleep(1200);
                } catch (Throwable ee2) {
                    //
                }
                if (remainingRetry <= 0) {
                    throw e;
                }
            }
        } while (true);
    }

    private ClientMaster.RegisterRequestP2M createRegisterRequest() throws Exception {
        ClientMaster.RegisterRequestP2M.Builder builder =
                ClientMaster.RegisterRequestP2M.newBuilder();
        builder.setClientId(producerId);
        builder.addAllTopicList(publishTopics.keySet());
        builder.setBrokerCheckSum(this.brokerInfoCheckSum);
        builder.setHostName(AddressUtils.getLocalAddress());
        ClientMaster.MasterCertificateInfo.Builder authInfoBuilder =
                genMasterCertificateInfo(true);
        if (authInfoBuilder != null) {
            builder.setAuthInfo(authInfoBuilder.build());
        }
        return builder.build();
    }

    private ClientMaster.HeartRequestP2M createHeartbeatRequest() throws Exception {
        ClientMaster.HeartRequestP2M.Builder builder =
                ClientMaster.HeartRequestP2M.newBuilder();
        builder.setClientId(producerId);
        builder.addAllTopicList(publishTopics.keySet());
        builder.setBrokerCheckSum(this.brokerInfoCheckSum);
        if ((System.currentTimeMillis() - this.lastBrokerUpdatedTime)
                > BROKER_UPDATED_TIME_AFTER_RETRY_FAIL) {
            builder.setBrokerCheckSum(-1L);
            this.lastBrokerUpdatedTime = System.currentTimeMillis();
        }
        builder.setHostName(AddressUtils.getLocalAddress());
        ClientMaster.MasterCertificateInfo.Builder authInfoBuilder =
                genMasterCertificateInfo(true);
        if (authInfoBuilder != null) {
            builder.setAuthInfo(authInfoBuilder.build());
        }
        return builder.build();
    }

    private ClientMaster.CloseRequestP2M createCloseProducerRequest() {
        ClientMaster.CloseRequestP2M.Builder builder =
                ClientMaster.CloseRequestP2M.newBuilder();
        builder.setClientId(producerId);
        ClientMaster.MasterCertificateInfo.Builder authInfoBuilder =
                genMasterCertificateInfo(true);
        if (authInfoBuilder != null) {
            builder.setAuthInfo(authInfoBuilder.build());
        }
        return builder.build();
    }

    private void updateTopicPartitions(List<TopicInfo> topicInfoList) {
        Map<String, Map<Integer, List<Partition>>> paritionListMap =
                new ConcurrentHashMap<String, Map<Integer, List<Partition>>>();
        for (TopicInfo topicInfo : topicInfoList) {
            Map<Integer, List<Partition>> brokerPartList =
                    paritionListMap.get(topicInfo.getTopic());
            if (brokerPartList == null) {
                brokerPartList = new ConcurrentHashMap<Integer, List<Partition>>();
                paritionListMap.put(topicInfo.getTopic(), brokerPartList);
            }
            for (int j = 0; j < topicInfo.getTopicStoreNum(); j++) {
                int baseValue = j * TBaseConstants.META_STORE_INS_BASE;
                for (int i = 0; i < topicInfo.getPartitionNum(); i++) {
                    Partition part =
                            new Partition(topicInfo.getBroker(), topicInfo.getTopic(), baseValue + i);
                    List<Partition> partList = brokerPartList.get(part.getBrokerId());
                    if (partList == null) {
                        partList = new ArrayList<Partition>();
                        brokerPartList.put(part.getBrokerId(), partList);
                    }
                    partList.add(part);
                }
            }
        }
        topicPartitionMap = paritionListMap;
    }

    private synchronized String generateProducerID() throws Exception {
        String pidName = ManagementFactory.getRuntimeMXBean().getName();
        if (pidName != null && pidName.contains("@")) {
            pidName = pidName.split("@")[0];
        }
        return new StringBuilder(256)
                .append(AddressUtils.getLocalAddress())
                .append("-").append(pidName)
                .append("-").append(System.currentTimeMillis())
                .append("-").append(producerCounter.incrementAndGet())
                .append("-").append(TubeClientVersion.PRODUCER_VERSION).toString();
    }

    private void updateBrokerInfoList(boolean isRegister, List<String> pkgBrokerInfos,
                                      long pkgCheckSum, StringBuilder sBuilder) {
        if (pkgCheckSum != brokerInfoCheckSum) {
            if (pkgBrokerInfos != null) {
                brokersMap =
                        DataConverterUtil.convertBrokerInfo(pkgBrokerInfos);
                brokerInfoCheckSum = pkgCheckSum;
                lastBrokerUpdatedTime = System.currentTimeMillis();
                if (pkgBrokerInfos.isEmpty()) {
                    if (System.currentTimeMillis() - lastEmptyBrokerPrintTIme > 60000) {
                        if (isRegister) {
                            logger.warn(sBuilder
                                    .append("[Register Update] Found empty brokerList, chaneged checksum is ")
                                    .append(brokerInfoCheckSum).toString());
                        } else {
                            logger.warn(sBuilder
                                    .append("[Heartbeat Update] Found empty brokerList chaneged checksum is ")
                                    .append(brokerInfoCheckSum).toString());
                        }
                        sBuilder.delete(0, sBuilder.length());
                        lastEmptyBrokerPrintTIme = System.currentTimeMillis();
                    }
                } else {
                    if (!isRegister) {
                        logger.info(sBuilder
                                .append("[Heartbeat Update] Found brokerList chaneged checksum is ")
                                .append(brokerInfoCheckSum).toString());
                        sBuilder.delete(0, sBuilder.length());
                    }
                }
            }
        }
    }

    private void processRegAuthorizedToken(ClientMaster.RegisterResponseM2P response) {
        if (response.hasAuthorizedInfo()) {
            processAuthorizedToken(response.getAuthorizedInfo());
        }
    }

    private void processHeartBeatAuthorizedToken(ClientMaster.HeartResponseM2P response) {
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
                    if (curAuthAuthorizedToken == null
                            || !inAuthAuthorizedToken.equals(curAuthAuthorizedToken)) {
                        authAuthorizedTokenRef.set(inAuthAuthorizedToken);
                    }
                }
            }
        }
    }

    private ClientMaster.MasterCertificateInfo.Builder genMasterCertificateInfo(boolean force) {
        boolean needAdd = false;
        ClientMaster.MasterCertificateInfo.Builder authInfoBuilder = null;
        if (this.tubeClientConfig.isEnableUserAuthentic()) {
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
                    .genMasterAuthenticateToken(tubeClientConfig.getUsrName(),
                            tubeClientConfig.getUsrPassWord()).build());
        }
        return authInfoBuilder;
    }

    // #lizard forgives
    private class ProducerHeartbeatTask implements Runnable {
        @Override
        public void run() {
            StringBuilder sBuilder = new StringBuilder(512);
            while (!heartBeatStatus.compareAndSet(0, 1)) {
                try {
                    Thread.sleep(100);
                } catch (Throwable e1) {
                    //
                }
            }
            if (publishTopics.isEmpty()) {
                return;
            }
            try {
                ClientMaster.HeartResponseM2P response =
                        masterService.producerHeartbeatP2M(createHeartbeatRequest(),
                                AddressUtils.getLocalAddress(), tubeClientConfig.isTlsEnable());
                if (response == null || !response.getSuccess()) {
                    heartbeatRetryTimes++;
                    if (response == null) {
                        logger.error("[Heartbeat Failed] receive null HeartResponseM2P response!");
                    } else {
                        logger.error(sBuilder.append("[Heartbeat Failed] ")
                                .append(response.getErrMsg()).toString());
                        sBuilder.delete(0, sBuilder.length());
                        if (response.getErrCode() == TErrCodeConstants.HB_NO_NODE) {
                            try {
                                register2Master();
                            } catch (Throwable ee) {
                                logger.error(sBuilder
                                    .append("[Heartbeat Failed] re-register failure, error is ")
                                    .append(ee.getMessage()).toString());
                                sBuilder.delete(0, sBuilder.length());
                            }
                        } else if (response.getErrCode() == TErrCodeConstants.CERTIFICATE_FAILURE) {
                            adjustHeartBeatPeriod("certificate failure", sBuilder);
                        }
                    }
                    return;
                }
                if (response.hasRequireAuth()) {
                    nextWithAuthInfo2M.set(response.getRequireAuth());
                }
                processHeartBeatAuthorizedToken(response);
                if (response.getErrCode() == TErrCodeConstants.NOT_READY) {
                    lastHeartbeatTime = System.currentTimeMillis();
                    return;
                }
                if (response.getBrokerCheckSum() != brokerInfoCheckSum) {
                    updateBrokerInfoList(false, response.getBrokerInfosList(),
                            response.getBrokerCheckSum(), sBuilder);
                }
                if (response.getTopicInfosList() != null) {
                    if (response.getTopicInfosList().isEmpty()) {
                        if (System.currentTimeMillis() - lastEmptyTopicPrintTIme > 60000) {
                            logger.warn("[Heartbeat Update] found empty topicList update!");
                            lastEmptyTopicPrintTIme = System.currentTimeMillis();
                        }
                    }
                    updateTopicPartitions(DataConverterUtil
                            .convertTopicInfo(brokersMap, response.getTopicInfosList()));
                } else {
                    logger.error(sBuilder
                            .append("[Heartbeat Failed] Found brokerList or topicList is null, brokerList is ")
                            .append(response.getBrokerInfosList() != null).toString());
                    sBuilder.delete(0, sBuilder.length());
                }
                heartbeatRetryTimes = 0;
                long currentTime = System.currentTimeMillis();
                if ((currentTime - lastHeartbeatTime)
                        > (tubeClientConfig.getHeartbeatPeriodMs() * 4)) {
                    logger.warn(sBuilder.append(producerId)
                            .append(" heartbeat interval is too long, please check! Total time : ")
                            .append(currentTime - lastHeartbeatTime).toString());
                    sBuilder.delete(0, sBuilder.length());
                }
                lastHeartbeatTime = currentTime;
            } catch (Throwable e) {
                sBuilder.delete(0, sBuilder.length());
                if (!(e.getCause() != null
                        && e.getCause() instanceof ClientClosedException)) {
                    logger.error("Heartbeat failed,retry later.Reason:{}",
                            sBuilder.append(e.getClass().getSimpleName())
                                    .append("#").append(e.getMessage()).toString());
                    sBuilder.delete(0, sBuilder.length());
                }
                adjustHeartBeatPeriod("heartbeat exception", sBuilder);
            } finally {
                heartBeatStatus.compareAndSet(1, 0);
            }
        }

        private void adjustHeartBeatPeriod(String reason, StringBuilder sBuilder) {
            lastHeartbeatTime = System.currentTimeMillis();
            heartbeatRetryTimes++;
            if ((nodeStatus.get() != 1)
                && heartbeatRetryTimes > tubeClientConfig.getMaxHeartBeatRetryTimes()) {
                logger.warn(sBuilder.append("Adjust HeartbeatPeriod for ").append(reason)
                    .append(", sleep ").append(tubeClientConfig.getHeartbeatPeriodAfterFail())
                    .append(" Ms").toString());
                sBuilder.delete(0, sBuilder.length());
                try {
                    Thread.sleep(tubeClientConfig.getHeartbeatPeriodAfterFail());
                } catch (InterruptedException e1) {
                    return;
                }
            }
        }
    }

}

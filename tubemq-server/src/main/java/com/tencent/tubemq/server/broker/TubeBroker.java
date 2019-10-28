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

package com.tencent.tubemq.server.broker;

import com.tencent.tubemq.corebase.TErrCodeConstants;
import com.tencent.tubemq.corebase.aaaclient.ClientAuthenticateHandler;
import com.tencent.tubemq.corebase.aaaclient.SimpleClientAuthenticateHandler;
import com.tencent.tubemq.corebase.cluster.MasterInfo;
import com.tencent.tubemq.corebase.policies.FlowCtrlRuleHandler;
import com.tencent.tubemq.corebase.protobuf.generated.ClientMaster;
import com.tencent.tubemq.corebase.protobuf.generated.ClientMaster.CloseRequestB2M;
import com.tencent.tubemq.corebase.protobuf.generated.ClientMaster.EnableBrokerFunInfo;
import com.tencent.tubemq.corebase.protobuf.generated.ClientMaster.HeartRequestB2M;
import com.tencent.tubemq.corebase.protobuf.generated.ClientMaster.HeartResponseM2B;
import com.tencent.tubemq.corebase.protobuf.generated.ClientMaster.RegisterRequestB2M;
import com.tencent.tubemq.corebase.protobuf.generated.ClientMaster.RegisterResponseM2B;
import com.tencent.tubemq.corebase.utils.ServiceStatusHolder;
import com.tencent.tubemq.corerpc.RpcConfig;
import com.tencent.tubemq.corerpc.RpcConstants;
import com.tencent.tubemq.corerpc.RpcServiceFactory;
import com.tencent.tubemq.corerpc.netty.NettyClientFactory;
import com.tencent.tubemq.corerpc.service.MasterService;
import com.tencent.tubemq.server.Stoppable;
import com.tencent.tubemq.server.broker.exception.StartupException;
import com.tencent.tubemq.server.broker.metadata.BrokerMetadataManage;
import com.tencent.tubemq.server.broker.metadata.MetadataManage;
import com.tencent.tubemq.server.broker.msgstore.MessageStoreManager;
import com.tencent.tubemq.server.broker.nodeinfo.ConsumerNodeInfo;
import com.tencent.tubemq.server.broker.offset.DefaultOffsetManager;
import com.tencent.tubemq.server.broker.offset.OffsetService;
import com.tencent.tubemq.server.broker.utils.BrokerSamplePrint;
import com.tencent.tubemq.server.broker.web.WebServer;
import com.tencent.tubemq.server.common.TubeServerVersion;
import com.tencent.tubemq.server.common.aaaserver.SimpleCertificateBrokerHandler;
import com.tencent.tubemq.server.common.utils.Sleeper;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Tube broker server. In charge of init each components, and communicating to master.
 */
public class TubeBroker implements Stoppable, Runnable {
    private static final Logger logger =
            LoggerFactory.getLogger(TubeBroker.class);
    // tube broker config
    private final BrokerConfig tubeConfig;
    // broker id
    private final String brokerId;
    private final NettyClientFactory clientFactory = new NettyClientFactory();
    private final RpcServiceFactory rpcServiceFactory;
    // tube web server
    private final WebServer webServer;
    // tube broker's metadata manager
    private final MetadataManage metadataManage;
    // tube broker's store manager
    private final MessageStoreManager storeManager;
    // tube broker's offset manager
    private final OffsetService offsetManager;
    private final BrokerServiceServer brokerServiceServer;
    private final BrokerSamplePrint samplePrintCtrl =
            new BrokerSamplePrint(logger);
    private final ScheduledExecutorService scheduledExecutorService;
    private final Sleeper sleeper;
    // shutdown hook.
    private final ShutdownHook shutdownHook = new ShutdownHook();
    // certificate handler.
    private final SimpleCertificateBrokerHandler serverAuthHandler;
    private final ClientAuthenticateHandler clientAuthHandler =
            new SimpleClientAuthenticateHandler();
    private MasterService masterService;
    private boolean requireReportConf = false;
    private boolean isOnline = false;
    private AtomicBoolean shutdown = new AtomicBoolean(true);
    private AtomicBoolean shutdownHooked = new AtomicBoolean(false);
    private AtomicLong heartbeatErrors = new AtomicLong(0);
    private int maxReleaseTryCnt = 10;
    private AtomicBoolean enableProduceAuthenticate = new AtomicBoolean(false);
    private AtomicBoolean enableProduceAuthorize = new AtomicBoolean(false);
    private AtomicBoolean enableConsumeAuthenticate = new AtomicBoolean(false);
    private AtomicBoolean getEnableConsumeAuthorize = new AtomicBoolean(false);

    public TubeBroker(final BrokerConfig tubeConfig) throws Exception {
        super();
        java.security.Security.setProperty("networkaddress.cache.ttl", "3");
        java.security.Security.setProperty("networkaddress.cache.negative.ttl", "1");
        this.tubeConfig = tubeConfig;
        this.brokerId = generateBrokerClientId();
        this.metadataManage = new BrokerMetadataManage();
        this.offsetManager = new DefaultOffsetManager(tubeConfig);
        this.storeManager = new MessageStoreManager(this, tubeConfig);
        this.serverAuthHandler = new SimpleCertificateBrokerHandler(this);
        // rpc config.
        RpcConfig rpcConfig = new RpcConfig();
        rpcConfig.put(RpcConstants.CONNECT_TIMEOUT, 3000);
        rpcConfig.put(RpcConstants.REQUEST_TIMEOUT, this.tubeConfig.getRpcReadTimeoutMs());
        clientFactory.configure(rpcConfig);
        this.rpcServiceFactory =
                new RpcServiceFactory(clientFactory);
        // broker service.
        this.brokerServiceServer =
                new BrokerServiceServer(this, tubeConfig);
        // web server.
        this.webServer = new WebServer(tubeConfig.getHostName(), tubeConfig.getWebPort(), this);
        this.webServer.start();
        // used for communicate to master.
        this.masterService =
                rpcServiceFactory.getFailoverService(MasterService.class,
                        new MasterInfo(tubeConfig.getMasterAddressList()), rpcConfig);
        // used for heartbeat.
        this.scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "Broker Heartbeat Thread");
                        t.setPriority(Thread.MAX_PRIORITY);
                        return t;
                    }
                });
        this.sleeper = new Sleeper(3000, this);
        Runtime.getRuntime().addShutdownHook(this.shutdownHook);
    }

    public ConsumerNodeInfo getConsumerNodeInfo(String storeKey) {
        return this.brokerServiceServer.getConsumerNodeInfo(storeKey);
    }

    public OffsetService getOffsetManager() {
        return this.offsetManager;
    }

    public RpcServiceFactory getRpcServiceFactory() {
        return this.rpcServiceFactory;
    }

    public MetadataManage getMetadataManage() {
        return metadataManage;
    }

    public SimpleCertificateBrokerHandler getServerAuthHandler() {
        return serverAuthHandler;
    }

    @Override
    public boolean isStopped() {
        return this.shutdown.get();
    }

    public MessageStoreManager getStoreManager() {
        return this.storeManager;
    }

    public BrokerServiceServer getBrokerServiceServer() {
        return brokerServiceServer;
    }

    @Override
    public void run() {
        try {
            this.start();
            while (!this.shutdown.get()) {
                this.sleeper.sleep();
            }
        } catch (Exception e) {
            logger.error("Running exception.", e);
        }
        this.stop("Stop running.");
    }

    /***
     * Start broker service.
     *
     * @throws Exception
     */
    public synchronized void start() throws Exception {
        logger.info("Starting tube server...");
        if (!this.shutdown.get()) {
            return;
        }
        this.shutdown.set(false);
        // register to master, and heartbeat to master.
        this.register2Master();
        this.scheduledExecutorService.scheduleAtFixedRate(
                new Runnable() {
                    @Override
                    public void run() {
                        if (!shutdown.get()) {
                            long currErrCnt = heartbeatErrors.get();
                            if (currErrCnt > maxReleaseTryCnt) {
                                if (currErrCnt % 2 == 0) {
                                    heartbeatErrors.incrementAndGet();
                                    return;
                                }
                            }
                            try {
                                HeartResponseM2B response =
                                        masterService.brokerHeartbeatB2M(createBrokerHeartBeatRequest(),
                                                tubeConfig.getHostName(), false);
                                if (!response.getSuccess()) {
                                    if (response.getErrCode() == TErrCodeConstants.HB_NO_NODE) {
                                        register2Master();
                                        heartbeatErrors.set(0);
                                        logger.info("Re-register to master successfully!");
                                    }
                                    return;
                                }
                                heartbeatErrors.set(0);
                                FlowCtrlRuleHandler flowCtrlRuleHandler =
                                        metadataManage.getFlowCtrlRuleHandler();
                                long flowCheckId = flowCtrlRuleHandler.getFlowCtrlId();
                                long ssdTrnasLateId = flowCtrlRuleHandler.getSsdTranslateId();
                                int qryPriorityId = flowCtrlRuleHandler.getQryPriorityId();
                                ServiceStatusHolder
                                        .setReadWriteServiceStatus(response.getStopRead(),
                                                response.getStopWrite(), "Master");
                                if (response.hasFlowCheckId()) {
                                    qryPriorityId = response.hasQryPriorityId()
                                            ? response.getQryPriorityId() : qryPriorityId;
                                    if (response.getFlowCheckId() != flowCheckId) {
                                        flowCheckId = response.getFlowCheckId();
                                        ssdTrnasLateId = response.getSsdStoreId();
                                        try {
                                            flowCtrlRuleHandler
                                                    .updateDefFlowCtrlInfo(ssdTrnasLateId,
                                                            qryPriorityId, flowCheckId, response.getFlowControlInfo());
                                        } catch (Exception e1) {
                                            logger.warn(
                                                    "[HeartBeat response] found parse flowCtrl rules failure", e1);
                                        }
                                    }
                                    if (response.getSsdStoreId() != ssdTrnasLateId) {
                                        ssdTrnasLateId = response.getSsdStoreId();
                                        flowCtrlRuleHandler.setSsdTranslateId(ssdTrnasLateId);
                                    }
                                    if (qryPriorityId != flowCtrlRuleHandler.getQryPriorityId()) {
                                        flowCtrlRuleHandler.setQryPriorityId(qryPriorityId);
                                    }
                                }
                                requireReportConf = response.getNeedReportData();
                                StringBuilder sBuilder = new StringBuilder(512);
                                if (response.getTakeConfInfo()) {
                                    logger.info(sBuilder
                                            .append("[HeartBeat response] received broker metadata info: brokerConfId=")
                                            .append(response.getCurBrokerConfId())
                                            .append(",configCheckSumId=").append(response.getConfCheckSumId())
                                            .append(",hasFlowCtrl=").append(response.hasFlowCheckId())
                                            .append(",curFlowCtrlId=").append(flowCheckId)
                                            .append(",curSsdTrnasLateId=").append(ssdTrnasLateId)
                                            .append(",curQryPriorityId=").append(qryPriorityId)
                                            .append(",brokerDefaultConfInfo=")
                                            .append(response.getBrokerDefaultConfInfo())
                                            .append(",brokerTopicSetConfList=")
                                            .append(response.getBrokerTopicSetConfInfoList().toString()).toString());
                                    sBuilder.delete(0, sBuilder.length());
                                    metadataManage
                                            .updateBrokerTopicConfigMap(response.getCurBrokerConfId(),
                                                    response.getConfCheckSumId(), response.getBrokerDefaultConfInfo(),
                                                    response.getBrokerTopicSetConfInfoList(), false, sBuilder);
                                }
                                if (response.hasAuthorizedInfo()) {
                                    serverAuthHandler.appendVisitToken(response.getAuthorizedInfo());
                                }

                                boolean needProcess =
                                        metadataManage.updateBrokerRemoveTopicMap(
                                                response.getTakeRemoveTopicInfo(),
                                                response.getRemoveTopicConfInfoList(), sBuilder);
                                if (needProcess) {
                                    new Thread() {
                                        @Override
                                        public void run() {
                                            storeManager.removeTopicStore();
                                        }
                                    }.start();
                                }
                            } catch (Throwable t) {
                                heartbeatErrors.incrementAndGet();
                                samplePrintCtrl.printExceptionCaught(t);
                            }
                        }
                    }
                }, tubeConfig.getHeartbeatPeriodMs(), tubeConfig.getHeartbeatPeriodMs(),
                TimeUnit.MILLISECONDS);
        this.storeManager.start();
        this.brokerServiceServer.start();
        isOnline = true;
        logger.info(new StringBuilder(512)
                .append("Start tube server successfully, broker version=")
                .append(TubeServerVersion.BROKER_VERSION).toString());
    }

    public synchronized void reloadConfig() {
        this.tubeConfig.reload();
    }

    public void waitForServerOnline() {
        while (!isOnline() && !isStopped()) {
            sleeper.sleep();
        }
    }

    public boolean isOnline() {
        return this.isOnline;
    }

    public void stop(String why) {
        if (this.shutdown.get()) {
            return;
        }
        if (!shutdown.compareAndSet(false, true)) {
            return;
        }
        logger.info(why + ".Stopping Tube server...");
        try {
            TubeBroker.this.webServer.stop();
            logger.info("Tube WebService stopped.......");
            masterService.brokerCloseClientB2M(createMasterCloseRequest(),
                    tubeConfig.getHostName(), false);
            logger.info("Tube Closing to Master.....");
        } catch (Throwable e) {
            logger.warn("CloseBroker throw exception : ", e);
        }
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            //
        }
        logger.info("Tube Client StoreService stopping.....");
        TubeBroker.this.brokerServiceServer.stop();
        logger.info("Tube Client StoreService stopped.....");
        TubeBroker.this.storeManager.close();
        logger.info("Tube message store stopped.....");
        TubeBroker.this.offsetManager.close(-1);
        logger.info("Tube offset store stopped.....");
        scheduledExecutorService.shutdownNow();
        if (!shutdownHooked.get()) {
            Runtime.getRuntime().removeShutdownHook(TubeBroker.this.shutdownHook);
        }
        try {
            TubeBroker.this.rpcServiceFactory.destroy();
            TubeBroker.this.clientFactory.shutdown();
        } catch (Throwable e2) {
            logger.error("Stop rpcService failure", e2);
        }
        logger.info("Stop tube server successfully.");
        LogManager.shutdown();
        try {
            Thread.sleep(2000);
        } catch (Throwable ee) {
            //
        }
    }

    private String generateBrokerClientId() {
        return new StringBuilder(512).append(tubeConfig.getBrokerId()).append(":")
                .append(tubeConfig.getHostName()).append(":")
                .append(tubeConfig.getPort()).append(":")
                .append(TubeServerVersion.BROKER_VERSION).toString();
    }

    /***
     * Register to master. Try multi times if failed.
     *
     * @throws StartupException
     */
    private void register2Master() throws StartupException {
        int remainingRetry = 5;
        StringBuilder sBuilder = new StringBuilder(512);
        while (true) {
            try {
                final RegisterResponseM2B response =
                        masterService.brokerRegisterB2M(createMasterRegisterRequest(),
                                tubeConfig.getHostName(), false);
                if (!response.getSuccess()) {
                    logger.warn("Register to master failure, errInfo is " + response.getErrMsg());
                    throw new StartupException(sBuilder
                            .append("Register to master failed! The error message is ")
                            .append(response.getErrMsg()).toString());
                }
                ServiceStatusHolder
                        .setReadWriteServiceStatus(response.getStopRead(),
                                response.getStopWrite(), "Master");
                FlowCtrlRuleHandler flowCtrlRuleHandler =
                        metadataManage.getFlowCtrlRuleHandler();
                if (response.hasFlowCheckId()) {
                    int qryPriorityId = response.hasQryPriorityId()
                            ? response.getQryPriorityId() : flowCtrlRuleHandler.getQryPriorityId();
                    if (response.getFlowCheckId() != flowCtrlRuleHandler.getFlowCtrlId()) {
                        try {
                            flowCtrlRuleHandler
                                    .updateDefFlowCtrlInfo(response.getSsdStoreId(),
                                            response.getQryPriorityId(), response.getFlowCheckId(),
                                            response.getFlowControlInfo());
                        } catch (Exception e1) {
                            logger.warn(
                                    "[Register response] found parse flowCtrl rules failure", e1);
                        }
                    }
                    if (response.getSsdStoreId() != flowCtrlRuleHandler.getSsdTranslateId()) {
                        flowCtrlRuleHandler.setSsdTranslateId(response.getSsdStoreId());
                    }
                    if (qryPriorityId != flowCtrlRuleHandler.getQryPriorityId()) {
                        flowCtrlRuleHandler.setQryPriorityId(qryPriorityId);
                    }
                }
                EnableBrokerFunInfo enableFunInfo = response.getEnableBrokerInfo();
                if (enableFunInfo != null) {
                    if (enableFunInfo.getEnableProduceAuthenticate() != enableProduceAuthenticate.get()) {
                        enableProduceAuthenticate.set(enableFunInfo.getEnableProduceAuthenticate());
                    }
                    if (enableFunInfo.getEnableProduceAuthorize() != enableProduceAuthorize.get()) {
                        this.enableProduceAuthorize.set(enableFunInfo.getEnableProduceAuthorize());
                    }
                    if (enableFunInfo.getEnableConsumeAuthenticate() != enableConsumeAuthenticate.get()) {
                        this.enableConsumeAuthenticate.set(enableFunInfo.getEnableConsumeAuthenticate());
                    }
                    if (enableFunInfo.getEnableConsumeAuthorize() != getEnableConsumeAuthorize.get()) {
                        this.getEnableConsumeAuthorize.set(enableFunInfo.getEnableConsumeAuthorize());
                    }
                }

                serverAuthHandler.configure(enableProduceAuthenticate.get(), enableProduceAuthorize.get(),
                        enableConsumeAuthenticate.get(), getEnableConsumeAuthorize.get());
                if (response.hasAuthorizedInfo()) {
                    serverAuthHandler.appendVisitToken(response.getAuthorizedInfo());
                }
                logger.info(sBuilder
                        .append("[Register response] received broker metadata info: brokerConfId=")
                        .append(response.getCurBrokerConfId())
                        .append(",configCheckSumId=").append(response.getConfCheckSumId())
                        .append(",hasFlowCtrl=").append(response.hasFlowCheckId())
                        .append(",enableProduceAuthenticate=").append(this.enableProduceAuthenticate.get())
                        .append(",enableProduceAuthorize=").append(this.enableProduceAuthorize.get())
                        .append(",curFlowCtrlId=").append(flowCtrlRuleHandler.getFlowCtrlId())
                        .append(",curSsdTransLateId=").append(flowCtrlRuleHandler.getSsdTranslateId())
                        .append(",curQryPriorityId=").append(flowCtrlRuleHandler.getQryPriorityId())
                        .append(",brokerDefaultConfInfo=").append(response.getBrokerDefaultConfInfo())
                        .append(",brokerTopicSetConfList=")
                        .append(response.getBrokerTopicSetConfInfoList().toString()).toString());
                sBuilder.delete(0, sBuilder.length());
                metadataManage.updateBrokerTopicConfigMap(response.getCurBrokerConfId(),
                        response.getConfCheckSumId(), response.getBrokerDefaultConfInfo(),
                        response.getBrokerTopicSetConfInfoList(), true, sBuilder);
                break;

            } catch (Throwable e) {
                sBuilder.delete(0, sBuilder.length());
                remainingRetry--;
                if (remainingRetry == 0) {
                    throw new StartupException("Register to master failed!", e);
                }
            }
        }
    }

    /***
     * Build register request to master.
     *
     * @return
     * @throws Exception
     */
    private RegisterRequestB2M createMasterRegisterRequest() throws Exception {
        RegisterRequestB2M.Builder builder = RegisterRequestB2M.newBuilder();
        builder.setClientId(this.brokerId);
        builder.setBrokerOnline(isOnline);
        builder.setEnableTls(this.tubeConfig.isTlsEnable());
        builder.setTlsPort(this.tubeConfig.getTlsPort());
        builder.setReadStatusRpt(ServiceStatusHolder.getReadServiceReportStatus());
        builder.setWriteStatusRpt(ServiceStatusHolder.getWriteServiceReportStatus());
        builder.setCurBrokerConfId(metadataManage.getBrokerMetadataConfId());
        builder.setConfCheckSumId(metadataManage.getBrokerConfCheckSumId());
        FlowCtrlRuleHandler flowCtrlRuleHandler =
                metadataManage.getFlowCtrlRuleHandler();
        builder.setFlowCheckId(flowCtrlRuleHandler.getFlowCtrlId());
        builder.setQryPriorityId(flowCtrlRuleHandler.getQryPriorityId());
        builder.setSsdStoreId(flowCtrlRuleHandler.getSsdTranslateId());
        String brokerDefaultConfInfo = metadataManage.getBrokerDefMetaConfInfo();
        if (brokerDefaultConfInfo != null) {
            builder.setBrokerDefaultConfInfo(brokerDefaultConfInfo);
        }
        List<String> topicConfInfoList = metadataManage.getTopicMetaConfInfoLst();
        if (topicConfInfoList != null) {
            builder.addAllBrokerTopicSetConfInfo(topicConfInfoList);
        }
        ClientMaster.MasterCertificateInfo.Builder authInfoBuilder = genMasterCertificateInfo();
        if (authInfoBuilder != null) {
            builder.setAuthInfo(authInfoBuilder.build());
        }
        logger.info(new StringBuilder(512)
                .append("[Register request] current broker report info: brokerConfId=")
                .append(metadataManage.getBrokerMetadataConfId())
                .append(",isTlsEnable=").append(tubeConfig.isTlsEnable())
                .append(",TlsPort=").append(tubeConfig.getTlsPort())
                .append(",flowCtrlId=").append(flowCtrlRuleHandler.getFlowCtrlId())
                .append(",SSDTranslateId=").append(flowCtrlRuleHandler.getSsdTranslateId())
                .append(",QryPriorityId=").append(flowCtrlRuleHandler.getQryPriorityId())
                .append(",configCheckSumId=").append(metadataManage.getBrokerConfCheckSumId())
                .append(",brokerDefaultConfInfo=").append(brokerDefaultConfInfo)
                .append(",brokerTopicSetConfList=").append(topicConfInfoList).toString());
        return builder.build();
    }

    /***
     * Build heartbeat request to master.
     *
     * @return
     */
    private HeartRequestB2M createBrokerHeartBeatRequest() {
        HeartRequestB2M.Builder builder = HeartRequestB2M.newBuilder();
        builder.setBrokerId(String.valueOf(tubeConfig.getBrokerId()));
        builder.setBrokerOnline(isOnline);
        builder.setReadStatusRpt(ServiceStatusHolder.getReadServiceReportStatus());
        builder.setWriteStatusRpt(ServiceStatusHolder.getWriteServiceReportStatus());
        builder.setCurBrokerConfId(metadataManage.getBrokerMetadataConfId());
        builder.setConfCheckSumId(metadataManage.getBrokerConfCheckSumId());
        FlowCtrlRuleHandler flowCtrlRuleHandler =
                metadataManage.getFlowCtrlRuleHandler();
        builder.setFlowCheckId(flowCtrlRuleHandler.getFlowCtrlId());
        builder.setQryPriorityId(flowCtrlRuleHandler.getQryPriorityId());
        builder.setSsdStoreId(flowCtrlRuleHandler.getSsdTranslateId());
        builder.setTakeConfInfo(false);
        builder.setTakeRemovedTopicInfo(false);
        List<String> removedTopics = this.metadataManage.getHardRemovedTopics();
        if (!removedTopics.isEmpty()) {
            builder.setTakeRemovedTopicInfo(true);
            builder.addAllRemovedTopicsInfo(removedTopics);
        }
        ClientMaster.MasterCertificateInfo.Builder authInfoBuilder = genMasterCertificateInfo();
        if (authInfoBuilder != null) {
            builder.setAuthInfo(authInfoBuilder.build());
        }
        if (metadataManage.isBrokerMetadataChanged() || requireReportConf) {
            builder.setTakeConfInfo(true);
            builder.setBrokerDefaultConfInfo(metadataManage.getBrokerDefMetaConfInfo());
            builder.addAllBrokerTopicSetConfInfo(metadataManage.getTopicMetaConfInfoLst());
            logger.info(new StringBuilder(512)
                    .append("[HeartBeat request] current broker report info: brokerConfId=")
                    .append(metadataManage.getBrokerMetadataConfId())
                    .append(",lastReportedConfigId=").append(metadataManage.getLastRptBrokerMetaConfId())
                    .append(",configCheckSumId=").append(metadataManage.getBrokerConfCheckSumId())
                    .append(",brokerDefaultConfInfo=").append(metadataManage.getBrokerDefMetaConfInfo())
                    .append(",flowCtrlId=").append(flowCtrlRuleHandler.getFlowCtrlId())
                    .append(",SSDTranslateId=").append(flowCtrlRuleHandler.getSsdTranslateId())
                    .append(",QryPriorityId=").append(flowCtrlRuleHandler.getQryPriorityId())
                    .append(",ReadStatusRpt=").append(builder.getReadStatusRpt())
                    .append(",WriteStatusRpt=").append(builder.getWriteStatusRpt())
                    .append(",brokerTopicSetConfList=").append(metadataManage.getTopicMetaConfInfoLst()).toString());
            metadataManage.setLastRptBrokerMetaConfId(metadataManage.getBrokerMetadataConfId());
            requireReportConf = false;
        }
        return builder.build();
    }

    /***
     * Build close request to master.
     *
     * @return
     */
    private CloseRequestB2M createMasterCloseRequest() {
        CloseRequestB2M.Builder builder = CloseRequestB2M.newBuilder();
        builder.setBrokerId(String.valueOf(tubeConfig.getBrokerId()));
        ClientMaster.MasterCertificateInfo.Builder authInfoBuilder = genMasterCertificateInfo();
        if (authInfoBuilder != null) {
            builder.setAuthInfo(authInfoBuilder.build());
        }
        return builder.build();
    }

    /***
     * Build master certificate info.
     *
     * @return
     */
    private ClientMaster.MasterCertificateInfo.Builder genMasterCertificateInfo() {
        ClientMaster.MasterCertificateInfo.Builder authInfoBuilder = null;
        if (tubeConfig.isVisitMasterAuth()) {
            authInfoBuilder = ClientMaster.MasterCertificateInfo.newBuilder();
            authInfoBuilder.setAuthInfo(clientAuthHandler
                    .genMasterAuthenticateToken(tubeConfig.getVisitName(),
                            tubeConfig.getVisitPassword()));
        }
        return authInfoBuilder;
    }

    /***
     * Shutdown hook.
     */
    private final class ShutdownHook extends Thread {
        @Override
        public void run() {
            if (shutdownHooked.compareAndSet(false, true)) {
                TubeBroker.this.stop("Shutdown by Hook");
            }
        }
    }
}

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

import com.tencent.tubemq.corebase.Message;
import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.TErrCodeConstants;
import com.tencent.tubemq.corebase.TokenConstants;
import com.tencent.tubemq.corebase.cluster.Partition;
import com.tencent.tubemq.corebase.config.TLSConfig;
import com.tencent.tubemq.corebase.protobuf.generated.ClientBroker.CommitOffsetRequestC2B;
import com.tencent.tubemq.corebase.protobuf.generated.ClientBroker.CommitOffsetResponseB2C;
import com.tencent.tubemq.corebase.protobuf.generated.ClientBroker.GetMessageRequestC2B;
import com.tencent.tubemq.corebase.protobuf.generated.ClientBroker.GetMessageResponseB2C;
import com.tencent.tubemq.corebase.protobuf.generated.ClientBroker.HeartBeatRequestC2B;
import com.tencent.tubemq.corebase.protobuf.generated.ClientBroker.HeartBeatResponseB2C;
import com.tencent.tubemq.corebase.protobuf.generated.ClientBroker.RegisterRequestC2B;
import com.tencent.tubemq.corebase.protobuf.generated.ClientBroker.RegisterResponseB2C;
import com.tencent.tubemq.corebase.protobuf.generated.ClientBroker.SendMessageRequestP2B;
import com.tencent.tubemq.corebase.protobuf.generated.ClientBroker.SendMessageResponseB2P;
import com.tencent.tubemq.corebase.protobuf.generated.ClientBroker.TransferedMessage;
import com.tencent.tubemq.corebase.utils.AddressUtils;
import com.tencent.tubemq.corebase.utils.CheckSum;
import com.tencent.tubemq.corebase.utils.DataConverterUtil;
import com.tencent.tubemq.corebase.utils.ServiceStatusHolder;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.corerpc.RpcConfig;
import com.tencent.tubemq.corerpc.RpcConstants;
import com.tencent.tubemq.corerpc.service.BrokerReadService;
import com.tencent.tubemq.corerpc.service.BrokerWriteService;
import com.tencent.tubemq.server.Server;
import com.tencent.tubemq.server.broker.metadata.MetadataManage;
import com.tencent.tubemq.server.broker.msgstore.MessageStore;
import com.tencent.tubemq.server.broker.msgstore.MessageStoreManager;
import com.tencent.tubemq.server.broker.msgstore.disk.GetMessageResult;
import com.tencent.tubemq.server.broker.nodeinfo.ConsumerNodeInfo;
import com.tencent.tubemq.server.broker.offset.OffsetService;
import com.tencent.tubemq.server.broker.stats.CountService;
import com.tencent.tubemq.server.broker.stats.GroupCountService;
import com.tencent.tubemq.server.common.TServerConstants;
import com.tencent.tubemq.server.common.TStatusConstants;
import com.tencent.tubemq.server.common.aaaserver.CertificateBrokerHandler;
import com.tencent.tubemq.server.common.aaaserver.CertifiedResult;
import com.tencent.tubemq.server.common.exception.HeartbeatException;
import com.tencent.tubemq.server.common.heartbeat.HeartbeatManager;
import com.tencent.tubemq.server.common.heartbeat.TimeoutInfo;
import com.tencent.tubemq.server.common.heartbeat.TimeoutListener;
import com.tencent.tubemq.server.common.offsetstorage.OffsetStorageInfo;
import com.tencent.tubemq.server.common.paramcheck.PBParameterUtils;
import com.tencent.tubemq.server.common.paramcheck.ParamCheckResult;
import com.tencent.tubemq.server.common.utils.IdWorker;
import com.tencent.tubemq.server.common.utils.RowLock;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Broker service. Receive and conduct client's request, store messages, query messages, print statistics, etc.
 */
public class BrokerServiceServer implements BrokerReadService, BrokerWriteService, Server {
    private static final Logger logger =
            LoggerFactory.getLogger(BrokerServiceServer.class);
    private final TubeBroker tubeBroker;
    private final BrokerConfig tubeConfig;
    // registered consumers. format : consumer group - topic - partition id  --> consumer info
    private final ConcurrentHashMap<String/* group:topic-partitionId */, ConsumerNodeInfo> consumerRegisterMap =
            new ConcurrentHashMap<String, ConsumerNodeInfo>();
    // metadata manager.
    private final MetadataManage metadataManage;
    // offset storage manager.
    private final OffsetService offsetManager;
    // message storage manager.
    private final MessageStoreManager storeManager;
    // heartbeat manager.
    private final HeartbeatManager heartbeatManager;
    // sequencer id generator.
    private final IdWorker idWorker;
    // row lock.
    private final RowLock brokerRowLock;
    // statistics of produce.
    private final CountService putCounterGroup;
    // statistics of consume.
    private final CountService getCounterGroup;
    // certificate handler.
    private final CertificateBrokerHandler serverAuthHandler;
    // consumer timeout listener.
    private final ConsumerTimeoutListener consumerListener =
            new ConsumerTimeoutListener();
    // status of broker service.
    private boolean started = false;


    public BrokerServiceServer(final TubeBroker tubeBroker,
                               final BrokerConfig tubeConfig) {
        this.tubeConfig = tubeConfig;
        this.tubeBroker = tubeBroker;
        this.metadataManage = tubeBroker.getMetadataManage();
        this.storeManager = tubeBroker.getStoreManager();
        this.offsetManager = tubeBroker.getOffsetManager();
        this.serverAuthHandler = tubeBroker.getServerAuthHandler();
        ServiceStatusHolder.setStatisParameters(tubeConfig.getAllowedReadIOExcptCnt(),
                tubeConfig.getAllowedWriteIOExcptCnt(), tubeConfig.getIoExcptStatsDurationMs());
        this.idWorker = new IdWorker(0);
        this.putCounterGroup = new GroupCountService("PutCounterGroup", "Producer", 60 * 1000);
        this.getCounterGroup = new GroupCountService("GetCounterGroup", "Consumer", 60 * 1000);
        this.heartbeatManager = new HeartbeatManager();
        this.brokerRowLock =
                new RowLock("Broker-RowLock", this.tubeConfig.getRowLockWaitDurMs());
        heartbeatManager.regConsumerCheckBusiness(
                this.tubeConfig.getConsumerRegTimeoutMs(), consumerListener);
    }

    /***
     * Start broker service
     *
     * @throws Exception
     */
    @Override
    public void start() throws Exception {
        RpcConfig rpcWriteConfig = new RpcConfig();
        rpcWriteConfig.put(RpcConstants.NETTY_TCP_SENDBUF,
                this.tubeConfig.getSocketSendBuffer());
        rpcWriteConfig.put(RpcConstants.NETTY_TCP_RECEIVEBUF,
                this.tubeConfig.getSocketRecvBuffer());
        rpcWriteConfig.put(RpcConstants.WORKER_COUNT, this.tubeConfig.getTcpWriteServiceThread());
        tubeBroker.getRpcServiceFactory().publishService(BrokerWriteService.class,
                this, tubeConfig.getPort(), rpcWriteConfig);
        RpcConfig rpcReadConfig = new RpcConfig();
        rpcReadConfig.put(RpcConstants.NETTY_TCP_SENDBUF,
                this.tubeConfig.getSocketSendBuffer());
        rpcReadConfig.put(RpcConstants.NETTY_TCP_RECEIVEBUF,
                this.tubeConfig.getSocketRecvBuffer());
        rpcReadConfig.put(RpcConstants.WORKER_COUNT, this.tubeConfig.getTcpReadServiceThread());
        tubeBroker.getRpcServiceFactory().publishService(BrokerReadService.class,
                this, tubeConfig.getPort(), rpcReadConfig);
        if (this.tubeConfig.isTlsEnable()) {
            // add tls config if enable tls. support tcp and tls in different port.
            TLSConfig tlsConfig = this.tubeConfig.getTlsConfig();
            RpcConfig rpcTLSWriteConfig = new RpcConfig();
            rpcTLSWriteConfig.put(RpcConstants.TLS_OVER_TCP, true);
            rpcTLSWriteConfig.put(RpcConstants.NETTY_TCP_SENDBUF,
                    this.tubeConfig.getSocketSendBuffer());
            rpcTLSWriteConfig.put(RpcConstants.NETTY_TCP_RECEIVEBUF,
                    this.tubeConfig.getSocketRecvBuffer());
            rpcTLSWriteConfig.put(RpcConstants.WORKER_COUNT,
                    this.tubeConfig.getTlsWriteServiceThread());
            rpcTLSWriteConfig.put(RpcConstants.TLS_KEYSTORE_PATH,
                    tlsConfig.getTlsKeyStorePath());
            rpcTLSWriteConfig.put(RpcConstants.TLS_KEYSTORE_PASSWORD,
                    tlsConfig.getTlsKeyStorePassword());
            rpcTLSWriteConfig.put(RpcConstants.TLS_TWO_WAY_AUTHENTIC,
                    tlsConfig.isTlsTwoWayAuthEnable());
            if (tlsConfig.isTlsTwoWayAuthEnable()) {
                rpcTLSWriteConfig.put(RpcConstants.TLS_TRUSTSTORE_PATH,
                        tlsConfig.getTlsTrustStorePath());
                rpcTLSWriteConfig.put(RpcConstants.TLS_TRUSTSTORE_PASSWORD,
                        tlsConfig.getTlsTrustStorePassword());
            }
            // publish service
            tubeBroker.getRpcServiceFactory().publishService(BrokerWriteService.class,
                    this, tubeConfig.getTlsPort(), rpcTLSWriteConfig);
            RpcConfig rpcTLSReadConfig = new RpcConfig();
            rpcTLSReadConfig.put(RpcConstants.WORKER_COUNT,
                    this.tubeConfig.getTlsReadServiceThread());
            rpcTLSReadConfig.put(RpcConstants.TLS_OVER_TCP, true);
            rpcTLSReadConfig.put(RpcConstants.NETTY_TCP_SENDBUF,
                    this.tubeConfig.getSocketSendBuffer());
            rpcTLSReadConfig.put(RpcConstants.NETTY_TCP_RECEIVEBUF,
                    this.tubeConfig.getSocketRecvBuffer());
            rpcTLSReadConfig.put(RpcConstants.TLS_KEYSTORE_PATH,
                    tlsConfig.getTlsKeyStorePath());
            rpcTLSReadConfig.put(RpcConstants.TLS_KEYSTORE_PASSWORD,
                    tlsConfig.getTlsKeyStorePassword());
            rpcTLSReadConfig.put(RpcConstants.TLS_TWO_WAY_AUTHENTIC,
                    tlsConfig.isTlsTwoWayAuthEnable());
            if (tlsConfig.isTlsTwoWayAuthEnable()) {
                rpcTLSReadConfig.put(RpcConstants.TLS_TRUSTSTORE_PATH,
                        tlsConfig.getTlsTrustStorePath());
                rpcTLSReadConfig.put(RpcConstants.TLS_TRUSTSTORE_PASSWORD,
                        tlsConfig.getTlsTrustStorePassword());
            }
            tubeBroker.getRpcServiceFactory().publishService(BrokerReadService.class,
                    this, tubeConfig.getTlsPort(), rpcTLSReadConfig);
        }
        this.started = true;
    }

    /***
     * Stop broker service.
     */
    @Override
    public void stop() {
        if (!started) {
            return;
        }
        this.started = false;
        heartbeatManager.stop();
        putCounterGroup.close(-1);
        getCounterGroup.close(-1);
        logger.info("BrokerService server stopped");
    }

    /***
     * Get broker's registered consumer info
     *
     * @return
     */
    public Map<String, ConsumerNodeInfo> getConsumerRegisterMap() {
        return consumerRegisterMap;
    }

    /***
     * Get consumer's info by store key.
     *
     * @param storeKey
     * @return
     */
    public ConsumerNodeInfo getConsumerNodeInfo(String storeKey) {
        return consumerRegisterMap.get(storeKey);
    }

    /***
     * Get consumer's register time.
     *
     * @param consumerId
     * @param partitionStr
     * @return
     */
    public Long getConsumerRegisterTime(String consumerId, String partitionStr) {
        TimeoutInfo timeoutInfo =
                heartbeatManager.getConsumerRegMap()
                        .get(getHeartbeatNodeId(consumerId, partitionStr));
        if (timeoutInfo == null) {
            return null;
        }
        return (timeoutInfo.getTimeoutTime() - heartbeatManager.getConsumerTimeoutDlt());
    }

    /***
     * Handle consumer's getMessageRequest.
     *
     * @param request
     * @param rmtAddress
     * @param overtls
     * @return
     * @throws Throwable
     */
    @Override
    public GetMessageResponseB2C getMessagesC2B(GetMessageRequestC2B request,
                                                final String rmtAddress,
                                                boolean overtls) throws Throwable {
        final GetMessageResponseB2C.Builder builder =
                GetMessageResponseB2C.newBuilder();
        builder.setSuccess(false);
        builder.setCurrOffset(-1);
        builder.setEscFlowCtrl(false);
        builder.setCurrDataDlt(-1);
        builder.setMinLimitTime(0);
        StringBuilder strBuffer = new StringBuilder(512);
        if (ServiceStatusHolder.isReadServiceStop()) {
            builder.setErrCode(TErrCodeConstants.SERVICE_UNAVILABLE);
            builder.setErrMsg("Read StoreService temporary unavilable!");
            return builder.build();
        }
        // check request's parameters
        ParamCheckResult paramCheckResult =
                PBParameterUtils.checkClientId(request.getClientId(), strBuffer);
        if (!paramCheckResult.result) {
            builder.setErrCode(paramCheckResult.errCode);
            builder.setErrMsg(paramCheckResult.errMsg);
            return builder.build();
        }
        final String clientId = (String) paramCheckResult.checkData;
        paramCheckResult =
                PBParameterUtils.checkGroupName(request.getGroupName(), strBuffer);
        if (!paramCheckResult.result) {
            builder.setErrCode(paramCheckResult.errCode);
            builder.setErrMsg(paramCheckResult.errMsg);
            return builder.build();
        }
        final String groupName = (String) paramCheckResult.checkData;
        paramCheckResult =
                PBParameterUtils.checkConsumeTopicName(request.getTopicName(), this.metadataManage, strBuffer);
        if (!paramCheckResult.result) {
            builder.setErrCode(paramCheckResult.errCode);
            builder.setErrMsg(paramCheckResult.errMsg);
            return builder.build();
        }
        // get consumer info
        final String topicName = (String) paramCheckResult.checkData;
        final int partitionId = request.getPartitionId();
        boolean isEscFlowCtrl = request.hasEscFlowCtrl() && request.getEscFlowCtrl();
        String partStr = getPartStr(groupName, topicName, partitionId);
        String consumerId = null;
        ConsumerNodeInfo consumerNodeInfo = consumerRegisterMap.get(partStr);
        if (consumerNodeInfo != null) {
            consumerId = consumerNodeInfo.getConsumerId();
        }
        if (consumerId == null) {
            logger.warn(strBuffer.append("[UnRegistered Consumer]").append(clientId)
                    .append(TokenConstants.SEGMENT_SEP).append(partStr).toString());
            strBuffer.delete(0, strBuffer.length());
            builder.setErrCode(TErrCodeConstants.HB_NO_NODE);
            builder.setErrMsg(strBuffer.append("UnRegistered Consumer:")
                    .append(clientId)
                    .append(", you have to register firstly!").toString());
            return builder.build();
        }
        if (!clientId.equals(consumerId)) {
            strBuffer.append("[Duplicated Request] Partition=").append(partStr)
                    .append(" of Broker=").append(tubeConfig.getBrokerId())
                    .append(" has been consumed by ").append(consumerId)
                    .append(";Current consumer ").append(clientId);
            logger.warn(strBuffer.toString());
            builder.setErrCode(TErrCodeConstants.DUPLICATE_PARTITION);
            builder.setErrMsg(strBuffer.toString());
            return builder.build();
        }
        String rmtAddrInfo = consumerNodeInfo.getRmtAddrInfo();
        try {
            heartbeatManager.updConsumerNode(getHeartbeatNodeId(clientId, partStr));
        } catch (HeartbeatException e) {
            logger.warn(strBuffer.append("[Invalid Request]").append(clientId)
                    .append(TokenConstants.SEGMENT_SEP).append(topicName)
                    .append(TokenConstants.ATTR_SEP).append(partitionId).toString());
            builder.setErrCode(TErrCodeConstants.HB_NO_NODE);
            builder.setErrMsg(e.getMessage());
            return builder.build();
        }
        Integer topicStatusId = this.metadataManage.getCosedTopicStatusId(topicName);
        if ((topicStatusId != null)
                && (topicStatusId > TStatusConstants.STATUS_TOPIC_SOFT_DELETE)) {
            strBuffer.append("[Partition Closed] Partition has been closed, for topic=")
                    .append(topicName).append(",partitionId=").append(partitionId)
                    .append(" of Broker=").append(tubeConfig.getBrokerId());
            logger.warn(strBuffer.toString());
            builder.setErrCode(TErrCodeConstants.FORBIDDEN);
            builder.setErrMsg(strBuffer.toString());
            return builder.build();
        }
        // query data from store manager.
        boolean isGetStore = false;
        MessageStore dataStore = null;
        try {
            dataStore = this.storeManager.getOrCreateMessageStore(topicName, partitionId);
            isGetStore = true;
            GetMessageResult msgResult =
                    getMessages(dataStore, consumerNodeInfo, groupName, topicName, partitionId,
                            request.getLastPackConsumed(), request.getManualCommitOffset(),
                            clientId, this.tubeConfig.getHostName(), rmtAddrInfo, isEscFlowCtrl, strBuffer);
            if (msgResult.isSuccess) {
                consumerNodeInfo.setLastProcInfo(System.currentTimeMillis(),
                        msgResult.lastRdDataOffset,
                        msgResult.totalMsgSize);
                getCounterGroup.add(msgResult.tmpCounters);
                if (msgResult.isFromSsdFile) {
                    builder.setEscFlowCtrl(true);
                } else {
                    builder.setEscFlowCtrl(false);
                }
                builder.setRequireSlow(msgResult.isSlowFreq);
                builder.setSuccess(true);
                builder.setErrCode(TErrCodeConstants.SUCCESS);
                builder.setCurrOffset(msgResult.reqOffset);
                builder.setCurrDataDlt(msgResult.waitTime);
                builder.setErrMsg("OK!");
                builder.addAllMessages(msgResult.transferedMessageList);
                return builder.build();
            } else {
                builder.setErrCode(msgResult.getRetCode());
                builder.setErrMsg(msgResult.errInfo);
                builder.setMinLimitTime((int) msgResult.waitTime);
                return builder.build();
            }
        } catch (Throwable ee) {
            strBuffer.delete(0, strBuffer.length());
            builder.setErrCode(TErrCodeConstants.INTERNAL_SERVER_ERROR);
            if (isGetStore) {
                strBuffer.append("[GetMessage] Throwable error while getMessage,")
                        .append(ee.getMessage()).append(", position is")
                        .append(this.tubeConfig.getBrokerId())
                        .append(TokenConstants.ATTR_SEP).append(topicName)
                        .append(TokenConstants.ATTR_SEP).append(partitionId);
                logger.error(strBuffer.toString(), ee);
                builder.setErrMsg(ee.getMessage() == null ? strBuffer.toString() : ee.getMessage());
            } else {
                builder.setErrMsg(strBuffer.append("Get the store of topic ")
                        .append(topicName).append(" in partition ")
                        .append(partitionId).append(" failure!").toString());
            }
            return builder.build();
        }
    }

    /***
     * Query offset, then read data.
     *
     * @param msgStore
     * @param consumerNodeInfo
     * @param group
     * @param topic
     * @param partitionId
     * @param lastConsumed
     * @param isManualCommitOffset
     * @param sentAddr
     * @param brokerAddr
     * @param rmtAddrInfo
     * @param isEscFlowCtrl
     * @param sb
     * @return
     * @throws IOException
     */
    private GetMessageResult getMessages(final MessageStore msgStore,
                                         final ConsumerNodeInfo consumerNodeInfo,
                                         final String group, final String topic,
                                         final int partitionId, final boolean lastConsumed,
                                         final boolean isManualCommitOffset, final String sentAddr,
                                         final String brokerAddr, final String rmtAddrInfo,
                                         boolean isEscFlowCtrl, final StringBuilder sb) throws IOException {
        long requestOffset =
                offsetManager.getOffset(msgStore, group, topic,
                        partitionId, isManualCommitOffset, lastConsumed, sb);
        if (requestOffset < 0) {
            return new GetMessageResult(false, TErrCodeConstants.NOT_FOUND,
                    -requestOffset, 0, "The request offset reached maxOffset!");
        }
        final long maxDataOffset = msgStore.getDataMaxOffset();
        int reqSwitch = consumerNodeInfo.getQryPriorityId() <= 0
                ? (metadataManage.getFlowCtrlRuleHandler().getQryPriorityId() <= 0
                ? TServerConstants.CFG_DEFAULT_CONSUME_RULE
                : metadataManage.getFlowCtrlRuleHandler().getQryPriorityId())
                : consumerNodeInfo.getQryPriorityId();
        int msgDataSizeLimit = consumerNodeInfo.getCurrentAllowedSize(msgStore.getStoreKey(),
                metadataManage.getFlowCtrlRuleHandler(), maxDataOffset,
                this.storeManager.getMaxMsgTransferSize(), isEscFlowCtrl);
        if (msgDataSizeLimit <= 0) {
            if (consumerNodeInfo.isSupportLimit()) {
                return new GetMessageResult(false, TErrCodeConstants.SERVER_CONSUME_SPEED_LIMIT,
                        requestOffset, 0, (-msgDataSizeLimit), "RpcServer consume speed limit!");
            } else {
                return new GetMessageResult(false, TErrCodeConstants.NOT_FOUND,
                        requestOffset, 0, "RpcServer consume speed limit!");
            }
        }
        try {
            String baseKey = sb.append(topic).append("#").append(brokerAddr)
                    .append("#").append(sentAddr).append("#").append(rmtAddrInfo)
                    .append("#").append(group).append("#").append(partitionId).toString();
            sb.delete(0, sb.length());
            GetMessageResult msgQueryResult =
                    msgStore.getMessages(reqSwitch, requestOffset,
                            partitionId, consumerNodeInfo, baseKey, msgDataSizeLimit);
            offsetManager.bookOffset(group, topic, partitionId,
                    msgQueryResult.lastReadOffset, isManualCommitOffset,
                    msgQueryResult.transferedMessageList.isEmpty(), sb);
            msgQueryResult.setWaitTime(maxDataOffset - msgQueryResult.lastRdDataOffset);
            return msgQueryResult;
        } catch (Throwable e1) {
            sb.delete(0, sb.length());
            logger.warn(sb.append("[Store Manager] get message failure, requestOffset=")
                    .append(requestOffset).append(",group=").append(group).append(",topic=").append(topic)
                    .append(",partitionId=").append(partitionId).toString(), e1);
            sb.delete(0, sb.length());
            return new GetMessageResult(false, TErrCodeConstants.INTERNAL_SERVER_ERROR,
                    requestOffset, 0, sb.append("Get message failure, errMsg=")
                    .append(e1.getMessage()).toString());
        }
    }

    /***
     * Get message snapshot by given parameters.
     *
     * @param topicName
     * @param partitionId
     * @param msgCount
     * @param filterCondSet
     * @param sb
     * @return
     * @throws Exception
     */
    public StringBuilder getMessageSnapshot(String topicName, int partitionId,
                                            int msgCount, final Set<String> filterCondSet,
                                            final StringBuilder sb) throws Exception {
        MessageStore dataStore = null;
        try {
            if (partitionId == -1) {
                final Collection<MessageStore> msgStores =
                        storeManager.getMessageStoresByTopic(topicName);
                if ((msgStores == null) || (msgStores.isEmpty())) {
                    sb.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                            .append("Invalid parameter: not found the store by topicName(")
                            .append(topicName).append(")!\"}");
                    return sb;
                }
                for (final MessageStore msgStore : msgStores) {
                    dataStore = msgStore;
                    if (dataStore != null) {
                        partitionId = msgStore.getStoreId() * TBaseConstants.META_STORE_INS_BASE;
                        break;
                    }
                }
                if (dataStore == null) {
                    sb.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                            .append("Invalid parameter: all store is null by topicName(")
                            .append(topicName).append(")!\"}");
                    return sb;
                }
            } else {
                dataStore = storeManager.getOrCreateMessageStore(topicName, partitionId);
                if (dataStore == null) {
                    sb.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                            .append("Invalid parameter: not found the store by topicName + partitionId(")
                            .append(topicName).append(":").append(partitionId).append(")!\"}");
                    return sb;
                }
            }
            GetMessageResult getMessageResult =
                    storeManager.getMessages(dataStore, topicName, partitionId, msgCount, filterCondSet);
            if ((getMessageResult.transferedMessageList == null)
                    || (getMessageResult.transferedMessageList.isEmpty())) {
                sb.append("{\"result\":false,\"errCode\":401,\"errMsg\":\"")
                        .append("Could not find message at position by topic (")
                        .append(topicName).append(")!\"}");
                return sb;
            } else {
                List<String> transferedMessageList = new ArrayList<String>();
                List<TransferedMessage> tmpMsgList = getMessageResult.transferedMessageList;
                List<Message> messageList = DataConverterUtil.convertMessage(topicName, tmpMsgList);
                int startPos = messageList.size() - msgCount < 0 ? 0 : messageList.size() - msgCount;
                for (; startPos < messageList.size(); startPos++) {
                    String msgItem = new String(
                            Base64.encodeBase64(messageList.get(startPos).getData()));
                    transferedMessageList.add(msgItem);
                }
                int i = 0;
                sb.append("{\"result\":true,\"errCode\":200,\"errMsg\":\"Success!\",\"dataSet\":[");
                for (String msgData : transferedMessageList) {
                    if (i > 0) {
                        sb.append(",");
                    }
                    sb.append("{\"index\":").append(i++)
                            .append(",\"data\":\"").append(msgData).append("\"}");
                }
                sb.append("]}");
                return sb;
            }
        } catch (Throwable ee) {
            sb.append("{\"result\":false,\"errCode\":501,\"errMsg\":\"Get Message failure, exception is ")
                    .append(ee.getMessage()).append("\"}");
            return sb;
        }
    }

    /***
     * Handle producer's sendMessage request.
     *
     * @param request
     * @param rmtAddress
     * @param overtls
     * @return
     * @throws Throwable
     */
    @Override
    public SendMessageResponseB2P sendMessageP2B(SendMessageRequestP2B request,
                                                 final String rmtAddress,
                                                 boolean overtls) throws Throwable {
        final StringBuilder strBuffer = new StringBuilder(512);
        SendMessageResponseB2P.Builder builder = SendMessageResponseB2P.newBuilder();
        builder.setSuccess(false);
        if (ServiceStatusHolder.isWriteServiceStop()) {
            builder.setErrCode(TErrCodeConstants.SERVICE_UNAVILABLE);
            builder.setErrMsg("Write StoreService temporary unavilable!");
            return builder.build();
        }
        CertifiedResult certResult =
                serverAuthHandler.identityValidUserInfo(request.getAuthInfo(), true);
        if (!certResult.result) {
            builder.setErrCode(certResult.errCode);
            builder.setErrMsg(certResult.errInfo);
            return builder.build();
        }
        ParamCheckResult paramCheckResult =
                PBParameterUtils.checkClientId(request.getClientId(), strBuffer);
        if (!paramCheckResult.result) {
            builder.setErrCode(paramCheckResult.errCode);
            builder.setErrMsg(paramCheckResult.errMsg);
            return builder.build();
        }
        final String producerId = (String) paramCheckResult.checkData;
        paramCheckResult =
                PBParameterUtils.checkExistTopicNameInfo(request.getTopicName(),
                        request.getPartitionId(), this.metadataManage, strBuffer);
        if (!paramCheckResult.result) {
            builder.setErrCode(paramCheckResult.errCode);
            builder.setErrMsg(paramCheckResult.errMsg);
            return builder.build();
        }
        final String reqTopic = (String) paramCheckResult.checkData;
        final int partition = request.getPartitionId();
        String msgType = null;
        int msgTypeCode = -1;
        if (TStringUtils.isNotBlank(request.getMsgType())) {
            msgType = request.getMsgType().trim();
            msgTypeCode = msgType.hashCode();
        }
        final byte[] msgData = request.getData().toByteArray();
        final int dataLength = msgData.length;
        if (dataLength <= 0) {
            builder.setErrCode(TErrCodeConstants.BAD_REQUEST);
            builder.setErrMsg("data length is zero!");
            return builder.build();
        }
        if (dataLength > TBaseConstants.META_MAX_MESSAGEG_DATA_SIZE + 1024) {
            builder.setErrCode(TErrCodeConstants.BAD_REQUEST);
            builder.setErrMsg(strBuffer.append("data length over max length, allowed max length is ")
                    .append(TBaseConstants.META_MAX_MESSAGEG_DATA_SIZE + 1024)
                    .append(", data length is ").append(dataLength).toString());
            return builder.build();
        }
        int checkSum = CheckSum.crc32(msgData);
        if (request.getCheckSum() != -1) {
            if (checkSum != request.getCheckSum()) {
                builder.setErrCode(TErrCodeConstants.FORBIDDEN);
                builder.setErrMsg(strBuffer.append("Checksum msg data failure: ")
                        .append(request.getCheckSum()).append(" of ").append(reqTopic)
                        .append(" not equal to the data's checksum of ")
                        .append(checkSum).toString());
                return builder.build();
            }
        }
        CertifiedResult authorizeResult =
                serverAuthHandler.validProduceAuthorizeInfo(
                        certResult.userName, reqTopic, msgType, rmtAddress);
        if (!authorizeResult.result) {
            builder.setErrCode(authorizeResult.errCode);
            builder.setErrMsg(authorizeResult.errInfo);
            return builder.build();
        }
        try {
            final MessageStore store =
                    this.storeManager.getOrCreateMessageStore(reqTopic, partition);
            final long messageId = this.idWorker.nextId();
            if (store.appendMsg(messageId, dataLength, checkSum, msgData,
                    msgTypeCode, request.getFlag(), partition, request.getSentAddr())) {
                String baseKey = strBuffer.append(reqTopic)
                        .append("#").append(AddressUtils.intToIp(request.getSentAddr()))
                        .append("#").append(tubeConfig.getHostName())
                        .append("#").append(request.getPartitionId())
                        .append("#").append(request.getMsgTime()).toString();
                putCounterGroup.add(baseKey, 1L, dataLength);
                builder.setSuccess(true);
                builder.setRequireAuth(certResult.reAuth);
                builder.setErrCode(TErrCodeConstants.SUCCESS);
                builder.setErrMsg(String.valueOf(messageId));
                return builder.build();
            } else {
                builder.setErrCode(TErrCodeConstants.SERVER_RECEIVE_OVERFLOW);
                builder.setErrMsg(strBuffer.append("Put message failed from ")
                        .append(tubeConfig.getHostName())
                        .append(", server receive message overflow!").toString());
                return builder.build();
            }
        } catch (final Exception e) {
            logger.error("Put message failed ", e);
            strBuffer.delete(0, strBuffer.length());
            builder.setSuccess(false);
            builder.setErrCode(TErrCodeConstants.INTERNAL_SERVER_ERROR);
            builder.setErrMsg(strBuffer.append("Put message failed from ")
                    .append(tubeConfig.getHostName()).append(" ")
                    .append((e.getMessage() != null ? e.getMessage() : " ")).toString());
            return builder.build();
        } catch (final Throwable ee) {
            logger.error("Put message failed2 ", ee);
            strBuffer.delete(0, strBuffer.length());
            builder.setSuccess(false);
            builder.setErrCode(TErrCodeConstants.INTERNAL_SERVER_ERROR);
            builder.setErrMsg(strBuffer.append("Put message failed2 from ")
                    .append(tubeConfig.getHostName()).append(" ")
                    .append((ee.getMessage() != null ? ee.getMessage() : " ")).toString());
            return builder.build();
        }
    }

    /***
     * Handle consumer register request.
     *
     * @param request
     * @param rmtAddress
     * @param overtls
     * @return
     * @throws Throwable
     */
    @Override
    public RegisterResponseB2C consumerRegisterC2B(RegisterRequestC2B request,
                                                   final String rmtAddress,
                                                   boolean overtls) throws Throwable {
        final StringBuilder strBuffer = new StringBuilder(512);
        RegisterResponseB2C.Builder builder = RegisterResponseB2C.newBuilder();
        builder.setSuccess(false);
        builder.setCurrOffset(-1);
        CertifiedResult certResult = serverAuthHandler.identityValidUserInfo(request.getAuthInfo(), false);
        if (!certResult.result) {
            builder.setErrCode(certResult.errCode);
            builder.setErrMsg(certResult.errInfo);
            return builder.build();
        }
        ParamCheckResult paramCheckResult = PBParameterUtils.checkClientId(request.getClientId(), strBuffer);
        if (!paramCheckResult.result) {
            builder.setErrCode(paramCheckResult.errCode);
            builder.setErrMsg(paramCheckResult.errMsg);
            return builder.build();
        }
        final String clientId = (String) paramCheckResult.checkData;
        paramCheckResult
                = PBParameterUtils.checkConsumeTopicName(request.getTopicName(), this.metadataManage, strBuffer);
        if (!paramCheckResult.result) {
            builder.setErrCode(paramCheckResult.errCode);
            builder.setErrMsg(paramCheckResult.errMsg);
            return builder.build();
        }
        final String topicName = (String) paramCheckResult.checkData;
        paramCheckResult = PBParameterUtils.checkGroupName(request.getGroupName(), strBuffer);
        if (!paramCheckResult.result) {
            builder.setErrCode(paramCheckResult.errCode);
            builder.setErrMsg(paramCheckResult.errMsg);
            return builder.build();
        }
        final String groupName = (String) paramCheckResult.checkData;

        boolean isRegister = (request.getOpType() == RpcConstants.MSG_OPTYPE_REGISTER);
        Set<String> filterCondSet = new HashSet<String>();
        if (request.getFilterCondStrList() != null && !request.getFilterCondStrList().isEmpty()) {
            for (String filterCond : request.getFilterCondStrList()) {
                if (TStringUtils.isNotBlank(filterCond)) {
                    filterCondSet.add(filterCond.trim());
                }
            }
        }
        CertifiedResult authorizeResult =
                serverAuthHandler.validConsumeAuthorizeInfo(certResult.userName,
                        groupName, topicName, filterCondSet, isRegister, rmtAddress);
        if (!authorizeResult.result) {
            builder.setErrCode(authorizeResult.errCode);
            builder.setErrMsg(authorizeResult.errInfo);
            return builder.build();
        }
        Integer lid = null;
        Integer partLock = null;
        String partStr = getPartStr(groupName, topicName, request.getPartitionId());
        try {
            lid = brokerRowLock.getLock(null, StringUtils.getBytesUtf8(clientId), true);
            try {
                partLock = brokerRowLock.getLock(null, StringUtils.getBytesUtf8(partStr), true);
                if (request.getOpType() == RpcConstants.MSG_OPTYPE_REGISTER) {
                    return inProcessConsumerRegister(clientId, groupName,
                            topicName, partStr, filterCondSet, overtls, request, builder, strBuffer);
                } else if (request.getOpType() == RpcConstants.MSG_OPTYPE_UNREGISTER) {
                    return inProcessConsumerUnregister(clientId, groupName,
                            topicName, partStr, request, overtls, builder, strBuffer);
                } else {
                    String message = strBuffer.append("Invalid request:").append(request.getOpType()).toString();
                    logger.info(message);
                    builder.setErrCode(TErrCodeConstants.BAD_REQUEST);
                    builder.setErrMsg(message);
                    return builder.build();
                }
            } finally {
                if (partLock != null) {
                    brokerRowLock.releaseRowLock(partLock);
                }
            }
        } catch (IOException e) {
            strBuffer.delete(0, strBuffer.length());
            String message = "Failed to lock.";
            logger.warn(message, e);
            builder.setErrCode(TErrCodeConstants.BAD_REQUEST);
            builder.setErrMsg(strBuffer.append(message).append(e.getMessage()).toString());
            return builder.build();
        } finally {
            if (lid != null) {
                brokerRowLock.releaseRowLock(lid);
            }
        }
    }

    /***
     * Handle consumer's register request.
     *
     * @param clientId
     * @param groupName
     * @param topicName
     * @param partStr
     * @param filterCondSet
     * @param overtls
     * @param request
     * @param builder
     * @param strBuffer
     * @return
     */
    private RegisterResponseB2C inProcessConsumerRegister(final String clientId, final String groupName,
                                                          final String topicName, final String partStr,
                                                          final Set<String> filterCondSet, boolean overtls,
                                                          RegisterRequestC2B request,
                                                          RegisterResponseB2C.Builder builder,
                                                          StringBuilder strBuffer) {
        String consumerId = null;
        ConsumerNodeInfo consumerNodeInfo = consumerRegisterMap.get(partStr);
        if (consumerNodeInfo != null) {
            consumerId = consumerNodeInfo.getConsumerId();
        }
        if (TStringUtils.isEmpty(consumerId) || consumerId.equals(clientId)) {
            final long reqOffset = request.hasCurrOffset() ? request.getCurrOffset() : -1;
            long reqSessionTime = request.hasSessionTime() ? request.getSessionTime() : -1;
            String reqSessionKey = request.hasSessionKey() ? request.getSessionKey() : null;
            long reqSsdStoreId = request.hasSsdStoreId()
                    ? request.getSsdStoreId() : TBaseConstants.META_VALUE_UNDEFINED;
            int reqQryPriorityId = request.hasQryPriorityId()
                    ? request.getQryPriorityId() : TBaseConstants.META_VALUE_UNDEFINED;
            boolean needSsdProc =
                    ((reqSsdStoreId != TBaseConstants.META_VALUE_UNDEFINED)
                            && (reqSsdStoreId == this.metadataManage.getFlowCtrlRuleHandler().getSsdTranslateId()));
            consumerRegisterMap.put(partStr, new ConsumerNodeInfo(storeManager, reqQryPriorityId,
                    clientId, filterCondSet, reqSessionKey, reqSessionTime, reqSsdStoreId, needSsdProc,
                    request.hasSsdStoreId(), partStr));
            heartbeatManager.regConsumerNode(getHeartbeatNodeId(clientId, partStr), clientId, partStr);
            MessageStore dataStore = null;
            try {
                dataStore = this.storeManager.getOrCreateMessageStore(topicName, request.getPartitionId());
                if (dataStore == null) {
                    builder.setErrCode(TErrCodeConstants.FORBIDDEN);
                    builder.setErrMsg(strBuffer.append("Topic ").append(topicName).append("-")
                            .append(request.getPartitionId())
                            .append(" not existed, please check your configure").toString());
                    return builder.build();
                }
            } catch (Throwable e0) {
                strBuffer.delete(0, strBuffer.length());
                String message = "Register broker failure!";
                logger.warn(message, e0);
                builder.setErrCode(TErrCodeConstants.INTERNAL_SERVER_ERROR);
                builder.setErrMsg(strBuffer.append(message).append(", exception is ")
                        .append(e0.getMessage()).toString());
                return builder.build();
            }
            OffsetStorageInfo offsetInfo =
                    offsetManager.loadOffset(dataStore, groupName, topicName,
                            request.getPartitionId(), request.getReadStatus(), reqOffset, strBuffer);
            logger.info(strBuffer.append("[Consumer Register]").append(clientId)
                    .append(TokenConstants.SEGMENT_SEP).append(partStr)
                    .append(TokenConstants.SEGMENT_SEP).append(offsetInfo)
                    .append(", requestOffset=").append(reqOffset)
                    .append(", req has SSD storeId=").append(request.hasSsdStoreId())
                    .append(", req qryPriorityId=").append(reqQryPriorityId)
                    .append(", cur SSD storeId=").append(reqSsdStoreId)
                    .append(", isOverTLS=").append(overtls).toString());
            builder.setSuccess(true);
            builder.setErrCode(TErrCodeConstants.SUCCESS);
            builder.setErrMsg("OK!");
            builder.setCurrOffset(offsetInfo.getOffset());
            return builder.build();
        } else {
            TimeoutInfo timeoutInfo =
                    heartbeatManager.getConsumerRegMap().get(getHeartbeatNodeId(consumerId, partStr));
            if (timeoutInfo == null || System.currentTimeMillis() >= timeoutInfo.getTimeoutTime()) {
                consumerRegisterMap.remove(partStr);
                strBuffer.append("[Duplicated Register] Remove Invalid Consumer Register ")
                        .append(consumerId).append(TokenConstants.SEGMENT_SEP).append(partStr);
            } else {
                strBuffer.append("[Duplicated Register] Partition ").append(tubeConfig.getBrokerId())
                        .append(TokenConstants.SEGMENT_SEP).append(partStr)
                        .append(" has been registered by ").append(consumerId);
            }
            logger.warn(strBuffer.toString());
            builder.setErrCode(TErrCodeConstants.PARTITION_OCCUPIED);
            builder.setErrMsg(strBuffer.toString());
            return builder.build();
        }
    }

    /***
     * Handle consumer's unregister request.
     *
     * @param clientId
     * @param groupName
     * @param topicName
     * @param partStr
     * @param request
     * @param overtls
     * @param builder
     * @param strBuffer
     * @return
     */
    private RegisterResponseB2C inProcessConsumerUnregister(final String clientId, final String groupName,
                                                            final String topicName, final String partStr,
                                                            RegisterRequestC2B request, boolean overtls,
                                                            RegisterResponseB2C.Builder builder,
                                                            StringBuilder strBuffer) {
        logger.info(strBuffer.append("[Consumer Unregister]").append(clientId)
                .append(", isOverTLS=").append(overtls).toString());
        strBuffer.delete(0, strBuffer.length());
        ConsumerNodeInfo consumerNodeInfo = consumerRegisterMap.get(partStr);
        if (consumerNodeInfo == null) {
            logger.warn(strBuffer.append("[UnRegistered Consumer2]").append(clientId)
                    .append(TokenConstants.SEGMENT_SEP).append(partStr).toString());
            strBuffer.delete(0, strBuffer.length());
            builder.setErrCode(TErrCodeConstants.HB_NO_NODE);
            builder.setErrMsg(strBuffer.append("UnRegistered Consumer ")
                    .append(clientId).append(", you have to register firstly!").toString());
            return builder.build();
        }
        if (!clientId.equals(consumerNodeInfo.getConsumerId())) {
            String message = strBuffer.append("[Duplicated Request]").append("Partition ").append(partStr)
                    .append(" has been consumed by ").append(consumerNodeInfo.getConsumerId())
                    .append(";Current consumer ").append(clientId).toString();
            logger.warn(message);
            builder.setErrCode(TErrCodeConstants.DUPLICATE_PARTITION);
            builder.setErrMsg(strBuffer.append(", broker=").append(tubeConfig.getHostName()).toString());
            return builder.build();
        }
        try {
            int readStatus = request.getReadStatus();
            long updatedOffset =
                    offsetManager.commitOffset(groupName, topicName,
                            request.getPartitionId(), readStatus == 0);
            logger.info(strBuffer.append("[Unregister Offset] update lastOffset, ")
                    .append(groupName).append(" topic:").append(topicName).append(" partition:")
                    .append(request.getPartitionId()).append(" updatedOffset:").append(updatedOffset).toString());
            strBuffer.delete(0, strBuffer.length());
            consumerRegisterMap.remove(partStr);
            heartbeatManager.unRegConsumerNode(
                    getHeartbeatNodeId(clientId, partStr));
        } catch (Exception e) {
            strBuffer.delete(0, strBuffer.length());
            String message = strBuffer.append("Unregister consumer:")
                    .append(clientId).append(" failed.").toString();
            logger.warn(message, e);
            builder.setErrCode(TErrCodeConstants.INTERNAL_SERVER_ERROR);
            builder.setErrMsg(strBuffer.append(" exception is ").append(e.getMessage()).toString());
            return builder.build();
        }
        builder.setSuccess(true);
        builder.setErrCode(TErrCodeConstants.SUCCESS);
        builder.setErrMsg("OK!");
        return builder.build();
    }

    /***
     * Handle consumer's heartbeat request.
     *
     * @param request
     * @param rmtAddress
     * @param overtls
     * @return
     * @throws Throwable
     */
    @Override
    public HeartBeatResponseB2C consumerHeartbeatC2B(HeartBeatRequestC2B request,
                                                     final String rmtAddress,
                                                     boolean overtls) throws Throwable {
        final HeartBeatResponseB2C.Builder builder = HeartBeatResponseB2C.newBuilder();
        final StringBuilder strBuffer = new StringBuilder(512);
        builder.setSuccess(false);
        CertifiedResult certResult =
                serverAuthHandler.identityValidUserInfo(request.getAuthInfo(), false);
        if (!certResult.result) {
            builder.setErrCode(certResult.errCode);
            builder.setErrMsg(certResult.errInfo);
            return builder.build();
        }
        ParamCheckResult paramCheckResult =
                PBParameterUtils.checkClientId(request.getClientId(), strBuffer);
        if (!paramCheckResult.result) {
            builder.setErrCode(paramCheckResult.errCode);
            builder.setErrMsg(paramCheckResult.errMsg);
            return builder.build();
        }
        final String clientId = (String) paramCheckResult.checkData;
        paramCheckResult =
                PBParameterUtils.checkGroupName(request.getGroupName(), strBuffer);
        if (!paramCheckResult.result) {
            builder.setErrCode(paramCheckResult.errCode);
            builder.setErrMsg(paramCheckResult.errMsg);
            return builder.build();
        }
        final String groupName = (String) paramCheckResult.checkData;
        long reqSsdStoreId = request.hasSsdStoreId()
                ? request.getSsdStoreId() : TBaseConstants.META_VALUE_UNDEFINED;
        int reqQryPriorityId = request.hasQryPriorityId()
                ? request.getQryPriorityId() : TBaseConstants.META_VALUE_UNDEFINED;
        List<Partition> partitions =
                DataConverterUtil.convertPartitionInfo(request.getPartitionInfoList());
        CertifiedResult authorizeResult = null;
        boolean isAuthorized = false;
        List<String> failureInfo = new ArrayList<String>();
        for (Partition partition : partitions) {
            String topic = partition.getTopic();
            int partitionId = partition.getPartitionId();
            String partStr = getPartStr(groupName, topic, partitionId);
            ConsumerNodeInfo consumerNodeInfo = consumerRegisterMap.get(partStr);
            if (consumerNodeInfo == null) {
                failureInfo.add(strBuffer.append(TErrCodeConstants.HB_NO_NODE)
                        .append(TokenConstants.ATTR_SEP)
                        .append(partition.toString()).toString());
                strBuffer.delete(0, strBuffer.length());
                logger.warn(strBuffer.append("[Heartbeat Check] UnRegistered Consumer:")
                        .append(clientId).append(TokenConstants.SEGMENT_SEP)
                        .append(partStr).toString());
                strBuffer.delete(0, strBuffer.length());
                continue;
            }
            if (!clientId.equals(consumerNodeInfo.getConsumerId())) {
                failureInfo.add(strBuffer.append(TErrCodeConstants.DUPLICATE_PARTITION)
                        .append(TokenConstants.ATTR_SEP).append(partition.toString()).toString());
                strBuffer.delete(0, strBuffer.length());
                strBuffer.append("[Heartbeat Check] Duplicated partition: Partition ").append(partStr)
                        .append(" has been consumed by ").append(consumerNodeInfo.getConsumerId())
                        .append(";Current consumer ").append(clientId);
                logger.warn(strBuffer.toString());
                strBuffer.delete(0, strBuffer.length());
                continue;
            }
            if (!isAuthorized) {
                authorizeResult =
                        serverAuthHandler.validConsumeAuthorizeInfo(certResult.userName,
                                groupName, topic, consumerNodeInfo.getFilterCondStrs(), true, rmtAddress);
                if (!authorizeResult.result) {
                    builder.setRequireAuth(authorizeResult.reAuth);
                    builder.setErrCode(authorizeResult.errCode);
                    builder.setErrMsg(authorizeResult.errInfo);
                    return builder.build();
                }
                isAuthorized = true;
            }
            try {
                heartbeatManager.updConsumerNode(
                        getHeartbeatNodeId(clientId, partStr));
            } catch (HeartbeatException e) {
                failureInfo.add(strBuffer.append(TErrCodeConstants.HB_NO_NODE)
                        .append(TokenConstants.ATTR_SEP)
                        .append(partition.toString()).toString());
                strBuffer.delete(0, strBuffer.length());
                logger.warn(strBuffer.append("[Heartbeat Check] Invalid Request")
                        .append(clientId).append(TokenConstants.SEGMENT_SEP)
                        .append(topic).append(TokenConstants.ATTR_SEP).append(partitionId).toString());
                strBuffer.delete(0, strBuffer.length());
                continue;
            }
            if (consumerNodeInfo.getSsdTransId() != reqSsdStoreId) {
                boolean needSsdProc = ((reqSsdStoreId != TBaseConstants.META_VALUE_UNDEFINED)
                        && (reqSsdStoreId == metadataManage.getFlowCtrlRuleHandler().getSsdTranslateId()));
                consumerNodeInfo.setSsdTransId(reqSsdStoreId, needSsdProc);
            }
            if (consumerNodeInfo.getQryPriorityId() != reqQryPriorityId) {
                consumerNodeInfo.setQryPriorityId(reqQryPriorityId);
            }
        }
        builder.setRequireAuth(certResult.reAuth);
        builder.setSuccess(true);
        builder.setErrCode(TErrCodeConstants.SUCCESS);
        builder.setHasPartFailure(false);
        if (!failureInfo.isEmpty()) {
            builder.setHasPartFailure(true);
            builder.addAllFailureInfo(failureInfo);
        }
        builder.setErrMsg("OK!");
        return builder.build();
    }

    /***
     * Handle consumer's commit offset request.
     *
     * @param request
     * @param rmtAddress
     * @param overtls
     * @return
     * @throws Throwable
     */
    @Override
    public CommitOffsetResponseB2C consumerCommitC2B(CommitOffsetRequestC2B request,
                                                     final String rmtAddress,
                                                     boolean overtls) throws Throwable {
        final CommitOffsetResponseB2C.Builder builder = CommitOffsetResponseB2C.newBuilder();
        StringBuilder strBuffer = new StringBuilder(512);
        builder.setSuccess(false);
        builder.setCurrOffset(-1);
        ParamCheckResult paramCheckResult =
                PBParameterUtils.checkClientId(request.getClientId(), strBuffer);
        if (!paramCheckResult.result) {
            builder.setErrCode(paramCheckResult.errCode);
            builder.setErrMsg(paramCheckResult.errMsg);
            return builder.build();
        }
        final String clientId = (String) paramCheckResult.checkData;
        paramCheckResult =
                PBParameterUtils.checkGroupName(request.getGroupName(), strBuffer);
        if (!paramCheckResult.result) {
            builder.setErrCode(paramCheckResult.errCode);
            builder.setErrMsg(paramCheckResult.errMsg);
            return builder.build();
        }
        final String groupName = (String) paramCheckResult.checkData;
        int partitionId = request.getPartitionId();
        paramCheckResult =
                PBParameterUtils.checkExistTopicNameInfo(
                        request.getTopicName(), partitionId, this.metadataManage, strBuffer);
        if (!paramCheckResult.result) {
            builder.setErrCode(paramCheckResult.errCode);
            builder.setErrMsg(paramCheckResult.errMsg);
            return builder.build();
        }
        final String topicName = (String) paramCheckResult.checkData;
        String partStr = getPartStr(groupName, topicName, partitionId);
        ConsumerNodeInfo consumerNodeInfo = consumerRegisterMap.get(partStr);
        if (consumerNodeInfo == null) {
            builder.setErrCode(TErrCodeConstants.UNAUTHORIZED);
            builder.setErrMsg("The partition not registered by consumers");
            logger.error(strBuffer
                    .append("[consumerCommitC2B error] partition not registered by consumers: commit cosnumer is: ")
                    .append(clientId).append(", partition is : ").append(partStr).toString());
            return builder.build();
        }
        boolean isConsumed = true;
        if (request.hasLastPackConsumed()) {
            isConsumed = request.getLastPackConsumed();
        }
        if (clientId.equals(consumerNodeInfo.getConsumerId())) {
            try {
                long currOffset =
                        offsetManager.commitOffset(groupName, topicName, partitionId, isConsumed);
                builder.setSuccess(true);
                builder.setErrCode(TErrCodeConstants.SUCCESS);
                builder.setErrMsg("OK!");
                builder.setCurrOffset(currOffset);
            } catch (Exception e) {
                builder.setErrMsg(e.getMessage());
                builder.setErrCode(TErrCodeConstants.INTERNAL_SERVER_ERROR);
                logger.error("[commitOffset error]", e);
            }
        } else {
            builder.setErrCode(TErrCodeConstants.UNAUTHORIZED);
            builder.setErrMsg(strBuffer
                    .append("The partition has been registered by other consumer: ")
                    .append(consumerNodeInfo.getConsumerId()).toString());
            strBuffer.delete(0, strBuffer.length());
            logger.error(strBuffer
                    .append("[consumerCommitC2B error] " +
                            "partition has been registered by other consumer: commit cosnumer is: ")
                    .append(clientId).append(", registered consumer is: ").append(consumerNodeInfo.getConsumerId())
                    .append(", partition is : ").append(partStr).toString());
        }
        return builder.build();
    }

    private String getPartStr(String group, String topic, int partitionId) {
        return new StringBuilder(512).append(group).append(TokenConstants.ATTR_SEP)
                .append(topic).append(TokenConstants.ATTR_SEP).append(partitionId).toString();
    }

    private String getHeartbeatNodeId(String consumerId, String partStr) {
        return new StringBuilder(512).append(consumerId)
                .append(TokenConstants.SEGMENT_SEP).append(partStr).toString();

    }

    /***
     * Consumer timeout handler. Update consumer's info if exists consumer timeout.
     */
    public class ConsumerTimeoutListener implements TimeoutListener {

        @Override
        public void onTimeout(final String nodeId, TimeoutInfo nodeInfo) {
            Integer lid = null;
            StringBuilder strBuffer = new StringBuilder(512);
            try {
                lid =
                        brokerRowLock.getLock(null, StringUtils.getBytesUtf8(nodeInfo.getSecondKey()), true);
                Integer partLock = null;
                try {
                    partLock =
                            brokerRowLock.getLock(null, StringUtils.getBytesUtf8(nodeInfo.getThirdKey()), true);
                    ConsumerNodeInfo consumerNodeInfo =
                            consumerRegisterMap.get(nodeInfo.getThirdKey());
                    if (consumerNodeInfo == null) {
                        return;
                    }
                    if (consumerNodeInfo.getConsumerId().equalsIgnoreCase(nodeInfo.getSecondKey())) {
                        consumerRegisterMap.remove(nodeInfo.getThirdKey());
                        String[] groupTopicPart =
                                consumerNodeInfo.getPartStr().split(TokenConstants.ATTR_SEP);
                        long updatedOffset =
                                offsetManager.commitOffset(groupTopicPart[0],
                                        groupTopicPart[1], Integer.valueOf(groupTopicPart[2]), false);
                        logger.info(strBuffer.append("[Consumer-Partition Timeout]")
                                .append(nodeId).append(",updatedOffset=")
                                .append(updatedOffset).toString());
                    }
                } catch (IOException e1) {
                    logger.warn("Failed to lock.", e1);
                } finally {
                    if (partLock != null) {
                        brokerRowLock.releaseRowLock(partLock);
                    }
                }
            } catch (IOException e2) {
                logger.warn("Failed to lock.", e2);
            } finally {
                if (lid != null) {
                    brokerRowLock.releaseRowLock(lid);
                }
            }
        }
    }

}

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

import com.google.protobuf.ByteString;
import com.tencent.tubemq.client.config.TubeClientConfig;
import com.tencent.tubemq.client.exception.TubeClientException;
import com.tencent.tubemq.client.factory.InnerSessionFactory;
import com.tencent.tubemq.client.producer.qltystats.DefaultBrokerRcvQltyStats;
import com.tencent.tubemq.corebase.Message;
import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.TErrCodeConstants;
import com.tencent.tubemq.corebase.cluster.BrokerInfo;
import com.tencent.tubemq.corebase.cluster.Partition;
import com.tencent.tubemq.corebase.protobuf.generated.ClientBroker;
import com.tencent.tubemq.corebase.utils.AddressUtils;
import com.tencent.tubemq.corebase.utils.MessageFlagUtils;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.corerpc.RpcConfig;
import com.tencent.tubemq.corerpc.RpcConstants;
import com.tencent.tubemq.corerpc.RpcServiceFactory;
import com.tencent.tubemq.corerpc.client.Callback;
import com.tencent.tubemq.corerpc.exception.LocalConnException;
import com.tencent.tubemq.corerpc.service.BrokerWriteService;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.codec.binary.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of MessageProducer
 */
public class SimpleMessageProducer implements MessageProducer {
    private static final Logger logger =
            LoggerFactory.getLogger(SimpleMessageProducer.class);
    private final TubeClientConfig producerConfig;
    private final ConcurrentHashMap<String, Long> publishTopicMap =
            new ConcurrentHashMap<String, Long>();
    private final InnerSessionFactory sessionFactory;
    private final RpcServiceFactory rpcServiceFactory;
    private final ProducerManager producerManager;
    private final PartitionRouter partitionRouter;
    private final DefaultBrokerRcvQltyStats brokerRcvQltyStats;
    private final RpcConfig rpcConfig = new RpcConfig();
    private AtomicBoolean isShutDown = new AtomicBoolean(false);

    public SimpleMessageProducer(final InnerSessionFactory sessionFactory,
                                 TubeClientConfig tubeClientConfig) throws TubeClientException {
        java.security.Security.setProperty("networkaddress.cache.ttl", "3");
        java.security.Security.setProperty("networkaddress.cache.negative.ttl", "1");
        if (sessionFactory == null || tubeClientConfig == null) {
            throw new TubeClientException(
                    "Illegal parameter: messageSessionFactory or tubeClientConfig is null!");
        }
        this.producerConfig = tubeClientConfig;
        this.sessionFactory = sessionFactory;
        this.rpcServiceFactory = this.sessionFactory.getRpcServiceFactory();
        this.producerManager = this.sessionFactory.getProducerManager();
        this.brokerRcvQltyStats = sessionFactory.getBrokerRcvQltyStats();
        this.partitionRouter = new RoundRobinPartitionRouter();
        this.rpcConfig.put(RpcConstants.CONNECT_TIMEOUT, 3000);
        this.rpcConfig.put(RpcConstants.REQUEST_TIMEOUT,
                tubeClientConfig.getRpcTimeoutMs());
        this.rpcConfig.put(RpcConstants.NETTY_WRITE_HIGH_MARK,
                tubeClientConfig.getNettyWriteBufferHighWaterMark());
        this.rpcConfig.put(RpcConstants.NETTY_WRITE_LOW_MARK,
                tubeClientConfig.getNettyWriteBufferLowWaterMark());
        this.rpcConfig.put(RpcConstants.WORKER_COUNT,
                tubeClientConfig.getRpcConnProcesserCnt());
        this.rpcConfig.put(RpcConstants.WORKER_THREAD_NAME,
                "tube_producer_netty_worker-");
        this.rpcConfig.put(RpcConstants.WORKER_MEM_SIZE,
                tubeClientConfig.getRpcNettyWorkMemorySize());
        this.rpcConfig.put(RpcConstants.CALLBACK_WORKER_COUNT,
                tubeClientConfig.getRpcRspCallBackThreadCnt());
    }

    /**
     * Publish a topic.
     *
     * @param topic topic name
     * @throws TubeClientException
     */
    @Override
    public void publish(String topic) throws TubeClientException {
        if (isShutDown.get()) {
            throw new TubeClientException("Status error: producer has been shutdown!");
        }
        if (TStringUtils.isBlank(topic)) {
            throw new TubeClientException("Illegal parameter: blank topic!");
        }
        this.producerManager.publish(topic.trim());
        publishTopicMap.putIfAbsent(topic.trim(), System.currentTimeMillis());
    }

    /**
     * Publish a set of topics.
     *
     * @param topicSet topic names
     * @return successful published topic names
     * @throws TubeClientException
     */
    @Override
    public Set<String> publish(Set<String> topicSet) throws TubeClientException {
        if (isShutDown.get()) {
            throw new TubeClientException("Status error: producer has been shutdown!");
        }
        if ((topicSet == null) || topicSet.isEmpty()) {
            throw new TubeClientException("Illegal parameter: topicSet is null or empty!");
        }
        Set<String> newTopicSet = new HashSet<String>();
        for (String topicItem : topicSet) {
            if (TStringUtils.isBlank(topicItem)) {
                throw new TubeClientException(new StringBuilder(256)
                        .append("Illegal parameter: found blank topic value in topicSet : ")
                        .append(topicSet).toString());
            }
            newTopicSet.add(topicItem.trim());
        }
        long curTime = System.currentTimeMillis();
        for (String topicNam : newTopicSet) {
            publishTopicMap.putIfAbsent(topicNam, curTime);
        }
        return this.producerManager.publish(newTopicSet);
    }

    /**
     * Get published topic names. You cannot get topic set from a shutdown producer.
     *
     * @return published topic set
     * @throws TubeClientException
     */
    @Override
    public Set<String> getPublishedTopicSet() throws TubeClientException {
        if (isShutDown.get()) {
            throw new TubeClientException("Status error: producer has been shutdown!");
        }
        return this.publishTopicMap.keySet();
    }

    /**
     * Check if the given topic accept publish message.
     *
     * @param topic topic name
     * @return if accept message
     * @throws TubeClientException
     */
    @Override
    public boolean isTopicCurAcceptPublish(String topic) throws TubeClientException {
        if (isShutDown.get()) {
            throw new TubeClientException("Status error: producer has been shutdown!");
        }
        if (TStringUtils.isBlank(topic)) {
            throw new TubeClientException("Illegal parameter: blank topic!");
        }
        if (this.publishTopicMap.get(topic) == null) {
            return false;
        }
        if (this.producerManager.getTopicPartition(topic) == null) {
            return false;
        }
        return true;
    }

    @Override
    public synchronized void shutdown() throws Throwable {
        logger.info("[ShutDown] begin shutdown producer...");
        if (this.isShutDown.get()) {
            return;
        }
        if (this.isShutDown.compareAndSet(false, true)) {
            this.producerManager.removeTopic(publishTopicMap.keySet());
            this.publishTopicMap.clear();
            this.sessionFactory.removeClient(this);
            logger.info("[ShutDown] producer has stopped!");
        }
    }

    @Override
    public MessageSentResult sendMessage(final Message message)
            throws TubeClientException, InterruptedException {
        checkMessageAndStatus(message);
        Partition partition = this.selectPartition(message, BrokerWriteService.class);
        int brokerId = partition.getBrokerId();
        try {
            this.brokerRcvQltyStats.addSendStatistic(brokerId);
            ClientBroker.SendMessageResponseB2P response =
                    getBrokerService(partition.getBroker()).sendMessageP2B(
                            createSendMessageRequest(partition, message),
                            AddressUtils.getLocalAddress(), producerConfig.isTlsEnable());
            rpcServiceFactory.resetRmtAddrErrCount(partition.getBroker().getBrokerAddr());
            this.brokerRcvQltyStats.addReceiveStatistic(brokerId, response.getSuccess());
            return this.buildMsgSentResult(message, partition, response);
        } catch (final Throwable e) {
            if (e instanceof LocalConnException) {
                rpcServiceFactory.addRmtAddrErrCount(partition.getBroker().getBrokerAddr());
            }
            partition.increRetries(1);
            this.brokerRcvQltyStats.addReceiveStatistic(brokerId, false);
            throw new TubeClientException("Send message failed", e);
        }
    }

    @Override
    public void sendMessage(final Message message, final MessageSentCallback cb) throws TubeClientException,
            InterruptedException {
        checkMessageAndStatus(message);
        final Partition partition =
                this.selectPartition(message, BrokerWriteService.AsyncService.class);
        final int brokerId = partition.getBrokerId();
        try {
            this.brokerRcvQltyStats.addSendStatistic(brokerId);
            getAsyncBrokerService(partition.getBroker()).sendMessageP2B(
                    createSendMessageRequest(partition, message),
                    AddressUtils.getLocalAddress(), producerConfig.isTlsEnable(),
                    new Callback() {
                        @Override
                        public void handleResult(Object result) {
                            if (!(result instanceof ClientBroker.SendMessageResponseB2P)) {
                                return;
                            }
                            final ClientBroker.SendMessageResponseB2P responseB2P =
                                    (ClientBroker.SendMessageResponseB2P) result;
                            final MessageSentResult rt =
                                    SimpleMessageProducer.this.buildMsgSentResult(message, partition, responseB2P);
                            partition.resetRetries();
                            brokerRcvQltyStats.addReceiveStatistic(brokerId,
                                    responseB2P.getSuccess());
                            cb.onMessageSent(rt);
                        }

                        @Override
                        public void handleError(Throwable error) {
                            partition.increRetries(1);
                            brokerRcvQltyStats.addReceiveStatistic(brokerId, false);
                            cb.onException(error);
                        }
                    });
            rpcServiceFactory.resetRmtAddrErrCount(partition.getBroker().getBrokerAddr());
        } catch (final Throwable e) {
            if (e instanceof LocalConnException) {
                rpcServiceFactory.addRmtAddrErrCount(partition.getBroker().getBrokerAddr());
            }
            // if failed,increment the counter
            partition.increRetries(1);
            this.brokerRcvQltyStats.addReceiveStatistic(brokerId, false);
            cb.onException(e);
        }
    }

    private void checkMessageAndStatus(final Message message) throws TubeClientException {
        if (message == null) {
            throw new TubeClientException("Illegal parameter: null message package!");
        }
        if (TStringUtils.isBlank(message.getTopic())) {
            throw new TubeClientException("Illegal parameter: blank topic in message package!");
        }
        if ((message.getData() == null)
                || (message.getData().length == 0)) {
            throw new TubeClientException("Illegal parameter: null data in message package!");
        }
        int msgSize = TStringUtils.isBlank(message.getAttribute())
                ? message.getData().length : (message.getData().length + message.getAttribute().length());
        if (msgSize > TBaseConstants.META_MAX_MESSAGEG_DATA_SIZE) {
            throw new TubeClientException(new StringBuilder(512)
                    .append("Illegal parameter: over max message length for the total size of")
                    .append(" message data and attribute, allowed size is ")
                    .append(TBaseConstants.META_MAX_MESSAGEG_DATA_SIZE)
                    .append(", message's real size is ").append(msgSize).toString());
        }
        if (isShutDown.get()) {
            throw new TubeClientException("Status error: producer has been shutdown!");
        }

        if (this.publishTopicMap.get(message.getTopic()) == null) {
            throw new TubeClientException(new StringBuilder(512)
                    .append("Topic ").append(message.getTopic())
                    .append(" not publish, please publish first!").toString());
        }
        if (this.producerManager.getTopicPartition(message.getTopic()) == null) {
            throw new TubeClientException(new StringBuilder(512)
                    .append("Topic ").append(message.getTopic())
                    .append(" not publish, make sure the topic exist or acceptPublish and try later!").toString());
        }
    }

    private ClientBroker.SendMessageRequestP2B createSendMessageRequest(Partition partition,
                                                                        Message message) {
        ClientBroker.SendMessageRequestP2B.Builder builder =
                ClientBroker.SendMessageRequestP2B.newBuilder();
        builder.setClientId(this.producerManager.getProducerId());
        builder.setTopicName(partition.getTopic());
        builder.setPartitionId(partition.getPartitionId());
        builder.setData(ByteString.copyFrom(encodePayload(message)));
        builder.setFlag(MessageFlagUtils.getFlag(message));
        builder.setSentAddr(this.producerManager.getProducerAddrId());
        builder.setCheckSum(-1);
        if (TStringUtils.isNotBlank(message.getMsgType())) {
            builder.setMsgType(message.getMsgType());
        }
        if (TStringUtils.isNotBlank(message.getMsgTime())) {
            builder.setMsgTime(message.getMsgTime());
        }
        builder = this.producerManager.setAuthorizedTokenInfo(builder);
        return builder.build();
    }

    private byte[] encodePayload(final Message message) {
        final byte[] payload = message.getData();
        final String attribute = message.getAttribute();
        if (TStringUtils.isBlank(attribute)) {
            return payload;
        }
        byte[] attrData = StringUtils.getBytesUtf8(attribute);
        final ByteBuffer buffer =
                ByteBuffer.allocate(4 + attrData.length + payload.length);
        buffer.putInt(attrData.length);
        buffer.put(attrData);
        buffer.put(payload);
        return buffer.array();
    }

    private MessageSentResult buildMsgSentResult(final Message message,
                                                 final Partition partition,
                                                 final ClientBroker.SendMessageResponseB2P response) {
        final String resultStr = response.getErrMsg();
        if (response.getErrCode() == TErrCodeConstants.SUCCESS) {
            return new MessageSentResult(true, response.getErrCode(), "Ok!",
                    message, Long.parseLong(resultStr), partition);
        } else {
            return new MessageSentResult(false, response.getErrCode(), resultStr,
                    message, TBaseConstants.META_VALUE_UNDEFINED, partition);
        }
    }

    private Partition selectPartition(final Message message,
                                      Class clazz) throws TubeClientException {
        String topic = message.getTopic();
        StringBuilder sBuilder = new StringBuilder(512);
        Map<Integer, List<Partition>> brokerPartList =
                this.producerManager.getTopicPartition(topic);
        if (brokerPartList == null || brokerPartList.isEmpty()) {
            throw new TubeClientException(sBuilder.append("Null partition for topic: ")
                    .append(message.getTopic()).append(", please try later!").toString());
        }
        List<Partition> partList =
                this.brokerRcvQltyStats.getAllowedBrokerPartitions(brokerPartList);
        if (partList == null || partList.isEmpty()) {
            throw new TubeClientException(sBuilder.append("No available partition for topic: ")
                    .append(message.getTopic()).toString());
        }
        Partition partition =
                this.partitionRouter.getPartition(message, partList);
        if (partition == null) {
            throw new TubeClientException(new StringBuilder(512)
                    .append("Not found available partition for topic: ")
                    .append(message.getTopic()).toString());
        }
        if (rpcServiceFactory.isServiceEmpty()) {
            return partition;
        }
        BrokerInfo brokerInfo = partition.getBroker();
        int count = 0;
        while (count++ < partList.size()) {
            if (rpcServiceFactory.getOrCreateService(clazz, brokerInfo, rpcConfig) != null) {
                break;
            }
            partition = this.partitionRouter.getPartition(message, partList);
            brokerInfo = partition.getBroker();
        }
        return partition;
    }

    private BrokerWriteService getBrokerService(BrokerInfo brokerInfo) {
        return rpcServiceFactory.getService(BrokerWriteService.class, brokerInfo, rpcConfig);
    }

    private BrokerWriteService.AsyncService getAsyncBrokerService(BrokerInfo brokerInfo) {
        return rpcServiceFactory.getService(
                BrokerWriteService.AsyncService.class, brokerInfo, rpcConfig);
    }
}

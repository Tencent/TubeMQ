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

import com.tencent.tubemq.client.config.ConsumerConfig;
import com.tencent.tubemq.client.exception.TubeClientException;
import com.tencent.tubemq.client.factory.InnerSessionFactory;
import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.TokenConstants;
import com.tencent.tubemq.corebase.cluster.Partition;
import com.tencent.tubemq.corebase.protobuf.generated.ClientBroker;
import com.tencent.tubemq.corebase.utils.AddressUtils;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import java.util.Map;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of PullMessageConsumer
 */
public class SimplePullMessageConsumer implements PullMessageConsumer {
    private static final Logger logger = LoggerFactory.getLogger(SimplePullMessageConsumer.class);
    private final BaseMessageConsumer baseConsumer;

    public SimplePullMessageConsumer(final InnerSessionFactory messageSessionFactory,
                                     final ConsumerConfig consumerConfig) throws TubeClientException {
        baseConsumer =
                new BaseMessageConsumer(messageSessionFactory, consumerConfig, true);
    }

    @Override
    public boolean isPartitionsReady(long maxWaitTime) {
        return baseConsumer.rmtDataCache.isPartitionsReady(maxWaitTime);
    }

    @Override
    public void shutdown() throws Throwable {
        baseConsumer.shutdown();
    }

    @Override
    public String getClientVersion() {
        return baseConsumer.getClientVersion();
    }

    @Override
    public String getConsumerId() {
        return baseConsumer.getConsumerId();
    }

    @Override
    public boolean isShutdown() {
        return baseConsumer.isShutdown();
    }

    @Override
    public ConsumerConfig getConsumerConfig() {
        return baseConsumer.getConsumerConfig();
    }

    @Override
    public boolean isFilterConsume(String topic) {
        return baseConsumer.isFilterConsume(topic);
    }

    @Override
    public Map<String, ConsumeOffsetInfo> getCurConsumedPartitions() throws TubeClientException {
        return baseConsumer.getCurConsumedPartitions();
    }

    @Override
    public PullMessageConsumer subscribe(String topic,
                                         TreeSet<String> filterConds) throws TubeClientException {
        baseConsumer.subscribe(topic, filterConds, null);
        return this;
    }

    @Override
    public void completeSubscribe() throws TubeClientException {
        baseConsumer.completeSubscribe();
    }

    @Override
    public void completeSubscribe(final String sessionKey,
                                  final int sourceCount,
                                  final boolean isSelectBig,
                                  final Map<String, Long> partOffsetMap) throws TubeClientException {
        baseConsumer.completeSubscribe(sessionKey, sourceCount, isSelectBig, partOffsetMap);
    }

    @Override
    public ConsumerResult getMessage() throws TubeClientException {
        baseConsumer.checkClientRunning();
        if (!baseConsumer.isSubscribed()) {
            throw new TubeClientException("Please complete topic's Subscribe call first!");
        }
        StringBuilder sBuilder = new StringBuilder(512);
        // Check the data cache first
        PartitionSelectResult selectResult = baseConsumer.rmtDataCache.pullSelect();
        if (!selectResult.isSuccess()) {
            return new ConsumerResult(selectResult.getErrCode(), selectResult.getErrMsg());
        }
        FetchContext taskContext = baseConsumer.fetchMessage(selectResult, sBuilder);
        return new ConsumerResult(taskContext);
    }

    @Override
    public ConsumerResult confirmConsume(final String confirmContext,
                                         boolean isConsumed) throws TubeClientException {
        baseConsumer.checkClientRunning();
        if (!baseConsumer.isSubscribed()) {
            throw new TubeClientException("Please complete topic's Subscribe call first!");
        }
        StringBuilder sBuilder = new StringBuilder(512);
        long currOffset = TBaseConstants.META_VALUE_UNDEFINED;
        // Verify if the confirmContext is valid
        if (TStringUtils.isBlank(confirmContext)) {
            throw new TubeClientException("ConfirmContext is null !");
        }
        String[] strConfirmContextItems =
                confirmContext.split(TokenConstants.ATTR_SEP);
        if (strConfirmContextItems.length != 4) {
            throw new TubeClientException(
                    "ConfirmContext format error: value must be aaaa:bbbb:cccc:ddddd !");
        }
        for (String itemStr : strConfirmContextItems) {
            if (TStringUtils.isBlank(itemStr)) {
                throw new TubeClientException(sBuilder
                        .append("ConfirmContext's format error: item (")
                        .append(itemStr).append(") is null !").toString());
            }
        }
        String keyId = sBuilder.append(strConfirmContextItems[0].trim())
                .append(TokenConstants.ATTR_SEP).append(strConfirmContextItems[1].trim())
                .append(TokenConstants.ATTR_SEP).append(strConfirmContextItems[2].trim()).toString();
        sBuilder.delete(0, sBuilder.length());
        String topicName = strConfirmContextItems[1].trim();
        ConsumerResult result = new ConsumerResult(true, "Ok", topicName, keyId);
        long timeStamp = Long.valueOf(strConfirmContextItems[3]);
        if (!baseConsumer.rmtDataCache.isPartitionInUse(keyId, timeStamp)) {
            throw new TubeClientException("The confirmContext's value invalid!");
        }
        if (this.baseConsumer.consumerConfig.isPullConfirmInLocal()) {
            baseConsumer.rmtDataCache.succRspRelease(keyId, topicName,
                    timeStamp, isConsumed, isFilterConsume(topicName), currOffset);
        } else {
            try {
                Partition curPartiton =
                        baseConsumer.rmtDataCache.getPartitonByKey(keyId);
                if (curPartiton != null) {
                    ClientBroker.CommitOffsetResponseB2C commitResponse =
                            baseConsumer.getBrokerService(curPartiton.getBroker())
                                    .consumerCommitC2B(baseConsumer.createBrokerCommitRequest(curPartiton, isConsumed),
                                            AddressUtils.getLocalAddress(), getConsumerConfig().isTlsEnable());
                    if (commitResponse == null) {
                        result.setConfirmProcessResult(false, sBuilder.append("Confirm ")
                                .append(confirmContext).append("'s offset failed!").toString(), currOffset);
                        sBuilder.delete(0, sBuilder.length());
                    } else {
                        if (commitResponse.hasCurrOffset()) {
                            if (commitResponse.getCurrOffset() >= 0) {
                                currOffset = commitResponse.getCurrOffset();
                            }
                        }
                        result.setConfirmProcessResult(commitResponse.getSuccess(),
                                commitResponse.getErrMsg(), currOffset);
                    }
                } else {
                    result.setConfirmProcessResult(false, sBuilder
                            .append("Not found the partition by confirmContext:")
                            .append(confirmContext).toString(), currOffset);
                    sBuilder.delete(0, sBuilder.length());
                }
            } catch (Throwable e) {
                sBuilder.delete(0, sBuilder.length());
                throw new TubeClientException(sBuilder.append("Confirm ")
                        .append(confirmContext).append("'s offset failed.").toString(), e);
            } finally {
                baseConsumer.rmtDataCache.succRspRelease(keyId, topicName,
                        timeStamp, isConsumed, isFilterConsume(topicName), currOffset);
            }
        }
        return result;
    }
}

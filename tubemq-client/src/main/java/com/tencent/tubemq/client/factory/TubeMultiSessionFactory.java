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

package com.tencent.tubemq.client.factory;

import com.tencent.tubemq.client.config.ConsumerConfig;
import com.tencent.tubemq.client.config.TubeClientConfig;
import com.tencent.tubemq.client.config.TubeClientConfigUtils;
import com.tencent.tubemq.client.consumer.PullMessageConsumer;
import com.tencent.tubemq.client.consumer.PushMessageConsumer;
import com.tencent.tubemq.client.exception.TubeClientException;
import com.tencent.tubemq.client.producer.MessageProducer;
import com.tencent.tubemq.corebase.Shutdownable;
import com.tencent.tubemq.corerpc.RpcConfig;
import com.tencent.tubemq.corerpc.netty.NettyClientFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TubeMultiSessionFactory implements MessageSessionFactory {

    private static final Logger logger =
            LoggerFactory.getLogger(TubeMultiSessionFactory.class);
    private final NettyClientFactory clientFactory = new NettyClientFactory();
    private final TubeBaseSessionFactory baseSessionFactory;
    private final AtomicBoolean isShutDown = new AtomicBoolean(false);

    public TubeMultiSessionFactory(final TubeClientConfig tubeClientConfig) throws TubeClientException {
        RpcConfig config = TubeClientConfigUtils.getRpcConfigByClientConfig(tubeClientConfig, false);
        clientFactory.configure(config);
        baseSessionFactory = new TubeBaseSessionFactory(clientFactory, tubeClientConfig);
    }

    @Override
    public void shutdown() throws TubeClientException {
        if (isShutDown.compareAndSet(false, true)) {
            baseSessionFactory.shutdown();
            clientFactory.shutdown();
        }
    }

    @Override
    public <T extends Shutdownable> void removeClient(final T client) {
        this.baseSessionFactory.removeClient(client);
    }

    @Override
    public MessageProducer createProducer() throws TubeClientException {
        return this.baseSessionFactory.createProducer();
    }

    @Override
    public PushMessageConsumer createPushConsumer(final ConsumerConfig consumerConfig)
            throws TubeClientException {
        return this.baseSessionFactory.createPushConsumer(consumerConfig);
    }

    @Override
    public PullMessageConsumer createPullConsumer(ConsumerConfig consumerConfig)
            throws TubeClientException {
        return this.baseSessionFactory.createPullConsumer(consumerConfig);
    }

}

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
import com.tencent.tubemq.client.consumer.PullMessageConsumer;
import com.tencent.tubemq.client.consumer.PushMessageConsumer;
import com.tencent.tubemq.client.consumer.SimplePullMessageConsumer;
import com.tencent.tubemq.client.consumer.SimplePushMessageConsumer;
import com.tencent.tubemq.client.exception.TubeClientException;
import com.tencent.tubemq.client.producer.MessageProducer;
import com.tencent.tubemq.client.producer.ProducerManager;
import com.tencent.tubemq.client.producer.SimpleMessageProducer;
import com.tencent.tubemq.client.producer.qltystats.DefaultBrokerRcvQltyStats;
import com.tencent.tubemq.corebase.Shutdownable;
import com.tencent.tubemq.corebase.cluster.MasterInfo;
import com.tencent.tubemq.corerpc.RpcConfig;
import com.tencent.tubemq.corerpc.RpcConstants;
import com.tencent.tubemq.corerpc.RpcServiceFactory;
import com.tencent.tubemq.corerpc.client.ClientFactory;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TubeBaseSessionFactory implements InnerSessionFactory {

    private static final Logger logger =
            LoggerFactory.getLogger(TubeBaseSessionFactory.class);
    private final RpcServiceFactory rpcServiceFactory;
    private final ProducerManager producerManager;
    private final TubeClientConfig tubeClientConfig;
    private final CopyOnWriteArrayList<Shutdownable> clientLst =
            new CopyOnWriteArrayList<Shutdownable>();
    private final DefaultBrokerRcvQltyStats brokerRcvQltyStats;
    private AtomicBoolean shutdown = new AtomicBoolean(false);

    public TubeBaseSessionFactory(final ClientFactory clientFactory,
                                  final TubeClientConfig tubeClientConfig) throws TubeClientException {
        super();
        this.checkConfig(tubeClientConfig);
        this.tubeClientConfig = tubeClientConfig;
        RpcConfig config = new RpcConfig();
        config.put(RpcConstants.RPC_LQ_STATS_DURATION,
                tubeClientConfig.getLinkStatsDurationMs());
        config.put(RpcConstants.RPC_LQ_FORBIDDEN_DURATION,
                tubeClientConfig.getLinkStatsForbiddenDurationMs());
        config.put(RpcConstants.RPC_LQ_MAX_ALLOWED_FAIL_COUNT,
                tubeClientConfig.getLinkStatsMaxAllowedFailTimes());
        config.put(RpcConstants.RPC_LQ_MAX_FAIL_FORBIDDEN_RATE,
                tubeClientConfig.getLinkStatsMaxForbiddenRate());
        this.rpcServiceFactory = new RpcServiceFactory(clientFactory, config);
        this.producerManager = new ProducerManager(this, this.tubeClientConfig);
        this.brokerRcvQltyStats =
                new DefaultBrokerRcvQltyStats(this.getRpcServiceFactory(), this.tubeClientConfig);
        logger.info(new StringBuilder(512)
                .append("Created Session Factory, the config is: ")
                .append(tubeClientConfig.toJsonString()).toString());
    }

    public TubeClientConfig getTubeClientConfig() {
        return this.tubeClientConfig;
    }


    public CopyOnWriteArrayList<Shutdownable> getCurrClients() {
        return this.clientLst;
    }

    private void checkConfig(final TubeClientConfig tubeClientConfig) throws TubeClientException {
        if (tubeClientConfig == null) {
            throw new TubeClientException("null configuration");
        }
        MasterInfo masterInfo = tubeClientConfig.getMasterInfo();
        if ((masterInfo == null) || masterInfo.getAddrMap4failover().isEmpty()) {
            throw new TubeClientException("Blank MasterInfo content in ClientConfig");
        }
    }

    public DefaultBrokerRcvQltyStats getBrokerRcvQltyStats() {
        return this.brokerRcvQltyStats;
    }

    @Override
    public void shutdown() throws TubeClientException {
        logger.info("[SHUTDOWN_TUBE] Shutting down tube factory...");
        if (this.shutdown.get()) {
            return;
        }
        if (this.shutdown.compareAndSet(false, true)) {
            for (final Shutdownable client : this.clientLst) {
                try {
                    client.shutdown();
                } catch (Throwable e) {
                    logger.error("[SHUTDOWN_TUBE] child shutdown failed", e);
                }
            }
            try {
                this.producerManager.shutdown();
            } catch (Throwable e2) {
                //
            }
            brokerRcvQltyStats.stopBrokerStatistic();
            try {
                rpcServiceFactory.destroy();
            } catch (Exception e2) {
                logger.error("Fail to destroy RpcServiceFactory!", e2);
            }
        }
    }

    @Override
    public MessageProducer createProducer() throws TubeClientException {
        this.brokerRcvQltyStats.startBrokerStatistic();
        try {
            this.producerManager.start();
        } catch (Throwable e) {
            if (e instanceof TubeClientException) {
                throw (TubeClientException) e;
            } else {
                throw new TubeClientException("Create Producer failure, ", e);
            }
        }
        return this.addClient(new SimpleMessageProducer(this, this.tubeClientConfig));
    }

    @Override
    public RpcServiceFactory getRpcServiceFactory() {
        return this.rpcServiceFactory;
    }

    @Override
    public ProducerManager getProducerManager() {
        return this.producerManager;
    }

    @Override
    public <T extends Shutdownable> void removeClient(final T client) {
        this.clientLst.remove(client);
    }

    @Override
    public PullMessageConsumer createPullConsumer(ConsumerConfig consumerConfig)
            throws TubeClientException {
        if (!tubeClientConfig.getMasterInfo().equals(consumerConfig.getMasterInfo())) {
            throw new TubeClientException(new StringBuilder(512)
                    .append("consumerConfig's masterInfo not equal!")
                    .append(" SessionFactory's masterInfo is ")
                    .append(tubeClientConfig.getMasterInfo())
                    .append(", consumerConfig's masterInfo is ")
                    .append(consumerConfig.getMasterInfo()).toString());
        }
        return this.addClient(new SimplePullMessageConsumer(this, consumerConfig));
    }

    @Override
    public PushMessageConsumer createPushConsumer(final ConsumerConfig consumerConfig)
            throws TubeClientException {
        if (!tubeClientConfig.getMasterInfo().equals(consumerConfig.getMasterInfo())) {
            throw new TubeClientException(new StringBuilder(512)
                    .append("consumerConfig's masterInfo not equal!")
                    .append(" SessionFactory's masterInfo is ")
                    .append(tubeClientConfig.getMasterInfo())
                    .append(", consumerConfig's masterInfo is ")
                    .append(consumerConfig.getMasterInfo()).toString());
        }
        return this.addClient(new SimplePushMessageConsumer(this, consumerConfig));
    }

    public boolean isShutdown() {
        return shutdown.get();
    }

    private <T extends Shutdownable> T addClient(final T client) {
        this.clientLst.add(client);
        return client;
    }

}

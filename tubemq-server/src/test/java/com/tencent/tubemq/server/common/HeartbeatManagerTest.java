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

package com.tencent.tubemq.server.common;

import com.tencent.tubemq.server.common.heartbeat.HeartbeatManager;
import com.tencent.tubemq.server.common.heartbeat.TimeoutInfo;
import com.tencent.tubemq.server.common.heartbeat.TimeoutListener;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartbeatManagerTest {
    private static final Logger logger = LoggerFactory.getLogger(HeartbeatManager.class);
    private static HeartbeatManager heartbeatManager;

    @BeforeClass
    public static void setup() {
        heartbeatManager = new HeartbeatManager();
    }

    @AfterClass
    public static void tearDown() {
        heartbeatManager.clearAllHeartbeat();
    }

    @Test
    public void testBrokerTimeout() {
        heartbeatManager.regBrokerCheckBusiness(1000,
                new TimeoutListener() {
                    @Override
                    public void onTimeout(final String nodeId, TimeoutInfo nodeInfo) throws Exception {
                        logger.info(new StringBuilder(512).append("[Broker Timeout] ")
                                .append(nodeId).toString());
                    }
                });
        heartbeatManager.regBrokerNode("node1");
        Assert.assertTrue(heartbeatManager.getBrokerRegMap().get("node1").getTimeoutTime() >
                System.currentTimeMillis());
    }

    @Test
    public void testConsumerTimeout() {
        heartbeatManager.regConsumerCheckBusiness(1000,
                new TimeoutListener() {
                    @Override
                    public void onTimeout(final String nodeId, TimeoutInfo nodeInfo) throws Exception {
                        logger.info(new StringBuilder(512).append("[Broker Timeout] ")
                                .append(nodeId).toString());
                    }
                });
        heartbeatManager.regConsumerNode("node1");
        Assert.assertTrue(heartbeatManager.getConsumerRegMap().get("node1").getTimeoutTime()
                > System.currentTimeMillis());
    }

    @Test
    public void testProducerTimeout() {
        heartbeatManager.regProducerCheckBusiness(1000,
                new TimeoutListener() {
                    @Override
                    public void onTimeout(final String nodeId, TimeoutInfo nodeInfo) throws Exception {
                        logger.info(new StringBuilder(512).append("[Broker Timeout] ")
                                .append(nodeId).toString());
                    }
                });
        heartbeatManager.regProducerNode("node1");
        Assert.assertTrue(heartbeatManager.getProducerRegMap().get("node1").getTimeoutTime()
                > System.currentTimeMillis());
    }
}
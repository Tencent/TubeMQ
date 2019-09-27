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

package com.tencent.tubemq.server.master.nodemanage.nodeconsumer;

import static org.mockito.Mockito.mock;
import com.tencent.tubemq.corebase.balance.ConsumerEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ConsumerEventManagerTest {
    private ConsumerEventManager consumerEventManager;
    private ConsumerInfoHolder consumerInfoHolder;

    @Before
    public void setUp() throws Exception {
        consumerInfoHolder = mock(ConsumerInfoHolder.class);
        consumerEventManager = new ConsumerEventManager(consumerInfoHolder);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void eventTest() {
        ConsumerEvent event1 = mock(ConsumerEvent.class);
        consumerEventManager.addDisconnectEvent("consumer001", event1);
        ConsumerEvent event2 = mock(ConsumerEvent.class);
        consumerEventManager.addConnectEvent("consumer002", event2);

        Assert.assertTrue(consumerEventManager.hasEvent());
        Assert.assertEquals(2, consumerEventManager.getUnProcessedIdSet().size());

        consumerEventManager.removeAll("consumer001");
        consumerEventManager.removeAll("consumer002");
        Assert.assertFalse(consumerEventManager.hasEvent());
    }
}

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.tencent.tubemq.client.config.ConsumerConfig;
import com.tencent.tubemq.client.config.TubeClientConfig;
import com.tencent.tubemq.client.consumer.PullMessageConsumer;
import com.tencent.tubemq.client.consumer.PushMessageConsumer;
import com.tencent.tubemq.client.producer.MessageProducer;
import com.tencent.tubemq.client.producer.ProducerManager;
import com.tencent.tubemq.corebase.cluster.MasterInfo;
import com.tencent.tubemq.corebase.utils.AddressUtils;
import com.tencent.tubemq.corerpc.client.ClientFactory;
import java.lang.reflect.Field;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(AddressUtils.class)
public class TubeBaseSessionFactoryTest {

    @Test
    public void testTubeBaseSessionFactory() throws Exception {
        TubeClientConfig config = mock(TubeClientConfig.class);
        when(config.getMasterInfo()).thenReturn(new MasterInfo("192.168.1.1:18080"));

        ClientFactory clientFactory = mock(ClientFactory.class);
        PowerMockito.mockStatic(AddressUtils.class);
        PowerMockito.when(AddressUtils.getLocalAddress()).thenReturn("127.0.0.1:18080");

        TubeBaseSessionFactory factory = new TubeBaseSessionFactory(clientFactory, config);
        assertFalse(factory.isShutdown());

        // Use reflection to change the producer manager to a mock one
        ProducerManager mockPM = mock(ProducerManager.class);
        Field field = factory.getClass().getDeclaredField("producerManager");
        field.setAccessible(true);
        field.set(factory, mockPM);
        MessageProducer producer = factory.createProducer();
        assertEquals(1, factory.getCurrClients().size());

        final PullMessageConsumer pullMessageConsumer = factory.createPullConsumer(
                new ConsumerConfig("127.0.0.1:18080", "192.168.1.1:18080", "test"));
        final PushMessageConsumer pushMessageConsumer = factory.createPushConsumer(
                new ConsumerConfig("127.0.0.1:18081", "192.168.1.1:18080", "test"));

        assertEquals(3, factory.getCurrClients().size());
        factory.removeClient(producer);
        factory.removeClient(pullMessageConsumer);
        factory.removeClient(pushMessageConsumer);
        assertEquals(0, factory.getCurrClients().size());

        factory.shutdown();
        assertTrue(factory.isShutdown());
    }
}

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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.tencent.tubemq.client.config.ConsumerConfig;
import com.tencent.tubemq.client.config.TubeClientConfig;
import com.tencent.tubemq.client.factory.TubeBaseSessionFactory;
import com.tencent.tubemq.corebase.cluster.MasterInfo;
import com.tencent.tubemq.corebase.utils.AddressUtils;
import com.tencent.tubemq.corerpc.client.ClientFactory;
import com.tencent.tubemq.corerpc.netty.NettyClientFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(AddressUtils.class)
public class MessageFetchManagerTest {
    @Test
    public void testMessageFetchManager() throws Exception {
        TubeClientConfig clientConfig = mock(TubeClientConfig.class);
        PowerMockito.mockStatic(AddressUtils.class);
        PowerMockito.when(AddressUtils.getLocalAddress()).thenReturn("127.0.0.1");

        when(clientConfig.getMasterInfo()).thenReturn(new MasterInfo("192.168.1.1:18080"));
        ConsumerConfig config = new ConsumerConfig("182.168.1.2:18080", "192.168.1.1:18080", "test");
        ClientFactory clientFactory = new NettyClientFactory();
        TubeBaseSessionFactory factory = new TubeBaseSessionFactory(clientFactory, clientConfig);
        SimplePushMessageConsumer consumer = new SimplePushMessageConsumer(factory, config);
        MessageFetchManager fetchManager = new MessageFetchManager(config, consumer);

        Assert.assertFalse(fetchManager.isShutdown());
        fetchManager.startFetchWorkers();
        fetchManager.stopFetchWorkers(true);
        Assert.assertTrue(fetchManager.isShutdown());
    }
}

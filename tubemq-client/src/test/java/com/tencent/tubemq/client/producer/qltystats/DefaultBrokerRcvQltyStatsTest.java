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

package com.tencent.tubemq.client.producer.qltystats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.tencent.tubemq.client.config.TubeClientConfig;
import com.tencent.tubemq.corebase.cluster.BrokerInfo;
import com.tencent.tubemq.corebase.cluster.Partition;
import com.tencent.tubemq.corerpc.RpcServiceFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.Test;

public class DefaultBrokerRcvQltyStatsTest {

    @Test
    public void testStartBrokerStatistic() throws Exception {
        RpcServiceFactory rpcServiceFactory = mock(RpcServiceFactory.class);
        when(rpcServiceFactory.getForbiddenAddrMap()).thenReturn(new ConcurrentHashMap<String, Long>());

        TubeClientConfig config = mock(TubeClientConfig.class);
        when(config.getSessionMaxAllowedDelayedMsgCount()).thenReturn(1000L);

        DefaultBrokerRcvQltyStats stats = new DefaultBrokerRcvQltyStats(rpcServiceFactory, config);
        stats.startBrokerStatistic();
        assertFalse(stats.isStopped());

        stats.addSendStatistic(0);
        stats.addReceiveStatistic(0, true);

        stats.statisticDltBrokerStatus();

        // Test getAllowedBrokerPartitions
        Map<Integer, List<Partition>> brokerPartList = new HashMap<>();
        List<Partition> partitions = new ArrayList<>();
        partitions.add(new Partition(new BrokerInfo("0:192.168.0.1:18080"), "test_topic", 1));
        brokerPartList.put(0, partitions);

        List<Partition> actualPartitions = stats.getAllowedBrokerPartitions(brokerPartList);
        assertEquals(1, actualPartitions.size());
        assertEquals(1, actualPartitions.get(0).getPartitionId());

        // Unregister and stop statistic server
        stats.removeUnRegisteredBroker(new ArrayList<Integer>());
        stats.stopBrokerStatistic();
        assertTrue(stats.isStopped());
    }
}

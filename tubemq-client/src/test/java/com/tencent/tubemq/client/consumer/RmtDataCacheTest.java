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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import com.tencent.tubemq.corebase.cluster.BrokerInfo;
import com.tencent.tubemq.corebase.cluster.Partition;
import com.tencent.tubemq.corebase.policies.FlowCtrlRuleHandler;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.Test;

public class RmtDataCacheTest {

    @Test
    public void testRmtDataCache() {
        FlowCtrlRuleHandler groupFlowCtrlRuleHandler = new FlowCtrlRuleHandler(false);
        FlowCtrlRuleHandler defFlowCtrlRuleHandler = new FlowCtrlRuleHandler(true);
        List<Partition> partitions = new ArrayList<>();
        BrokerInfo brokerInfo = new BrokerInfo(1, "127.0.0.1", 18080);
        Partition expectPartition = new Partition(brokerInfo, "test", 1);
        partitions.add(expectPartition);

        RmtDataCache cache = new RmtDataCache(defFlowCtrlRuleHandler, groupFlowCtrlRuleHandler, partitions);
        List<Partition> brokerPartitions = cache.getBrokerPartitionList(brokerInfo);
        assertEquals(1, brokerPartitions.size());
        assertEquals(expectPartition.getPartitionId(), brokerPartitions.get(0).getPartitionId());

        ConcurrentLinkedQueue<Partition> partitionQueue = cache.getPartitionByBroker(brokerInfo);
        assertEquals(1, partitionQueue.size());
        assertEquals(expectPartition.getPartitionId(), partitionQueue.peek().getPartitionId());

        Set<BrokerInfo> brokerInfos = cache.getAllRegisterBrokers();
        assertEquals(1, brokerInfos.size());
        assertTrue(brokerInfos.contains(brokerInfo));

        assertEquals(expectPartition.getPartitionId(), cache.getPartitonByKey("1:test:1").getBrokerId());
        cache.addPartition(new Partition(brokerInfo, "test", 2), 10);
        assertEquals(2, cache.getBrokerPartitionList(brokerInfo).size());

        assertTrue(cache.isPartitionsReady(1000));
        cache.pullSelect();
        cache.pushSelect();
        cache.removePartition(expectPartition);
        assertEquals(1, cache.getBrokerPartitionList(brokerInfo).size());
        assertEquals(1, cache.getCurPartitionInfoMap().size());
        assertEquals(1, cache.getAllPartitionListWithStatus().size());
        cache.resumeTimeoutConsumePartitions(1000);
        Map<BrokerInfo, List<Partition>> infoMap = new HashMap<>();
        cache.removeAndGetPartition(infoMap, new ArrayList<String>(), 1000, true);
        cache.getSubscribeInfoList("test", "test");
        cache.errReqRelease("1:test:2", 1000, true);
        cache.succRspRelease("1:test:2", "test", 1000, true, true, 1000);
        cache.close();
    }
}

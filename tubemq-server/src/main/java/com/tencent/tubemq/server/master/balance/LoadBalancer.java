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

package com.tencent.tubemq.server.master.balance;

import com.tencent.tubemq.corebase.cluster.ConsumerInfo;
import com.tencent.tubemq.corebase.cluster.Partition;
import com.tencent.tubemq.server.common.offsetstorage.OffsetStorage;
import com.tencent.tubemq.server.master.nodemanage.nodebroker.BrokerConfManage;
import com.tencent.tubemq.server.master.nodemanage.nodebroker.BrokerInfoHolder;
import com.tencent.tubemq.server.master.nodemanage.nodebroker.TopicPSInfoManager;
import com.tencent.tubemq.server.master.nodemanage.nodeconsumer.ConsumerInfoHolder;
import java.util.List;
import java.util.Map;


public interface LoadBalancer {

    Map<String, Map<String, List<Partition>>> balanceCluster(
            Map<String, Map<String, Map<String, Partition>>> clusterState,
            ConsumerInfoHolder consumerHolder,
            BrokerInfoHolder brokerHolder,
            TopicPSInfoManager topicPSInfoManager,
            List<String> groups,
            BrokerConfManage brokerConfManage,
            int defAllowBClientRate,
            final StringBuilder sBuilder);

    Map<String, Map<String, Map<String, Partition>>> resetBalanceCluster(
            Map<String, Map<String, Map<String, Partition>>> clusterState,
            ConsumerInfoHolder consumerHolder,
            TopicPSInfoManager topicPSInfoManager,
            List<String> groups,
            OffsetStorage zkOffsetStorage,
            BrokerConfManage defaultBrokerConfManage,
            final StringBuilder sBuilder);

    Map<String, Map<String, List<Partition>>> bukAssign(ConsumerInfoHolder consumerHolder,
                                                        TopicPSInfoManager topicPSInfoManager,
                                                        List<String> groups,
                                                        BrokerConfManage brokerConfManage,
                                                        int defAllowBClientRate,
                                                        final StringBuilder sBuilder);

    Map<String, Map<String, Map<String, Partition>>> resetBukAssign(ConsumerInfoHolder consumerHolder,
                                                                    TopicPSInfoManager topicPSInfoManager,
                                                                    List<String> groups,
                                                                    OffsetStorage zkOffsetStorage,
                                                                    BrokerConfManage defaultBrokerConfManage,
                                                                    final StringBuilder sBuilder);

    Map<String, List<Partition>> roundRobinAssignment(List<Partition> partitions,
                                                      List<String> consumers);


    ConsumerInfo randomAssignment(List<ConsumerInfo> servers);
}

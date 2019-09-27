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

import com.tencent.tubemq.corebase.TokenConstants;
import com.tencent.tubemq.corebase.cluster.ConsumerInfo;
import com.tencent.tubemq.corebase.cluster.Partition;
import com.tencent.tubemq.server.common.offsetstorage.OffsetStorage;
import com.tencent.tubemq.server.common.offsetstorage.OffsetStorageInfo;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbBrokerConfEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbConsumeGroupSettingEntity;
import com.tencent.tubemq.server.master.nodemanage.nodebroker.BrokerConfManage;
import com.tencent.tubemq.server.master.nodemanage.nodebroker.BrokerInfoHolder;
import com.tencent.tubemq.server.master.nodemanage.nodebroker.TopicPSInfoManager;
import com.tencent.tubemq.server.master.nodemanage.nodeconsumer.ConsumerBandInfo;
import com.tencent.tubemq.server.master.nodemanage.nodeconsumer.ConsumerInfoHolder;
import com.tencent.tubemq.server.master.nodemanage.nodeconsumer.NodeRebInfo;
import com.tencent.tubemq.server.master.nodemanage.nodeconsumer.RebProcessInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Load balance class for server side load balance, (partition size) mod (consumer size) */
public class DefaultLoadBalancer implements LoadBalancer {
    private static final Logger logger = LoggerFactory.getLogger(LoadBalancer.class);
    private static final Random RANDOM = new Random(System.currentTimeMillis());

    public DefaultLoadBalancer() {

    }

    /**
     * Load balance
     *
     * @param clusterState
     * @param consumerHolder
     * @param brokerHolder
     * @param topicPSInfoManager
     * @param groupSet
     * @param brokerConfManage
     * @param defAllowBClientRate
     * @param strBuffer
     * @return
     */
    public Map<String, Map<String, List<Partition>>> balanceCluster(
            Map<String, Map<String, Map<String, Partition>>> clusterState,
            ConsumerInfoHolder consumerHolder,
            BrokerInfoHolder brokerHolder,
            TopicPSInfoManager topicPSInfoManager,
            List<String> groupSet,
            BrokerConfManage brokerConfManage,
            int defAllowBClientRate,
            final StringBuilder strBuffer) {
        // #lizard forgives
        //　load balance according to group
        Map<String/* consumer */,
                Map<String/* topic */, List<Partition>>> finalSubInfoMap =
                new HashMap<String, Map<String, List<Partition>>>();
        Map<String, RebProcessInfo> rejGroupClientINfoMap = new HashMap<String, RebProcessInfo>();
        Set<String> onlineOfflineGroupSet = new HashSet<String>();
        Set<String> bandGroupSet = new HashSet<String>();
        for (String group : groupSet) {
            if (group == null) {
                continue;
            }
            ConsumerBandInfo consumerBandInfo = consumerHolder.getConsumerBandInfo(group);
            if (consumerBandInfo == null) {
                continue;
            }
            List<ConsumerInfo> consumerList = consumerBandInfo.getConsumerInfoList();
            if (CollectionUtils.isEmpty(consumerList)) {
                continue;
            }
            // deal with regular consumer allocation, band consume allocation not in this part
            Map<String, String> partsConsumerMap =
                    consumerBandInfo.getPartitionInfoMap();
            if (consumerBandInfo.isBandConsume()
                    && consumerBandInfo.isNotAllocate()
                    && !partsConsumerMap.isEmpty()
                    && consumerBandInfo.getAllocatedTimes() < 2) {
                bandGroupSet.add(group);
                continue;
            }
            List<ConsumerInfo> newConsumerList = new ArrayList<ConsumerInfo>();
            for (ConsumerInfo consumerInfo : consumerList) {
                if (consumerInfo != null) {
                    newConsumerList.add(consumerInfo);
                }
            }
            if (CollectionUtils.isEmpty(newConsumerList)) {
                continue;
            }
            Set<String> topicSet = consumerBandInfo.getTopicSet();
            if (!consumerBandInfo.isBandConsume()
                    && consumerBandInfo.getRebalanceCheckStatus() <= 0) {
                // check if current client meet minimal requirements
                BdbConsumeGroupSettingEntity offsetResetGroupEntity =
                        brokerConfManage.getBdbConsumeGroupSetting(group);
                int confAllowBClientRate = (offsetResetGroupEntity != null
                        && offsetResetGroupEntity.getAllowedBrokerClientRate() > 0)
                        ? offsetResetGroupEntity.getAllowedBrokerClientRate() : -2;
                int allowRate = confAllowBClientRate > 0
                        ? confAllowBClientRate : defAllowBClientRate;
                int maxBrokerCount = topicPSInfoManager.getTopicMaxBrokerCount(topicSet);
                int curBClientRate = (int) Math.floor(maxBrokerCount / newConsumerList.size());
                if (curBClientRate > allowRate) {
                    int minClientCnt = maxBrokerCount / allowRate;
                    if (maxBrokerCount % allowRate != 0) {
                        minClientCnt += 1;
                    }
                    consumerHolder.setCurConsumeBClientInfo(group, defAllowBClientRate,
                            confAllowBClientRate, curBClientRate, minClientCnt, false);
                    if (consumerBandInfo.isRebalanCheckPrint()) {
                        logger.info(strBuffer.append("[UnBound Alloc 2] Not allocate partition :group(")
                                .append(group).append(")'s consumer getCachedSize(")
                                .append(consumerBandInfo.getGroupCnt())
                                .append(") low than min required client count:")
                                .append(minClientCnt).toString());
                        strBuffer.delete(0, strBuffer.length());
                    }
                    continue;
                } else {
                    consumerHolder.setCurConsumeBClientInfo(group,
                            defAllowBClientRate, confAllowBClientRate, curBClientRate, -2, true);
                }
            }
            RebProcessInfo rebProcessInfo = new RebProcessInfo();
            if (!consumerBandInfo.isRebalanceMapEmpty()) {
                rebProcessInfo = consumerHolder.getNeedRebNodeList(group);
                if (!rebProcessInfo.isProcessInfoEmpty()) {
                    rejGroupClientINfoMap.put(group, rebProcessInfo);
                }
            }
            List<ConsumerInfo> newConsumerList2 = new ArrayList<ConsumerInfo>();
            Map<String, Partition> psMap = topicPSInfoManager.getPartitionMap(topicSet);
            Map<String, NodeRebInfo> rebProcessInfoMap = consumerBandInfo.getRebalanceMap();
            for (ConsumerInfo consumer : newConsumerList) {
                Map<String, List<Partition>> partitions = new HashMap<String, List<Partition>>();
                finalSubInfoMap.put(consumer.getConsumerId(), partitions);
                Map<String, Map<String, Partition>> relation = clusterState.get(consumer.getConsumerId());
                if (relation != null) {
                    // filter client which can not meet requirements
                    if (rebProcessInfo.needProcessList.contains(consumer.getConsumerId())
                            || rebProcessInfo.needEscapeList.contains(consumer.getConsumerId())) {
                        NodeRebInfo tmpNodeRegInfo =
                                rebProcessInfoMap.get(consumer.getConsumerId());
                        if (tmpNodeRegInfo != null
                                && tmpNodeRegInfo.getReqType() == 0) {
                            newConsumerList2.add(consumer);
                        }
                        for (Entry<String, Map<String, Partition>> entry : relation.entrySet()) {
                            partitions.put(entry.getKey(), new ArrayList<Partition>());
                        }
                        continue;
                    }
                    newConsumerList2.add(consumer);
                    for (Entry<String, Map<String, Partition>> entry : relation.entrySet()) {
                        List<Partition> ps = new ArrayList<Partition>();
                        partitions.put(entry.getKey(), ps);
                        Map<String, Partition> partitionMap = entry.getValue();
                        if (partitionMap != null && !partitionMap.isEmpty()) {
                            for (Partition partition : partitionMap.values()) {
                                Partition curPart = psMap.remove(partition.getPartitionKey());
                                if (curPart != null) {
                                    ps.add(curPart);
                                }
                            }
                        }
                    }
                }
            }
            //　random allocate
            if (psMap.size() > 0) {
                onlineOfflineGroupSet.add(group);
                if (!newConsumerList2.isEmpty()) {
                    this.randomAssign(psMap, newConsumerList2,
                            finalSubInfoMap, clusterState, rebProcessInfo.needProcessList);
                }
            }
        }
        List<String> groupsNeedToBalance = null;
        if (onlineOfflineGroupSet.size() == 0) {
            groupsNeedToBalance = groupSet;
        } else {
            groupsNeedToBalance = new ArrayList<String>();
            for (String group : groupSet) {
                if (group == null) {
                    continue;
                }
                if (!onlineOfflineGroupSet.contains(group)) {
                    groupsNeedToBalance.add(group);
                }
            }
        }
        if (bandGroupSet.size() > 0) {
            for (String group : bandGroupSet) {
                groupsNeedToBalance.remove(group);
            }
        }
        if (groupsNeedToBalance.size() > 0) {
            finalSubInfoMap =
                    balance(finalSubInfoMap, consumerHolder, topicPSInfoManager,
                            groupsNeedToBalance, clusterState, rejGroupClientINfoMap);
        }
        if (!rejGroupClientINfoMap.isEmpty()) {
            for (Entry<String, RebProcessInfo> entry :
                    rejGroupClientINfoMap.entrySet()) {
                consumerHolder.setRebNodeProcessed(entry.getKey(),
                        entry.getValue().needProcessList);
            }
        }
        return finalSubInfoMap;
    }

    // #lizard forgives
    private Map<String, Map<String, List<Partition>>> balance(
            Map<String, Map<String, List<Partition>>> clusterState,
            ConsumerInfoHolder consumerHolder,
            TopicPSInfoManager topicPSInfoManager,
            List<String> groupSet,
            Map<String, Map<String, Map<String, Partition>>> oldClusterState,
            Map<String, RebProcessInfo> rejGroupClientINfoMap) {
        // according to group
        for (String group : groupSet) {
            ConsumerBandInfo consumerBandInfo = consumerHolder.getConsumerBandInfo(group);
            if (consumerBandInfo == null) {
                continue;
            }
            // filter consumer which don't need to handle
            List<ConsumerInfo> consumerList = new ArrayList<ConsumerInfo>();
            List<ConsumerInfo> consumerList1 = consumerBandInfo.getConsumerInfoList();
            RebProcessInfo rebProcessInfo = rejGroupClientINfoMap.get(group);
            if (rebProcessInfo != null) {
                for (ConsumerInfo consumerInfo : consumerList1) {
                    if (rebProcessInfo.needProcessList.contains(consumerInfo.getConsumerId())
                            || rebProcessInfo.needEscapeList.contains(consumerInfo.getConsumerId())) {
                        Map<String, List<Partition>> partitions2 =
                                clusterState.get(consumerInfo.getConsumerId());
                        if (partitions2 == null) {
                            partitions2 = new HashMap<String, List<Partition>>();
                            clusterState.put(consumerInfo.getConsumerId(), partitions2);
                        }
                        Map<String, Map<String, Partition>> relation =
                                oldClusterState.get(consumerInfo.getConsumerId());
                        if (relation != null) {
                            for (String topic : relation.keySet()) {
                                partitions2.put(topic, new ArrayList<Partition>());
                            }
                        }
                        continue;
                    }
                    consumerList.add(consumerInfo);
                }
            } else {
                consumerList = consumerList1;
            }
            if (CollectionUtils.isEmpty(consumerList)) {
                continue;
            }
            // sort consumer and partitions, then mod
            Set<String> topics = consumerBandInfo.getTopicSet();
            Map<String, Partition> psPartMap = topicPSInfoManager.getPartitionMap(topics);
            int min = psPartMap.size() / consumerList.size();
            int max = psPartMap.size() % consumerList.size() == 0 ? min : min + 1;
            int serverNumToLoadMax = psPartMap.size() % consumerList.size();
            Queue<Partition> partitionToMove = new LinkedBlockingQueue<Partition>();
            Map<String, Integer> serverToTake = new HashMap<String, Integer>();
            for (ConsumerInfo consumer : consumerList) {
                Map<String, List<Partition>> partitions =
                        clusterState.get(consumer.getConsumerId());
                if (partitions == null) {
                    partitions = new HashMap<String, List<Partition>>();
                }
                int load = 0;
                for (List<Partition> entry : partitions.values()) {
                    load += entry.size();
                }
                if (load < max) {
                    if (load == 0) {
                        serverToTake.put(consumer.getConsumerId(), max - load);
                    } else if (load < min) {
                        serverToTake.put(consumer.getConsumerId(), max - load);
                    }
                    continue;
                }
                int numToOffload;
                if (serverNumToLoadMax > 0) {
                    serverNumToLoadMax--;
                    numToOffload = load - max;
                } else {
                    numToOffload = load - min;
                }
                // calculate if current consumer partition need to release or add
                for (List<Partition> entry : partitions.values()) {
                    if (entry.size() > numToOffload) {
                        int condition = numToOffload;
                        for (int i = 0; i < condition; i++) {
                            partitionToMove.add(entry.remove(0));
                            numToOffload--;
                        }
                        if (numToOffload <= 0) {
                            break;
                        }
                    } else {
                        numToOffload -= entry.size();
                        partitionToMove.addAll(entry);
                        entry.clear();
                        if (numToOffload <= 0) {
                            break;
                        }
                    }
                }
            }
            // random allocate the rest partition
            for (Entry<String, Integer> entry : serverToTake.entrySet()) {
                for (int i = 0; i < entry.getValue() && partitionToMove.size() > 0; i++) {
                    Partition partition = partitionToMove.poll();
                    assign(partition, clusterState, entry.getKey());
                }
            }
            // load balance partition between consumer
            if (partitionToMove.size() > 0) {
                for (String consumerId : serverToTake.keySet()) {
                    if (partitionToMove.size() <= 0) {
                        break;
                    }
                    assign(partitionToMove.poll(), clusterState, consumerId);
                }
            }

        }
        return clusterState;
    }

    private void assign(Partition partition,
                        Map<String, Map<String, List<Partition>>> clusterState,
                        String consumerId) {
        Map<String, List<Partition>> partitions = clusterState.get(consumerId);
        if (partitions == null) {
            partitions = new HashMap<String, List<Partition>>();
            clusterState.put(consumerId, partitions);
        }
        List<Partition> ps = partitions.get(partition.getTopic());
        if (ps == null) {
            ps = new ArrayList<Partition>();
            partitions.put(partition.getTopic(), ps);
        }
        ps.add(partition);
    }

    /**
     * Random assign partition
     *
     * @param partitionToAssignMap
     * @param consumerList
     * @param clusterState
     * @param oldClusterState
     * @param filterList
     */
    private void randomAssign(Map<String, Partition> partitionToAssignMap,
                              List<ConsumerInfo> consumerList,
                              Map<String, Map<String, List<Partition>>> clusterState,
                              Map<String, Map<String, Map<String, Partition>>> oldClusterState,
                              List<String> filterList) {
        int searched = 1;
        int consumerSize = consumerList.size();
        for (Partition partition : partitionToAssignMap.values()) {
            ConsumerInfo consumer = null;
            int indeId = RANDOM.nextInt(consumerSize);
            do {
                searched = 1;
                consumer = consumerList.get(indeId);
                if (filterList.contains(consumer.getConsumerId())) {
                    if (consumerList.size() == 1) {
                        searched = 0;
                        break;
                    }
                    Map<String, Map<String, Partition>> oldPartitionMap =
                            oldClusterState.get(consumer.getConsumerId());
                    if (oldPartitionMap != null) {
                        Map<String, Partition> oldPartitions =
                                oldPartitionMap.get(partition.getTopic());
                        if (oldPartitions != null) {
                            if (oldPartitions.get(partition.getPartitionKey()) != null) {
                                searched = 2;
                                indeId = (indeId + 1) % consumerSize;
                            }
                        }
                    }
                }
            } while (searched >= 2);
            if (searched == 0) {
                return;
            }
            Map<String, List<Partition>> partitions = clusterState.get(consumer.getConsumerId());
            if (partitions == null) {
                partitions = new HashMap<String, List<Partition>>();
                clusterState.put(consumer.getConsumerId(), partitions);
            }
            List<Partition> ps = partitions.get(partition.getTopic());
            if (ps == null) {
                ps = new ArrayList<Partition>();
                partitions.put(partition.getTopic(), ps);
            }
            ps.add(partition);
        }
    }

    /**
     * Round robin assign partitions
     *
     * @param partitions
     * @param consumers
     * @return
     */
    public Map<String, List<Partition>> roundRobinAssignment(List<Partition> partitions,
                                                             List<String> consumers) {
        if (partitions.isEmpty() || consumers.isEmpty()) {
            return null;
        }
        Map<String, List<Partition>> assignments = new TreeMap<String, List<Partition>>();
        int numPartitions = partitions.size();
        int numServers = consumers.size();
        int max = (int) Math.ceil((float) numPartitions / numServers);
        int serverIdx = 0;
        if (numServers > 1) {
            serverIdx = RANDOM.nextInt(numServers);
        }
        int partitionIdx = 0;
        for (int j = 0; j < numServers; j++) {
            String server = consumers.get((j + serverIdx) % numServers);
            List<Partition> serverPartitions = new ArrayList<Partition>(max);
            for (int i = partitionIdx; i < numPartitions; i += numServers) {
                serverPartitions.add(partitions.get(i % numPartitions));
            }
            assignments.put(server, serverPartitions);
            partitionIdx++;
        }
        return assignments;
    }

    @Override
    public ConsumerInfo randomAssignment(List<ConsumerInfo> consumers) {
        if (consumers == null || consumers.isEmpty()) {
            logger.warn("Wanted to do random assignment but no servers to assign to");
            return null;
        }

        return consumers.get(RANDOM.nextInt(consumers.size()));
    }

    /**
     * Assign consumer partitions
     *
     * @param consumerHolder
     * @param topicPSInfoManager
     * @param groupSet
     * @param brokerConfManage
     * @param defAllowBClientRate
     * @param strBuffer
     * @return
     */
    @Override
    public Map<String, Map<String, List<Partition>>> bukAssign(
            ConsumerInfoHolder consumerHolder,
            TopicPSInfoManager topicPSInfoManager,
            List<String> groupSet,
            BrokerConfManage brokerConfManage,
            int defAllowBClientRate,
            final StringBuilder strBuffer) {
        // #lizard forgives
        // regular consumer allocate operation
        Map<String, Map<String, List<Partition>>> finalSubInfoMap =
                new HashMap<String, Map<String, List<Partition>>>();
        for (String group : groupSet) {
            ConsumerBandInfo consumerBandInfo = consumerHolder.getConsumerBandInfo(group);
            // filter empty consumer
            if (consumerBandInfo == null) {
                continue;
            }

            List<ConsumerInfo> consumerList = consumerBandInfo.getConsumerInfoList();
            if (CollectionUtils.isEmpty(consumerList)) {
                continue;
            }
            Map<String, String> partsConsumerMap =
                    consumerBandInfo.getPartitionInfoMap();
            if (consumerBandInfo.isBandConsume()
                    && consumerBandInfo.isNotAllocate()
                    && !partsConsumerMap.isEmpty()
                    && consumerBandInfo.getAllocatedTimes() < 2) {
                continue;
            }
            // check if current client meet minimal requirements
            Set<String> topicSet = consumerBandInfo.getTopicSet();
            BdbConsumeGroupSettingEntity offsetResetGroupEntity =
                    brokerConfManage.getBdbConsumeGroupSetting(group);
            int confAllowBClientRate = (offsetResetGroupEntity != null
                    && offsetResetGroupEntity.getAllowedBrokerClientRate() > 0)
                    ? offsetResetGroupEntity.getAllowedBrokerClientRate() : -2;
            int allowRate = confAllowBClientRate > 0
                    ? confAllowBClientRate : defAllowBClientRate;
            int maxBrokerCount = topicPSInfoManager.getTopicMaxBrokerCount(topicSet);
            int curBClientRate = (int) Math.floor(maxBrokerCount / consumerList.size());
            if (curBClientRate > allowRate) {
                int minClientCnt = maxBrokerCount / allowRate;
                if (maxBrokerCount % allowRate != 0) {
                    minClientCnt += 1;
                }
                consumerHolder.setCurConsumeBClientInfo(group,
                        defAllowBClientRate, confAllowBClientRate,
                        curBClientRate, minClientCnt, false);
                if (consumerBandInfo.isRebalanCheckPrint()) {
                    logger.info(strBuffer.append("[UnBound Alloc 1] Not allocate partition :group(")
                            .append(group).append(")'s consumer getCachedSize(")
                            .append(consumerBandInfo.getGroupCnt())
                            .append(") low than min required client count:")
                            .append(minClientCnt).toString());
                    strBuffer.delete(0, strBuffer.length());
                }
                continue;
            } else {
                consumerHolder.setCurConsumeBClientInfo(group,
                        defAllowBClientRate, confAllowBClientRate, curBClientRate, -2, true);
            }
            // sort and mod
            Collections.sort(consumerList);
            for (String topic : topicSet) {
                List<Partition> partPubList = topicPSInfoManager.getPartitionList(topic);
                Collections.sort(partPubList);
                int partsPerConsumer = partPubList.size() / consumerList.size();
                int consumersWithExtraPart = partPubList.size() % consumerList.size();
                for (int i = 0; i < consumerList.size(); i++) {
                    String consumerId = consumerList.get(i).getConsumerId();
                    Map<String, List<Partition>> topicSubPartMap =
                            finalSubInfoMap.get(consumerId);
                    if (topicSubPartMap == null) {
                        topicSubPartMap = new HashMap<String, List<Partition>>();
                        finalSubInfoMap.put(consumerId, topicSubPartMap);
                    }
                    List<Partition> partList = topicSubPartMap.get(topic);
                    if (partList == null) {
                        partList = new ArrayList<Partition>();
                        topicSubPartMap.put(topic, partList);
                    }
                    int startIndex = partsPerConsumer * i + Math.min(i, consumersWithExtraPart);
                    int parts = partsPerConsumer + ((i + 1) > consumersWithExtraPart ? 0 : 1);
                    for (int j = startIndex; j < startIndex + parts; j++) {
                        final Partition part = partPubList.get(j);
                        partList.add(part);
                    }
                }
            }
        }
        return finalSubInfoMap;
    }

    /**
     * Reset
     *
     * @param consumerHolder
     * @param topicPSInfoManager
     * @param groupSet
     * @param zkOffsetStorage
     * @param defaultBrokerConfManage
     * @param strBuffer
     * @return
     */
    @Override
    public Map<String, Map<String, Map<String, Partition>>> resetBukAssign(
            ConsumerInfoHolder consumerHolder, TopicPSInfoManager topicPSInfoManager,
            List<String> groupSet, OffsetStorage zkOffsetStorage,
            BrokerConfManage defaultBrokerConfManage, final StringBuilder strBuffer) {
        return inReBalanceCluster(false, consumerHolder,
                topicPSInfoManager, groupSet, zkOffsetStorage, defaultBrokerConfManage, strBuffer);
    }

    /**
     * Reset balance
     *
     * @param clusterState
     * @param consumerHolder
     * @param topicPSInfoManager
     * @param groupSet
     * @param zkOffsetStorage
     * @param defaultBrokerConfManage
     * @param strBuffer
     * @return
     */
    public Map<String, Map<String, Map<String, Partition>>> resetBalanceCluster(
            Map<String, Map<String, Map<String, Partition>>> clusterState,
            ConsumerInfoHolder consumerHolder, TopicPSInfoManager topicPSInfoManager,
            List<String> groupSet, OffsetStorage zkOffsetStorage,
            BrokerConfManage defaultBrokerConfManage, final StringBuilder strBuffer) {
        return inReBalanceCluster(true, consumerHolder,
                topicPSInfoManager, groupSet, zkOffsetStorage, defaultBrokerConfManage, strBuffer);
    }

    // #lizard forgives
    private Map<String, Map<String, Map<String, Partition>>> inReBalanceCluster(
            boolean isResetRebalance,
            ConsumerInfoHolder consumerHolder,
            TopicPSInfoManager topicPSInfoManager,
            List<String> groupSet,
            OffsetStorage zkOffsetStorage,
            BrokerConfManage defaultBrokerConfManage,
            final StringBuilder strBuffer) {
        // band consume reset offset
        Map<String, Map<String, Map<String, Partition>>> finalSubInfoMap =
                new HashMap<String, Map<String, Map<String, Partition>>>();
        // according group
        for (String group : groupSet) {
            if (group == null) {
                continue;
            }
            // filter band consumer
            ConsumerBandInfo consumerBandInfo = consumerHolder.getConsumerBandInfo(group);
            if (consumerBandInfo == null) {
                continue;
            }
            List<ConsumerInfo> consumerList = consumerBandInfo.getConsumerInfoList();
            if (CollectionUtils.isEmpty(consumerList)) {
                continue;
            }
            Map<String, String> partsConsumerMap =
                    consumerBandInfo.getPartitionInfoMap();
            if (!consumerBandInfo.isBandConsume()
                    || !consumerBandInfo.isNotAllocate()
                    || partsConsumerMap.isEmpty()
                    || consumerBandInfo.getAllocatedTimes() >= 2) {
                continue;
            }
            if (!consumerBandInfo.isGroupFullSize()) {
                // check if client size meet minimal requirements
                Long checkCycle = consumerHolder.addCurCheckCycle(group);
                if (isResetRebalance) {
                    if (checkCycle != null && checkCycle % 15 == 0) {
                        logger.info(strBuffer.append("[Bound Alloc 2] Not allocate partition :group(")
                                .append(group).append(")'s consumer getCachedSize(")
                                .append(consumerBandInfo.getGroupCnt())
                                .append(") low than required source count:")
                                .append(consumerBandInfo.getSourceCount())
                                .append(", checked cycle is ")
                                .append(checkCycle).toString());
                        strBuffer.delete(0, strBuffer.length());
                    }
                } else {
                    logger.info(strBuffer.append("[Bound Alloc 1] Not allocate partition :group(")
                            .append(group).append(")'s consumer getCachedSize(")
                            .append(consumerBandInfo.getGroupCnt())
                            .append(") low than required source count:")
                            .append(consumerBandInfo.getSourceCount()).toString());
                    strBuffer.delete(0, strBuffer.length());
                }
                continue;
            }
            // actual reset offset
            Map<String, Long> partsOffsetMap = consumerBandInfo.getPartOffsetMap();
            List<OffsetStorageInfo> offsetInfoList = new ArrayList<OffsetStorageInfo>();
            Set<Partition> partPubList =
                    topicPSInfoManager.getPartitions(consumerBandInfo.getTopicSet());
            Map<String, Partition> partitionMap = new HashMap<String, Partition>();
            for (Partition partition : partPubList) {
                partitionMap.put(partition.getPartitionKey(), partition);
            }
            for (Entry<String, String> entry : partsConsumerMap.entrySet()) {
                Partition foundPart = partitionMap.get(entry.getKey());
                if (foundPart != null) {
                    if (partsOffsetMap.get(entry.getKey()) != null) {
                        offsetInfoList.add(new OffsetStorageInfo(foundPart.getTopic(),
                                foundPart.getBroker().getBrokerId(),
                                foundPart.getPartitionId(),
                                partsOffsetMap.get(entry.getKey()), 0));
                    }
                    String consumerId = entry.getValue();
                    Map<String, Map<String, Partition>> topicSubPartMap =
                            finalSubInfoMap.get(consumerId);
                    if (topicSubPartMap == null) {
                        topicSubPartMap = new HashMap<String, Map<String, Partition>>();
                        finalSubInfoMap.put(consumerId, topicSubPartMap);
                    }
                    Map<String, Partition> partMap = topicSubPartMap.get(foundPart.getTopic());
                    if (partMap == null) {
                        partMap = new HashMap<>();
                        topicSubPartMap.put(foundPart.getTopic(), partMap);
                    }
                    partMap.put(foundPart.getPartitionKey(), foundPart);
                    partitionMap.remove(entry.getKey());
                } else {
                    String[] partitionKeyItems = entry.getKey().split(TokenConstants.ATTR_SEP);
                    BdbBrokerConfEntity bdbBrokerConfEntity = defaultBrokerConfManage
                            .getBrokerDefaultConfigStoreInfo(Integer.valueOf(partitionKeyItems[0]));
                    if (bdbBrokerConfEntity != null) {
                        if (partsOffsetMap.get(entry.getKey()) != null) {
                            offsetInfoList.add(new OffsetStorageInfo(partitionKeyItems[1],
                                    bdbBrokerConfEntity.getBrokerId(),
                                    Integer.valueOf(partitionKeyItems[2]),
                                    partsOffsetMap.get(entry.getKey()), 0));
                        }
                    }
                }
            }
            zkOffsetStorage.commitOffset(group, offsetInfoList, false);
            consumerHolder.addAllocatedTimes(group);
        }
        return finalSubInfoMap;
    }
}

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

import com.tencent.tubemq.corebase.cluster.ConsumerInfo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class ConsumerBandInfo {

    private boolean isBandConsume = false;

    //session key, the same batch consumer have the same session key
    private String sessionKey = "";
    private long sessionTime = -1;  //session start time
    private int sourceCount = 0;    //consumer count(specific by client)
    private boolean isSelectedBig = true;   //select the bigger offset when offset conflict
    private AtomicBoolean notAllocate = new AtomicBoolean(true); //allocate offset flag
    private AtomicLong curCheckCycle = new AtomicLong(0);       //current check cycle
    private AtomicInteger allocatedTimes = new AtomicInteger(0);    //allocate times
    private long createTime = System.currentTimeMillis();           //create time
    private Set<String> topicSet = new HashSet<>();                 //topic set
    private Map<String, TreeSet<String>> topicConditions =          //filter condition set
            new HashMap<String, TreeSet<String>>();
    private ConcurrentHashMap<String, ConsumerInfo> consumerInfoMap =   //consumer info
            new ConcurrentHashMap<String, ConsumerInfo>();
    private ConcurrentHashMap<String, String> partitionInfoMap =        //partition info
            new ConcurrentHashMap<String, String>();
    private ConcurrentHashMap<String, Long> partOffsetMap =             //partition offset
            new ConcurrentHashMap<String, Long>();
    private ConcurrentHashMap<String, NodeRebInfo> rebalanceMap =       //load balance
            new ConcurrentHashMap<String, NodeRebInfo>();
    private int defBClientRate = -2;            //default broker/client ratio
    private int confBClientRate = -2;           //config broker/client ratio
    private int curBClientRate = -2;            //current broker/client ratio
    private int minRequireClientCnt = -2;       //minimal client count according to above ratio
    private int rebalanceCheckStatus = -2;      //rebalance check status
    private boolean rebalanCheckPrint = true;   //log print flag

    public ConsumerBandInfo(boolean isSelectedBig) {
        this.sessionKey = "";
        this.sessionTime = -1;
        this.sourceCount = -1;
        this.curCheckCycle.set(0);
        this.allocatedTimes.set(0);
        this.notAllocate.set(true);
        this.isBandConsume = false;
        this.isSelectedBig = isSelectedBig;
    }

    public ConsumerBandInfo(boolean isBandConsume, String sessionKey,
                            long sessionTime, int sourceCount, long createTime,
                            long curCheckCycle, boolean notAllocate, int allocatedTimes,
                            boolean isSelectedBig, Set<String> topicSet,
                            Map<String, TreeSet<String>> topicConditions,
                            Map<String, ConsumerInfo> consumerInfoMap,
                            Map<String, String> partitionInfoMap,
                            Map<String, Long> partOffsetMap,
                            Map<String, NodeRebInfo> rebalanceMap,
                            int defBClientRate, int confBClientRate,
                            int curBClientRate, int minRequireClientCnt,
                            int rebalanceCheckStatus, boolean rebalanCheckPrint) {
        this.isBandConsume = isBandConsume;
        this.sessionKey = sessionKey;
        this.sessionTime = sessionTime;
        this.sourceCount = sourceCount;
        this.createTime = createTime;
        this.isSelectedBig = isSelectedBig;
        this.notAllocate.set(notAllocate);
        this.allocatedTimes.set(allocatedTimes);
        this.curCheckCycle.set(curCheckCycle);
        this.defBClientRate = defBClientRate;
        this.confBClientRate = confBClientRate;
        this.curBClientRate = curBClientRate;
        this.minRequireClientCnt = minRequireClientCnt;
        this.rebalanceCheckStatus = rebalanceCheckStatus;
        this.rebalanCheckPrint = rebalanCheckPrint;
        for (String topic : topicSet) {
            this.topicSet.add(topic);
        }
        for (Map.Entry<String, ConsumerInfo> entry : consumerInfoMap.entrySet()) {
            this.consumerInfoMap.put(entry.getKey(), entry.getValue().clone());
        }
        for (Map.Entry<String, TreeSet<String>> entry : topicConditions.entrySet()) {
            this.topicConditions.put(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, String> entry : partitionInfoMap.entrySet()) {
            if (entry.getValue() != null) {
                this.partitionInfoMap.put(entry.getKey(), entry.getValue());
            }
        }
        for (Map.Entry<String, Long> entry : partOffsetMap.entrySet()) {
            if (entry.getValue() != null) {
                this.partOffsetMap.put(entry.getKey(), entry.getValue());
            }
        }
        if (rebalanceMap != null) {
            for (Map.Entry<String, NodeRebInfo> entry : rebalanceMap.entrySet()) {
                if (entry.getValue() != null) {
                    this.rebalanceMap.put(entry.getKey(), entry.getValue().clone());
                }
            }
        }
    }

    /**
     * Add consumer
     *
     * @param consumer
     */
    public void addConsumer(ConsumerInfo consumer) {
        if (this.consumerInfoMap.isEmpty()) {
            this.isBandConsume = consumer.isRequireBound();
            if (this.isBandConsume) {
                this.sessionKey = consumer.getSessionKey();
                this.sessionTime = consumer.getStartTime();
                this.sourceCount = consumer.getSourceCount();
                this.createTime = System.currentTimeMillis();
                this.curCheckCycle.set(0);
            }
            for (String topic : consumer.getTopicSet()) {
                this.topicSet.add(topic);
            }
            for (Map.Entry<String, TreeSet<String>> entry
                    : consumer.getTopicConditions().entrySet()) {
                this.topicConditions.put(entry.getKey(), entry.getValue());
            }
        }
        this.consumerInfoMap.put(consumer.getConsumerId(), consumer);
        Map<String, Long> consumerPartMap = consumer.getRequiredPartition();
        if (!this.isBandConsume
                || consumerPartMap == null
                || consumerPartMap.isEmpty()) {
            return;
        }
        for (Map.Entry<String, Long> entry : consumerPartMap.entrySet()) {
            String oldClientId = this.partitionInfoMap.get(entry.getKey());
            if (oldClientId == null) {
                this.partitionInfoMap.put(entry.getKey(), consumer.getConsumerId());
                this.partOffsetMap.put(entry.getKey(), entry.getValue());
            } else {
                ConsumerInfo oldConsumerInfo = this.consumerInfoMap.get(oldClientId);
                if (oldConsumerInfo == null) {
                    this.partitionInfoMap.put(entry.getKey(), consumer.getConsumerId());
                    this.partOffsetMap.put(entry.getKey(), entry.getValue());
                } else {
                    Map<String, Long> oldConsumerPartMap = oldConsumerInfo.getRequiredPartition();
                    if (oldConsumerPartMap == null || oldConsumerPartMap.isEmpty()) {
                        this.partitionInfoMap.put(entry.getKey(), consumer.getConsumerId());
                        this.partOffsetMap.put(entry.getKey(), entry.getValue());
                    } else {
                        Long oldConsumerOff = oldConsumerPartMap.get(entry.getKey());
                        if (oldConsumerOff == null) {
                            this.partitionInfoMap.put(entry.getKey(), consumer.getConsumerId());
                            this.partOffsetMap.put(entry.getKey(), entry.getValue());
                        } else {
                            if (this.isSelectedBig) {
                                if (entry.getValue() >= oldConsumerOff) {
                                    this.partitionInfoMap.put(entry.getKey(), consumer.getConsumerId());
                                    this.partOffsetMap.put(entry.getKey(), entry.getValue());
                                }
                            } else {
                                if (entry.getValue() < oldConsumerOff) {
                                    this.partitionInfoMap.put(entry.getKey(), consumer.getConsumerId());
                                    this.partOffsetMap.put(entry.getKey(), entry.getValue());
                                }
                            }
                        }
                    }
                }
            }
        }

    }

    /**
     * Remove consumer
     *
     * @param consumerId
     * @return
     */
    public ConsumerInfo removeConsumer(String consumerId) {
        if (consumerId == null) {
            return null;
        }
        this.rebalanceMap.remove(consumerId);
        List<String> partKeyList = new ArrayList<String>();
        for (Map.Entry<String, String> entry : partitionInfoMap.entrySet()) {
            if (entry.getValue() != null) {
                if (entry.getValue().equals(consumerId)) {
                    partKeyList.add(entry.getKey());
                }
            }
        }
        for (String partKey : partKeyList) {
            partitionInfoMap.remove(partKey);
            partOffsetMap.remove(partKey);
        }
        return this.consumerInfoMap.remove(consumerId);
    }

    public void clear() {
        this.isBandConsume = false;
        this.notAllocate.set(true);
        this.sessionKey = "";
        this.sessionTime = -1;
        this.sourceCount = -1;
        this.createTime = -1;
        this.curCheckCycle.set(0);
        this.allocatedTimes.set(0);
        this.topicSet.clear();
        this.consumerInfoMap.clear();
        this.topicConditions.clear();
        this.partitionInfoMap.clear();
        this.partOffsetMap.clear();
        this.rebalanceMap.clear();
    }

    /**
     * Get consumer group count
     *
     * @return group count
     */
    public int getGroupCnt() {
        return this.consumerInfoMap.size();
    }

    /**
     * Add node rebalance info
     *
     * @param clientId
     * @param waitDuration
     * @return
     */
    public NodeRebInfo addNodeRelInfo(String clientId, int waitDuration) {
        NodeRebInfo nodeRebInfo = this.rebalanceMap.get(clientId);
        if (nodeRebInfo != null) {
            if (nodeRebInfo.getStatus() == 4) {
                this.rebalanceMap.remove(clientId);
                nodeRebInfo = null;
            } else {
                return nodeRebInfo;
            }
        }
        if (consumerInfoMap.containsKey(clientId)) {
            NodeRebInfo tmpNodeInfo = new NodeRebInfo(clientId, waitDuration);
            nodeRebInfo = this.rebalanceMap.putIfAbsent(clientId, tmpNodeInfo);
            if (nodeRebInfo == null) {
                nodeRebInfo = tmpNodeInfo;
            }
        }
        return nodeRebInfo;
    }

    public RebProcessInfo getNeedRebNodeList() {
        List<String> needProcessList = new ArrayList<String>();
        List<String> needEscapeList = new ArrayList<String>();
        List<String> needRemoved = new ArrayList<String>();
        for (NodeRebInfo nodeRebInfo : this.rebalanceMap.values()) {
            if (nodeRebInfo.getStatus() == 0) {
                nodeRebInfo.setStatus(1);
                needProcessList.add(nodeRebInfo.getClientId());
            } else {
                if (nodeRebInfo.getReqType() == 1
                        && nodeRebInfo.getStatus() == 2) {
                    if (nodeRebInfo.decrAndGetWaitDuration() <= 0) {
                        nodeRebInfo.setStatus(4);
                        needRemoved.add(nodeRebInfo.getClientId());
                    } else {
                        needEscapeList.add(nodeRebInfo.getClientId());
                    }
                }
            }
        }
        for (String clientId : needRemoved) {
            this.rebalanceMap.remove(clientId);
        }
        return new RebProcessInfo(needProcessList, needEscapeList);
    }

    public void setRebNodeProcessed(List<String> processList) {
        if (processList == null
                || processList.isEmpty()
                || this.rebalanceMap.isEmpty()) {
            return;
        }
        List<String> needRemoved = new ArrayList<String>();
        for (NodeRebInfo nodeRebInfo : this.rebalanceMap.values()) {
            if (processList.contains(nodeRebInfo.getClientId())) {
                if (nodeRebInfo.getReqType() == 0) {
                    nodeRebInfo.setStatus(4);
                    needRemoved.add(nodeRebInfo.getClientId());
                } else {
                    nodeRebInfo.setStatus(2);
                }
            }
        }
        for (String clientId : needRemoved) {
            this.rebalanceMap.remove(clientId);
        }
        return;
    }

    public boolean isBandConsume() {
        return isBandConsume;
    }

    public String getSessionKey() {
        return sessionKey;
    }

    public boolean isNotAllocate() {
        return notAllocate.get();
    }

    public int getAllocatedTimes() {
        return allocatedTimes.get();
    }

    public void addAllocatedTimes() {
        this.allocatedTimes.incrementAndGet();
    }

    public boolean isGroupFullSize() {
        return this.consumerInfoMap.size() >= this.sourceCount;
    }

    public boolean isRebalanceMapEmpty() {
        return (this.rebalanceMap == null
                || this.rebalanceMap.isEmpty());
    }

    public boolean isBandPartsEmpty() {
        return this.partitionInfoMap.isEmpty();
    }

    public void settAllocated() {
        this.notAllocate.compareAndSet(true, false);
    }

    public long getSessionTime() {
        return sessionTime;
    }

    public int getSourceCount() {
        return sourceCount;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getCurCheckCycle() {
        return curCheckCycle.get();
    }

    public long addCurCheckCycle() {
        return curCheckCycle.addAndGet(1);
    }

    /**
     * Set current consumer  broker/client ratio
     *
     * @param defBClientRate
     * @param confBClientRate
     * @param curBClientRate
     * @param minRequireClientCnt
     * @param isRebalanced
     */
    public void setCurrConsumeBClientInfo(int defBClientRate,
                                          int confBClientRate,
                                          int curBClientRate,
                                          int minRequireClientCnt,
                                          boolean isRebalanced) {
        this.defBClientRate = defBClientRate;
        this.confBClientRate = confBClientRate;
        this.curBClientRate = curBClientRate;
        this.minRequireClientCnt = minRequireClientCnt;
        this.rebalanCheckPrint = false;
        if (isRebalanced) {
            this.rebalanceCheckStatus = 1;
        } else {
            this.rebalanceCheckStatus = 0;
        }
    }

    public boolean isRebalanCheckPrint() {
        return rebalanCheckPrint;
    }

    public AtomicBoolean getNotAllocate() {
        return notAllocate;
    }

    public int getDefBClientRate() {
        return defBClientRate;
    }

    public int getConfBClientRate() {
        return confBClientRate;
    }

    public int getCurBClientRate() {
        return curBClientRate;
    }

    public int getMinRequireClientCnt() {
        return minRequireClientCnt;
    }

    public int getRebalanceCheckStatus() {
        return rebalanceCheckStatus;
    }

    public Set<String> getTopicSet() {
        return this.topicSet;
    }

    public boolean isSelectedBig() {
        return isSelectedBig;
    }

    public Map<String, TreeSet<String>> getTopicConditions() {
        return this.topicConditions;
    }

    public List<ConsumerInfo> getConsumerInfoList() {
        List<ConsumerInfo> result = new ArrayList<ConsumerInfo>();
        for (ConsumerInfo consumerInfo : this.consumerInfoMap.values()) {
            result.add(consumerInfo);
        }
        return result;
    }

    public ConsumerInfo getConsumerInfo(String consumerId) {
        return consumerInfoMap.get(consumerId);
    }

    public Map<String, String> getPartitionInfoMap() {
        return this.partitionInfoMap;
    }

    public Map<String, Long> getPartOffsetMap() {
        return this.partOffsetMap;
    }

    public Map<String, NodeRebInfo> getRebalanceMap() {
        return this.rebalanceMap;
    }

    public ConsumerBandInfo clone() {
        // no need to deep clone
        return new ConsumerBandInfo(this.isBandConsume, this.sessionKey,
                this.sessionTime, this.sourceCount,
                this.createTime, this.curCheckCycle.get(),
                this.notAllocate.get(), this.allocatedTimes.get(),
                this.isSelectedBig,
                this.topicSet, this.topicConditions,
                this.consumerInfoMap, this.partitionInfoMap,
                this.partOffsetMap, this.rebalanceMap,
                this.defBClientRate, this.confBClientRate,
                this.curBClientRate, this.minRequireClientCnt,
                this.rebalanceCheckStatus, this.rebalanCheckPrint);
    }

}

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

import com.tencent.tubemq.client.config.TubeClientConfig;
import com.tencent.tubemq.client.exception.TubeClientException;
import com.tencent.tubemq.corebase.TokenConstants;
import com.tencent.tubemq.corebase.cluster.BrokerInfo;
import com.tencent.tubemq.corebase.cluster.Partition;
import com.tencent.tubemq.corerpc.RpcServiceFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of BrokerRcvQltyStats.
 */
public class DefaultBrokerRcvQltyStats implements BrokerRcvQltyStats {
    private static final Logger logger =
            LoggerFactory.getLogger(DefaultBrokerRcvQltyStats.class);
    private final TubeClientConfig clientConfig;
    private final RpcServiceFactory rpcServiceFactory;
    private final Thread statisticThread;
    // Broker link quality statistics.
    // Request failure analysis, by broker.
    private final ConcurrentHashMap<Integer, BrokerStatsItemSet> brokerStatis =
            new ConcurrentHashMap<Integer, BrokerStatsItemSet>();
    // The request number of the current broker.
    private final ConcurrentHashMap<Integer, AtomicLong> brokerCurSentReqNum =
            new ConcurrentHashMap<Integer, AtomicLong>();
    // The statistics of the blocking brokers.
    private final ConcurrentHashMap<Integer, Long> brokerForbiddenMap =
            new ConcurrentHashMap<Integer, Long>();
    // Status:
    // -1: Uninitialized
    // 0: Running
    // 1:Stopped
    private AtomicInteger statusId = new AtomicInteger(-1);
    private long lastPrinttime = System.currentTimeMillis();
    // Total sent request number.
    private AtomicLong curTotalSentRequestNum = new AtomicLong(0);
    // The time of last link quality statistic.
    private long lastLinkStatisticTime = System.currentTimeMillis();
    // Analyze the broker quality based on the request response. We calculate the quality metric by
    //     success response number / total request number
    // in a time range. The time range is same when we compare the quality of different brokers.
    // We think the quality is better when the successful ratio is higher. The bad quality brokers
    // will be blocked. The blocking ratio and time can be configured.
    private List<Map.Entry<Integer, BrokerStatsDltTuple>> cachedLinkQualitys =
            new ArrayList<Map.Entry<Integer, BrokerStatsDltTuple>>();
    private long lastQualityStatisticTime = System.currentTimeMillis();
    private long printCount = 0;

    public DefaultBrokerRcvQltyStats(final RpcServiceFactory rpcServiceFactory,
                                     final TubeClientConfig producerConfig) {
        this.clientConfig = producerConfig;
        this.rpcServiceFactory = rpcServiceFactory;
        this.statisticThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!isStopped()) {
                    try {
                        statisticDltBrokerStatus();
                    } catch (Throwable e) {
                        //
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (Throwable e) {
                        //
                    }
                }
            }
        }, "Sent Statistic Thread");
        this.statisticThread.setPriority(Thread.MAX_PRIORITY);
    }

    /**
     * Start the broker statistic thread.
     */
    public void startBrokerStatistic() {
        if (this.statusId.compareAndSet(-1, 0)) {
            this.statisticThread.start();
        }
    }

    /**
     * Check if the statistic thread is stopped.
     *
     * @return status
     */
    public boolean isStopped() {
        return (this.statusId.get() > 0);
    }

    /**
     * Get the partitions of allowed broker.
     *
     * @param brokerPartList broker partition mapping
     * @return partition list
     * @throws TubeClientException
     */
    @Override
    public List<Partition> getAllowedBrokerPartitions(
            Map<Integer, List<Partition>> brokerPartList) throws TubeClientException {
        // #lizard forgives
        List<Partition> partList = new ArrayList<Partition>();
        if ((brokerPartList == null) || (brokerPartList.isEmpty())) {
            throw new TubeClientException("Null brokers to select sent, please try later!");
        }
        long currentWaitCount = this.curTotalSentRequestNum.get();
        if (currentWaitCount >= this.clientConfig.getSessionMaxAllowedDelayedMsgCount()) {
            throw new TubeClientException(new StringBuilder(512)
                    .append("Current delayed messages over max allowed count, allowed is ")
                    .append(this.clientConfig.getSessionMaxAllowedDelayedMsgCount())
                    .append(", current count is ").append(currentWaitCount).toString());
        }
        Set<Integer> allowedBrokerIds = new HashSet<Integer>();
        for (Map.Entry<Integer, List<Partition>> oldBrokerPartEntry : brokerPartList.entrySet()) {
            List<Partition> partitionList = oldBrokerPartEntry.getValue();
            if ((partitionList != null) && !partitionList.isEmpty()) {
                Partition partition = partitionList.get(0);
                if (partition != null) {
                    BrokerInfo brokerInfo = partition.getBroker();
                    AtomicLong curMaxSentNum =
                            this.brokerCurSentReqNum.get(brokerInfo.getBrokerId());
                    if ((curMaxSentNum != null)
                            && (curMaxSentNum.get() > this.clientConfig.getLinkMaxAllowedDelayedMsgCount())) {
                        continue;
                    }
                    if (!rpcServiceFactory.isRemoteAddrForbidden(brokerInfo.getBrokerAddr())
                            && (!this.brokerForbiddenMap.containsKey(brokerInfo.getBrokerId()))) {
                        allowedBrokerIds.add(brokerInfo.getBrokerId());
                    }
                }
            }
        }
        if (allowedBrokerIds.isEmpty()) {
            throw new TubeClientException("The brokers of topic are all forbidden!");
        }
        int selectCount = allowedBrokerIds.size();
        int allowedCount = selectCount;
        if (currentWaitCount > this.clientConfig.getSessionWarnDelayedMsgCount()) {
            allowedCount =
                    (int) Math.rint(selectCount * (1 - this.clientConfig.getSessionWarnForbiddenRate()));
        }
        if ((this.cachedLinkQualitys.isEmpty()) || (selectCount == allowedCount)) {
            for (Integer selBrokerId : allowedBrokerIds) {
                partList.addAll(brokerPartList.get(selBrokerId));
            }
        } else {
            List<Integer> cachedBrokerIds = new ArrayList<Integer>();
            for (Map.Entry<Integer, BrokerStatsDltTuple> brokerEntry : this.cachedLinkQualitys) {
                cachedBrokerIds.add(brokerEntry.getKey());
            }
            for (Integer selBrokerId : allowedBrokerIds) {
                if (!cachedBrokerIds.contains(selBrokerId)) {
                    partList.addAll(brokerPartList.get(selBrokerId));
                    allowedCount--;
                }
                if (allowedCount <= 0) {
                    break;
                }
            }
            if (allowedCount > 0) {
                for (Map.Entry<Integer, BrokerStatsDltTuple> brokerEntry :
                        this.cachedLinkQualitys) {
                    if (allowedBrokerIds.contains(brokerEntry.getKey())) {
                        partList.addAll(brokerPartList.get(brokerEntry.getKey()));
                        allowedCount--;
                    }
                    if (allowedCount <= 0) {
                        break;
                    }
                }
            }
        }
        return partList;
    }

    /**
     * Remove a registered broker from the statistic list.
     *
     * @param registeredBrokerIdList
     */
    @Override
    public void removeUnRegisteredBroker(List<Integer> registeredBrokerIdList) {
        for (Integer curBrokerId : brokerStatis.keySet()) {
            if (!registeredBrokerIdList.contains(curBrokerId)) {
                brokerStatis.remove(curBrokerId);
            }
        }
    }

    @Override
    public void statisticDltBrokerStatus() {
        // #lizard forgives
        long currentTime = System.currentTimeMillis();
        if ((currentTime - this.lastLinkStatisticTime
                < this.clientConfig.getSessionStatisticCheckDuration())
                && (currentTime - this.lastQualityStatisticTime
                < this.clientConfig.getMaxForbiddenCheckDuration())) {
            return;
        }
        if (currentTime - this.lastLinkStatisticTime
                > this.clientConfig.getSessionStatisticCheckDuration()) {
            this.lastLinkStatisticTime = System.currentTimeMillis();
            this.cachedLinkQualitys = getCurBrokerSentWaitStatis();
        }
        if (System.currentTimeMillis() - this.lastQualityStatisticTime
                < this.clientConfig.getMaxForbiddenCheckDuration()) {
            return;
        }
        StringBuilder sBuilder = new StringBuilder(512);
        this.lastQualityStatisticTime = System.currentTimeMillis();
        if ((printCount++ % 10 == 0) && (!brokerStatis.isEmpty())) {
            if (!brokerForbiddenMap.isEmpty()) {
                logger.info(sBuilder.append("[status check]: current response quality respForbiddenMap is ")
                        .append(brokerForbiddenMap.toString()).toString());
                sBuilder.delete(0, sBuilder.length());
            }
            if (!rpcServiceFactory.getForbiddenAddrMap().isEmpty()) {
                logger.info(sBuilder.append("[status check]: current request quality reqForbiddenMap is ")
                        .append(rpcServiceFactory.getForbiddenAddrMap().toString()).toString());
                sBuilder.delete(0, sBuilder.length());
            }
        }

        boolean changed = false;
        long totalSuccRecNum = 0L;
        HashMap<Integer, BrokerStatsDltTuple> needSelNumTMap =
                new HashMap<Integer, BrokerStatsDltTuple>();
        for (Map.Entry<Integer, BrokerStatsItemSet> brokerForbiddenEntry : brokerStatis.entrySet()) {
            BrokerStatsItemSet curStatisItemSet = brokerStatis.get(brokerForbiddenEntry.getKey());
            if (curStatisItemSet != null) {
                long sendNum = curStatisItemSet.getDltAndSnapshotSendNum();
                long succRecvNum = curStatisItemSet.getDltAndSnapshotRecSucNum();
                if (!brokerForbiddenMap.containsKey(brokerForbiddenEntry.getKey())) {
                    totalSuccRecNum += succRecvNum;
                    needSelNumTMap.put(brokerForbiddenEntry.getKey(),
                            new BrokerStatsDltTuple(succRecvNum, sendNum));
                }
            }
        }
        for (Map.Entry<Integer, Long> brokerForbiddenEntry : brokerForbiddenMap.entrySet()) {
            if (System.currentTimeMillis() - brokerForbiddenEntry.getValue()
                    > this.clientConfig.getMaxForbiddenCheckDuration()) {
                changed = true;
                brokerForbiddenMap.remove(brokerForbiddenEntry.getKey());
            }
        }
        if (needSelNumTMap.isEmpty()) {
            if (changed) {
                if (!brokerForbiddenMap.isEmpty()) {
                    logger.info(sBuilder.append("End statistic 1: forbidden Broker Set is ")
                            .append(brokerForbiddenMap.toString()).toString());
                    sBuilder.delete(0, sBuilder.length());
                }
            }
            return;
        }
        List<Map.Entry<Integer, BrokerStatsDltTuple>> lstData =
                new ArrayList<Map.Entry<Integer, BrokerStatsDltTuple>>(needSelNumTMap.entrySet());
        // Sort the list in ascending order
        Collections.sort(lstData, new BrokerStatsDltTupleComparator(false));
        int filteredBrokerListSize = lstData.size();
        int needHoldCout =
                (int) Math.rint((filteredBrokerListSize + brokerForbiddenMap.size())
                        * clientConfig.getMaxSentForbiddenRate());
        needHoldCout -= brokerForbiddenMap.size();
        if (needHoldCout <= 0) {
            if (changed) {
                if (!brokerForbiddenMap.isEmpty()) {
                    logger.info(sBuilder.append("End statistic 2: forbidden Broker Set is ")
                            .append(brokerForbiddenMap.toString()).toString());
                    sBuilder.delete(0, sBuilder.length());
                }
            }
            return;
        }
        long avgSuccRecNumThreshold = 0L;
        if (filteredBrokerListSize <= 3) {
            totalSuccRecNum -= lstData.get(0).getValue().getSuccRecvNum();
            avgSuccRecNumThreshold = (long) (totalSuccRecNum / (filteredBrokerListSize - 1) * 0.2);
        } else {
            totalSuccRecNum -= lstData.get(0).getValue().getSuccRecvNum();
            totalSuccRecNum -= lstData.get(1).getValue().getSuccRecvNum();
            totalSuccRecNum -= lstData.get(lstData.size() - 1).getValue().getSuccRecvNum();
            avgSuccRecNumThreshold = (long) (totalSuccRecNum / (filteredBrokerListSize - 3) * 0.2);
        }
        ConcurrentHashMap<Integer, Boolean> tmpBrokerForbiddenMap =
                new ConcurrentHashMap<Integer, Boolean>();
        for (Map.Entry<Integer, BrokerStatsDltTuple> brokerDltNumEntry : lstData) {
            long succRecvNum = brokerDltNumEntry.getValue().getSuccRecvNum();
            long succSendNumThreshold = (long) (brokerDltNumEntry.getValue().getSendNum() * 0.1);
            if ((succRecvNum < avgSuccRecNumThreshold) && (succSendNumThreshold > 2)
                    && (succRecvNum < succSendNumThreshold)) {
                tmpBrokerForbiddenMap.put(brokerDltNumEntry.getKey(), true);
                logger.debug(sBuilder.append("[forbidden statistic] brokerId=")
                        .append(brokerDltNumEntry.getKey()).append(",succRecvNum=")
                        .append(succRecvNum).append(",avgSuccRecNumThreshold=")
                        .append(avgSuccRecNumThreshold).append(",succSendNumThreshold=")
                        .append(succSendNumThreshold).toString());
                sBuilder.delete(0, sBuilder.length());
            }
            if ((tmpBrokerForbiddenMap.size() >= needHoldCout)
                    || (succRecvNum >= avgSuccRecNumThreshold)) {
                break;
            }
        }
        for (Integer tmpBrokerId : tmpBrokerForbiddenMap.keySet()) {
            changed = true;
            brokerForbiddenMap.put(tmpBrokerId, System.currentTimeMillis());
        }
        if (changed) {
            if (!brokerForbiddenMap.isEmpty()) {
                logger.info(sBuilder.append("End statistic 3: forbidden Broker Set is ")
                        .append(brokerForbiddenMap.toString()).toString());
                sBuilder.delete(0, sBuilder.length());
            }
        }
    }

    private List<Map.Entry<Integer, BrokerStatsDltTuple>> getCurBrokerSentWaitStatis() {
        HashMap<Integer, BrokerStatsDltTuple> needSelNumTMap = new HashMap<Integer, BrokerStatsDltTuple>();
        for (Map.Entry<Integer, BrokerStatsItemSet> brokerForbiddenEntry : brokerStatis.entrySet()) {
            BrokerStatsItemSet curStatisItemSet = brokerForbiddenEntry.getValue();
            long num = curStatisItemSet.getSendNum() - curStatisItemSet.getReceiveNum();
            if (num < this.clientConfig.getLinkMaxAllowedDelayedMsgCount()) {
                needSelNumTMap.put(brokerForbiddenEntry.getKey(), new BrokerStatsDltTuple(num,
                        curStatisItemSet.getSendNum()));
            }
        }
        List<Map.Entry<Integer, BrokerStatsDltTuple>> lstData =
                new ArrayList<Map.Entry<Integer, BrokerStatsDltTuple>>(needSelNumTMap.entrySet());
        // Sort the list in descending order
        Collections.sort(lstData, new BrokerStatsDltTupleComparator(true));
        return lstData;
    }

    @Override
    public void addSendStatistic(int brokerId) {
        BrokerStatsItemSet curStatisItemSet = brokerStatis.get(brokerId);
        if (curStatisItemSet == null) {
            BrokerStatsItemSet newStatisItemSet = new BrokerStatsItemSet();
            curStatisItemSet = brokerStatis.putIfAbsent(brokerId, newStatisItemSet);
            if (curStatisItemSet == null) {
                curStatisItemSet = newStatisItemSet;
            }
        }
        curStatisItemSet.incrementAndGetSendNum();
        AtomicLong curBrokerNum = brokerCurSentReqNum.get(brokerId);
        if (curBrokerNum == null) {
            AtomicLong tmpCurBrokerNum = new AtomicLong(0);
            curBrokerNum = brokerCurSentReqNum.putIfAbsent(brokerId, tmpCurBrokerNum);
            if (curBrokerNum == null) {
                curBrokerNum = tmpCurBrokerNum;
            }
        }
        curBrokerNum.incrementAndGet();
        this.curTotalSentRequestNum.incrementAndGet();
    }

    @Override
    public void addReceiveStatistic(int brokerId, boolean isSuccess) {
        BrokerStatsItemSet curStatisItemSet = brokerStatis.get(brokerId);
        if (curStatisItemSet != null) {
            curStatisItemSet.incrementAndGetRecNum();
            if (isSuccess) {
                curStatisItemSet.incrementAndGetRecSucNum();
            }
            AtomicLong curBrokerNum = brokerCurSentReqNum.get(brokerId);
            if (curBrokerNum != null) {
                curBrokerNum.decrementAndGet();
            }
            this.curTotalSentRequestNum.decrementAndGet();
        }
    }

    @Override
    public void stopBrokerStatistic() {
        if (this.statusId.get() != 0) {
            return;
        }
        if (this.statusId.compareAndSet(0, 1)) {
            try {
                this.statisticThread.interrupt();
            } catch (Throwable e) {
                //
            }
        }
    }

    public String toString() {
        return "lastStatisticTime:" + this.lastLinkStatisticTime + TokenConstants.ATTR_SEP
                + ",lastPrinttime:" + this.lastPrinttime + TokenConstants.ATTR_SEP
                + ",producerMaxSentStatisScanDuration:" + this.clientConfig.getMaxForbiddenCheckDuration()
                + TokenConstants.ATTR_SEP + ",linkMaxAllowedDelayedMsgCount:"
                + this.clientConfig.getLinkMaxAllowedDelayedMsgCount() + TokenConstants.ATTR_SEP
                + ",brokerStatis:" + this.brokerStatis.toString() + TokenConstants.ATTR_SEP
                + ",brokerForbiddenMap:" + this.brokerForbiddenMap.toString();
    }

    private static class BrokerStatsDltTupleComparator
            implements Comparator<Map.Entry<Integer, BrokerStatsDltTuple>> {
        // Descending sort or ascending sort
        private boolean isDescSort = true;

        public BrokerStatsDltTupleComparator(boolean isDescSort) {
            this.isDescSort = isDescSort;
        }

        @Override
        public int compare(final Map.Entry<Integer, BrokerStatsDltTuple> o1,
                           final Map.Entry<Integer, BrokerStatsDltTuple> o2) {
            if (o1.getValue().getSuccRecvNum() == o2.getValue().getSuccRecvNum()) {
                return 0;
            } else {
                if (o1.getValue().getSuccRecvNum() > o2.getValue().getSuccRecvNum()) {
                    return this.isDescSort ? -1 : 1;
                } else {
                    return this.isDescSort ? 1 : -1;
                }
            }
        }
    }
}

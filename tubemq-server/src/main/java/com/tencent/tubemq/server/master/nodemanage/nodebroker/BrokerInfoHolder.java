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

package com.tencent.tubemq.server.master.nodemanage.nodebroker;

import com.tencent.tubemq.corebase.cluster.BrokerInfo;
import com.tencent.tubemq.corebase.protobuf.generated.ClientMaster;
import com.tencent.tubemq.server.common.TStatusConstants;
import com.tencent.tubemq.server.common.utils.WebParameterUtils;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbBrokerConfEntity;
import com.tencent.tubemq.server.master.web.handler.WebBrokerDefConfHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BrokerInfoHolder {
    private static final Logger logger =
            LoggerFactory.getLogger(BrokerInfoHolder.class);
    private final ConcurrentHashMap<Integer/* brokerId */, BrokerInfo> brokerInfoMap =
            new ConcurrentHashMap<Integer, BrokerInfo>();
    private final ConcurrentHashMap<Integer/* brokerId */, BrokerForbInfo> brokerForbiddenMap =
            new ConcurrentHashMap<Integer, BrokerForbInfo>();
    private final int maxAutoForbiddenCnt;
    private final BrokerConfManage brokerConfManage;
    private AtomicInteger brokerTotalCount = new AtomicInteger(0);
    private AtomicInteger brokerForbiddenCount = new AtomicInteger(0);


    public BrokerInfoHolder(final int maxAutoForbiddenCnt,
                            final BrokerConfManage brokerConfManage) {
        this.maxAutoForbiddenCnt = maxAutoForbiddenCnt;
        this.brokerConfManage = brokerConfManage;
    }

    public BrokerInfo getBrokerInfo(int brokerId) {
        return brokerInfoMap.get(brokerId);
    }

    public void setBrokerInfo(int brokerId, BrokerInfo brokerInfo) {
        if (brokerInfoMap.put(brokerId, brokerInfo) == null) {
            this.brokerTotalCount.incrementAndGet();
        }
    }

    public void updateBrokerReportStatus(int brokerId,
                                         int reportReadStatus,
                                         int reportWriteStatus) {
        if (reportReadStatus < 0 || reportWriteStatus < 0) {
            if (brokerForbiddenMap.get(brokerId) == null) {
                if (brokerForbiddenCount.incrementAndGet() > maxAutoForbiddenCnt) {
                    brokerForbiddenCount.decrementAndGet();
                } else {
                    int manageStatus =
                            getManageStatus(reportWriteStatus >= 0, reportReadStatus >= 0);
                    if (updateCurManageStatus(brokerId, manageStatus)) {
                        logger.warn(new StringBuilder(512)
                                .append("[Broker Status] master auto-change current broker ")
                                .append(brokerId).append("'s manage status to")
                                .append(manageStatus).toString());
                    } else {
                        brokerForbiddenCount.decrementAndGet();
                    }
                }
            }
        }
    }

    public void setBrokerHeartBeatReqStatus(int brokerId,
                                            ClientMaster.HeartResponseM2B.Builder builder) {
        BrokerForbInfo brokerForbInfo = brokerForbiddenMap.get(brokerId);
        if (brokerForbInfo == null) {
            builder.setStopWrite(false);
            builder.setStopRead(false);
        } else {
            switch (brokerForbInfo.newStatus) {
                case TStatusConstants.STATUS_MANAGE_ONLINE_NOT_READ: {
                    builder.setStopWrite(false);
                    builder.setStopRead(true);
                }
                break;
                case TStatusConstants.STATUS_MANAGE_ONLINE_NOT_WRITE: {
                    builder.setStopWrite(true);
                    builder.setStopRead(false);
                }
                break;
                case TStatusConstants.STATUS_MANAGE_OFFLINE: {
                    builder.setStopWrite(true);
                    builder.setStopRead(true);
                }
                break;
                case TStatusConstants.STATUS_MANAGE_ONLINE:
                default: {
                    builder.setStopWrite(false);
                    builder.setStopRead(false);
                }
            }
        }
    }

    public Map<Integer, BrokerInfo> getBrokerInfos(Collection<Integer> brokerIds) {
        HashMap<Integer, BrokerInfo> brokerMap = new HashMap<Integer, BrokerInfo>();
        for (Integer brokerId : brokerIds) {
            brokerMap.put(brokerId, brokerInfoMap.get(brokerId));
        }
        return brokerMap;
    }

    /**
     * Remove broker info and decrease total broker count and forbidden broker count
     *
     * @param brokerId
     * @return the deleted broker info
     */
    public BrokerInfo removeBroker(Integer brokerId) {
        BrokerInfo brokerInfo = brokerInfoMap.remove(brokerId);
        BrokerForbInfo brokerForbInfo = brokerForbiddenMap.remove(brokerId);
        if (brokerInfo != null) {
            this.brokerTotalCount.decrementAndGet();
        }
        if (brokerForbInfo != null) {
            this.brokerForbiddenCount.decrementAndGet();
        }
        return brokerInfo;
    }

    public Map<Integer, BrokerInfo> getBrokerInfoMap() {
        return brokerInfoMap;
    }

    public void clear() {
        brokerInfoMap.clear();
        brokerForbiddenCount.set(0);
        brokerForbiddenMap.clear();

    }

    public int getCurrentBrokerCount() {
        return this.brokerForbiddenCount.get();
    }

    /**
     * Deduce status according to publish status and subscribe status
     *
     * @param inAcceptPublish
     * @param inAcceptSubscribe
     * @return
     */
    private int getManageStatus(boolean inAcceptPublish, boolean inAcceptSubscribe) {
        int manageStatus = TStatusConstants.STATUS_MANAGE_ONLINE_NOT_READ;
        if ((inAcceptPublish) && (inAcceptSubscribe)) {
            manageStatus = TStatusConstants.STATUS_MANAGE_ONLINE;
        } else if (!inAcceptPublish && !inAcceptSubscribe) {
            manageStatus = TStatusConstants.STATUS_MANAGE_OFFLINE;
        } else if (inAcceptSubscribe) {
            manageStatus = TStatusConstants.STATUS_MANAGE_ONLINE_NOT_WRITE;
        }
        return manageStatus;
    }

    /**
     * Update broker status, if broker is not online, this operator will fail
     *
     * @param brokerId
     * @param manageStatus
     * @return true if success otherwise false
     */
    private boolean updateCurManageStatus(int brokerId, int manageStatus) {
        BdbBrokerConfEntity oldEntity =
                brokerConfManage.getBrokerDefaultConfigStoreInfo(brokerId);
        if (oldEntity == null) {
            return false;
        }
        if ((oldEntity.getManageStatus() == manageStatus)
                || ((manageStatus == TStatusConstants.STATUS_MANAGE_OFFLINE)
                && (oldEntity.getManageStatus() < TStatusConstants.STATUS_MANAGE_ONLINE))) {
            return false;
        }
        try {
            if (WebParameterUtils.checkBrokerInProcessing(oldEntity.getBrokerId(),
                    brokerConfManage, null)) {
                return false;
            }
            BdbBrokerConfEntity newEntity =
                    new BdbBrokerConfEntity(oldEntity.getBrokerId(),
                            oldEntity.getBrokerIp(), oldEntity.getBrokerPort(),
                            oldEntity.getDftNumPartitions(), oldEntity.getDftUnflushThreshold(),
                            oldEntity.getDftUnflushInterval(), oldEntity.getDftDeleteWhen(),
                            oldEntity.getDftDeletePolicy(), manageStatus,
                            oldEntity.isAcceptPublish(), oldEntity.isAcceptSubscribe(),
                            oldEntity.getAttributes(), oldEntity.isConfDataUpdated(),
                            oldEntity.isBrokerLoaded(), oldEntity.getRecordCreateUser(),
                            oldEntity.getRecordCreateDate(), "Broker AutoReport",
                            new Date());
            boolean isNeedFastStart =
                    WebBrokerDefConfHandler.isBrokerStartNeedFast(brokerConfManage,
                            newEntity.getBrokerId(), oldEntity.getManageStatus(), newEntity.getManageStatus());
            brokerConfManage.confModBrokerDefaultConfig(newEntity);
            brokerConfManage.triggerBrokerConfDataSync(newEntity,
                    oldEntity.getManageStatus(), isNeedFastStart);
            if (brokerForbiddenMap.putIfAbsent(brokerId,
                    new BrokerForbInfo(brokerId,
                            oldEntity.getManageStatus(), manageStatus,
                            System.currentTimeMillis())) != null) {
                return false;
            }
            return true;
        } catch (Throwable e1) {
            return false;
        }
    }

    public BrokerForbInfo getAutoForbiddenBrokerInfo(int brokerId) {
        return this.brokerForbiddenMap.get(brokerId);
    }

    public Map<Integer, BrokerForbInfo> getAutoForbiddenBrokerMapInfo() {
        return this.brokerForbiddenMap;
    }

    /**
     * Release forbidden broker info
     *
     * @param brokerIdSet
     * @param reason
     */
    public void relAutoForbiddenBrokerInfo(Set<Integer> brokerIdSet, String reason) {
        if (brokerIdSet == null || brokerIdSet.isEmpty()) {
            return;
        }
        List<BrokerForbInfo> brokerForbInfos = new ArrayList<BrokerForbInfo>();
        for (Integer brokerId : brokerIdSet) {
            BrokerForbInfo forbInfo = this.brokerForbiddenMap.remove(brokerId);
            if (forbInfo != null) {
                brokerForbInfos.add(forbInfo);
            }
        }
        if (!brokerForbInfos.isEmpty()) {
            logger.info(new StringBuilder(512)
                    .append("[Remove AutoForbidden] : remove current auto-forbidden brokers by reason ")
                    .append(reason).append(", release list is ").append(brokerForbInfos.toString()).toString());
        }
    }

    public class BrokerForbInfo {
        public int brokerId;
        public int befStatus;
        public int newStatus;
        public long forbiddenTime;

        BrokerForbInfo(int brokerId, int befStatus, int newStatus, long forbiddenTime) {
            this.brokerId = brokerId;
            this.befStatus = befStatus;
            this.forbiddenTime = forbiddenTime;

        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("brokerId", brokerId)
                    .append("befStatus", befStatus)
                    .append("newStatus", newStatus)
                    .append("forbiddenTime", forbiddenTime)
                    .toString();
        }
    }

}

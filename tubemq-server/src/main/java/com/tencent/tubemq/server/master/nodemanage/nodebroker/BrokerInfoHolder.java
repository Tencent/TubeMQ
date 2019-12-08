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

import static com.tencent.tubemq.server.common.utils.WebParameterUtils.getBrokerManageStatusStr;
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
    private final ConcurrentHashMap<Integer/* brokerId */, BrokerAbnInfo> brokerAbnormalMap =
            new ConcurrentHashMap<Integer, BrokerAbnInfo>();
    private final ConcurrentHashMap<Integer/* brokerId */, BrokerFbdInfo> brokerForbiddenMap =
            new ConcurrentHashMap<Integer, BrokerFbdInfo>();
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
        if (reportReadStatus == 0 && reportWriteStatus == 0) {
            BrokerAbnInfo brokerAbnInfo = brokerAbnormalMap.get(brokerId);
            if (brokerAbnInfo != null) {
                if (brokerForbiddenMap.get(brokerId) == null) {
                    brokerAbnInfo = brokerAbnormalMap.remove(brokerId);
                    if (brokerAbnInfo != null) {
                        logger.warn(new StringBuilder(512)
                            .append("[Broker AutoForbidden] broker ")
                            .append(brokerId).append(" return to normal!").toString());
                    }
                }
            }
            return;
        }
        BdbBrokerConfEntity oldEntity =
            brokerConfManage.getBrokerDefaultConfigStoreInfo(brokerId);
        if (oldEntity == null) {
            return;
        }
        int manageStatus = getManageStatus(reportWriteStatus, reportReadStatus);
        if ((oldEntity.getManageStatus() == manageStatus)
            || ((manageStatus == TStatusConstants.STATUS_MANAGE_OFFLINE)
            && (oldEntity.getManageStatus() < TStatusConstants.STATUS_MANAGE_ONLINE))) {
            return;
        }
        BrokerAbnInfo brokerAbnInfo = brokerAbnormalMap.get(brokerId);
        if (brokerAbnInfo == null) {
            if (brokerAbnormalMap.putIfAbsent(brokerId,
                new BrokerAbnInfo(brokerId, reportReadStatus, reportWriteStatus)) == null) {
                logger.warn(new StringBuilder(512)
                    .append("[Broker AutoForbidden] broker report abnormal, ")
                    .append(brokerId).append("'s reportReadStatus=")
                    .append(reportReadStatus).append(", reportWriteStatus=")
                    .append(reportWriteStatus).toString());
            }
        } else {
            brokerAbnInfo.updateLastRepStatus(reportReadStatus, reportWriteStatus);
        }
        BrokerFbdInfo brokerFbdInfo = brokerForbiddenMap.get(brokerId);
        if (brokerFbdInfo == null) {
            BrokerFbdInfo tmpBrokerFbdInfo =
                new BrokerFbdInfo(brokerId, oldEntity.getManageStatus(),
                    manageStatus, System.currentTimeMillis());
            if (reportReadStatus > 0 || reportWriteStatus > 0) {
                if (updateCurManageStatus(brokerId, oldEntity, manageStatus)) {
                    if (brokerForbiddenMap.putIfAbsent(brokerId, tmpBrokerFbdInfo) == null) {
                        brokerForbiddenCount.incrementAndGet();
                        logger.warn(new StringBuilder(512)
                            .append("[Broker AutoForbidden] master add missing forbidden broker, ")
                            .append(brokerId).append("'s manage status to ")
                            .append(getBrokerManageStatusStr(manageStatus)).toString());
                    }
                }
            } else {
                if (brokerForbiddenCount.incrementAndGet() > maxAutoForbiddenCnt) {
                    brokerForbiddenCount.decrementAndGet();
                    return;
                }
                if (updateCurManageStatus(brokerId, oldEntity, manageStatus)) {
                    if (brokerForbiddenMap.putIfAbsent(brokerId, tmpBrokerFbdInfo) != null) {
                        brokerForbiddenCount.decrementAndGet();
                        return;
                    }
                    logger.warn(new StringBuilder(512)
                        .append("[Broker AutoForbidden] master auto forbidden broker, ")
                        .append(brokerId).append("'s manage status to ")
                        .append(getBrokerManageStatusStr(manageStatus)).toString());
                } else {
                    brokerForbiddenCount.decrementAndGet();
                }
            }
        } else {
            if (updateCurManageStatus(brokerId, oldEntity, manageStatus)) {
                brokerFbdInfo.updateInfo(oldEntity.getManageStatus(), manageStatus);
            }
        }
    }

    public void setBrokerHeartBeatReqStatus(int brokerId,
                                            ClientMaster.HeartResponseM2B.Builder builder) {
        BrokerFbdInfo brokerFbdInfo = brokerForbiddenMap.get(brokerId);
        if (brokerFbdInfo == null) {
            builder.setStopWrite(false);
            builder.setStopRead(false);
        } else {
            switch (brokerFbdInfo.newStatus) {
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
        brokerAbnormalMap.remove(brokerId);
        BrokerFbdInfo brokerFbdInfo = brokerForbiddenMap.remove(brokerId);
        if (brokerInfo != null) {
            this.brokerTotalCount.decrementAndGet();
        }
        if (brokerFbdInfo != null) {
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
        brokerAbnormalMap.clear();
        brokerForbiddenMap.clear();

    }

    public int getCurrentBrokerCount() {
        return this.brokerForbiddenCount.get();
    }

    /**
     * Deduce status according to publish status and subscribe status
     *
     * @param repWriteStatus
     * @param repReadStatus
     * @return
     */
    private int getManageStatus(int repWriteStatus, int repReadStatus) {
        int manageStatus = TStatusConstants.STATUS_MANAGE_ONLINE_NOT_READ;
        if (repWriteStatus == 0 && repReadStatus == 0) {
            manageStatus = TStatusConstants.STATUS_MANAGE_ONLINE;
        } else if (repWriteStatus != 0 && repReadStatus != 0) {
            manageStatus = TStatusConstants.STATUS_MANAGE_OFFLINE;
        } else if (repWriteStatus != 0) {
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
    private boolean updateCurManageStatus(int brokerId, BdbBrokerConfEntity oldEntity, int manageStatus) {
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
            return true;
        } catch (Throwable e1) {
            return false;
        }
    }

    public Map<Integer, BrokerAbnInfo> getBrokerAbnormalMap() {
        return brokerAbnormalMap;
    }

    public BrokerFbdInfo getAutoForbiddenBrokerInfo(int brokerId) {
        return this.brokerForbiddenMap.get(brokerId);
    }

    public Map<Integer, BrokerFbdInfo> getAutoForbiddenBrokerMapInfo() {
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
        List<BrokerFbdInfo> brokerFbdInfos = new ArrayList<BrokerFbdInfo>();
        for (Integer brokerId : brokerIdSet) {
            BrokerFbdInfo fbdInfo = this.brokerForbiddenMap.remove(brokerId);
            if (fbdInfo != null) {
                brokerFbdInfos.add(fbdInfo);
                this.brokerAbnormalMap.remove(brokerId);
                this.brokerForbiddenCount.decrementAndGet();
            }
        }
        if (!brokerFbdInfos.isEmpty()) {
            logger.info(new StringBuilder(512)
                    .append("[Broker AutoForbidden] remove forbidden brokers by reason ")
                    .append(reason).append(", release list is ").append(brokerFbdInfos.toString()).toString());
        }
    }

    public class BrokerAbnInfo {
        private int brokerId;
        private int abnStatus;  // 0 normal , -100 read abnormal, -1 write abnormal, -101 r & w abnormal
        private long firstRepTime;
        private long lastRepTime;

        public BrokerAbnInfo(int brokerId, int reportReadStatus, int reportWriteStatus) {
            this.brokerId = brokerId;
            this.abnStatus = reportReadStatus * 100 + reportWriteStatus;
            this.firstRepTime = System.currentTimeMillis();
            this.lastRepTime = this.firstRepTime;
        }

        public void updateLastRepStatus(int reportReadStatus, int reportWriteStatus) {
            this.abnStatus = reportReadStatus * 100 + reportWriteStatus;
            this.lastRepTime = System.currentTimeMillis();
        }

        public int getAbnStatus() {
            return abnStatus;
        }

        public long getFirstRepTime() {
            return firstRepTime;
        }

        public long getLastRepTime() {
            return lastRepTime;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("brokerId", brokerId)
                    .append("abnStatus", abnStatus)
                    .append("firstRepTime", firstRepTime)
                    .append("lastRepTime", lastRepTime)
                    .toString();
        }
    }

    public class BrokerFbdInfo {
        private int brokerId;
        private int befStatus;
        private int newStatus;
        private long forbiddenTime;
        private long lastUpdateTime;

        public BrokerFbdInfo(int brokerId, int befStatus, int newStatus, long forbiddenTime) {
            this.brokerId = brokerId;
            this.befStatus = befStatus;
            this.newStatus = newStatus;
            this.forbiddenTime = forbiddenTime;
            this.lastUpdateTime = forbiddenTime;
        }

        public void updateInfo(int befStatus, int newStatus) {
            this.befStatus = befStatus;
            this.newStatus = newStatus;
            this.lastUpdateTime = System.currentTimeMillis();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                .append("brokerId", brokerId)
                .append("befStatus", befStatus)
                .append("newStatus", newStatus)
                .append("forbiddenTime", forbiddenTime)
                .append("lastUpdateTime", lastUpdateTime)
                .toString();
        }
    }

}

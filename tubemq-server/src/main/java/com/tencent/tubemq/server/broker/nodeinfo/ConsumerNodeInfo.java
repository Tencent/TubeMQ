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

package com.tencent.tubemq.server.broker.nodeinfo;

import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.policies.FlowCtrlResult;
import com.tencent.tubemq.corebase.policies.FlowCtrlRuleHandler;
import com.tencent.tubemq.corebase.policies.SSDCtrlResult;
import com.tencent.tubemq.server.broker.msgstore.MessageStoreManager;
import com.tencent.tubemq.server.common.TServerConstants;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Consumer node info, which broker contains.
 */
public class ConsumerNodeInfo {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerNodeInfo.class);
    // partition string format
    private final String partStr;
    private final MessageStoreManager storeManager;
    // consumer id
    private String consumerId;
    private String sessionKey;
    private long sessionTime;
    // is filter consumer or not
    private boolean isFilterConsume = false;
    // filter conditions in string format
    private Set<String> filterCondStrs = new HashSet<String>(10);
    // filter conditions in int format
    private Set<Integer> filterCondCode = new HashSet<Integer>(10);
    // consumer's address
    private String rmtAddrInfo;
    private boolean isSupportLimit = false;
    private long nextStatTime = 0L;
    private long lastGetTime = 0L;
    private long lastDataRdOffset = TBaseConstants.META_VALUE_UNDEFINED;
    private int sentMsgSize = 0;
    private int sentUnit = TServerConstants.CFG_STORE_DEFAULT_MSG_READ_UNIT;
    private long totalUnitSec = 0L;
    private long totalUnitMin = 0L;
    private FlowCtrlResult curFlowCtrlVal =
            new FlowCtrlResult(Long.MAX_VALUE, 0);
    private SSDCtrlResult curSsdDltLimit =
            new SSDCtrlResult(Long.MAX_VALUE, 0);
    private long nextLimitUpdateTime = 0;
    private AtomicBoolean needSsdProc =
            new AtomicBoolean(false);
    private AtomicLong ssdTransId =
            new AtomicLong(TBaseConstants.META_VALUE_UNDEFINED);
    // -2 : 未启用 0:已发起请求 1://已接收请求正在处理 2:已处理完可用 3:已处理不可用 4:已停止
    private AtomicInteger ssdProcStatus =
            new AtomicInteger(TBaseConstants.META_VALUE_UNDEFINED);
    private long lastOpTime = 0;
    private long startSsdDataOffset = -2;
    private long endSsdDataOffset = -2;
    private AtomicInteger qryPriorityId =
            new AtomicInteger(TBaseConstants.META_VALUE_UNDEFINED);
    private long createTime = System.currentTimeMillis();


    public ConsumerNodeInfo(final MessageStoreManager storeManager,
                            final String consumerId, Set<String> filterCodes,
                            final String sessionKey, long sessionTime, final String partStr) {
        setConsumerId(consumerId);
        if (filterCodes != null) {
            for (String filterItem : filterCodes) {
                this.filterCondStrs.add(filterItem);
                this.filterCondCode.add(filterItem.hashCode());
            }
        }
        this.sessionKey = sessionKey;
        this.sessionTime = sessionTime;
        this.isSupportLimit = false;
        this.needSsdProc.set(false);
        this.storeManager = storeManager;
        this.partStr = partStr;
        this.createTime = System.currentTimeMillis();
        if (filterCodes != null && !filterCodes.isEmpty()) {
            this.isFilterConsume = true;
        }
        this.ssdTransId.set(TBaseConstants.META_VALUE_UNDEFINED);
    }

    public ConsumerNodeInfo(final MessageStoreManager storeManager,
                            final int qryPriorityId, final String consumerId,
                            Set<String> filterCodes, final String sessionKey,
                            long sessionTime, long ssdTransId, boolean needSsdProc,
                            boolean isSupportLimit, final String partStr) {
        setConsumerId(consumerId);
        if (filterCodes != null) {
            for (String filterItem : filterCodes) {
                this.filterCondStrs.add(filterItem);
                this.filterCondCode.add(filterItem.hashCode());
            }
        }
        this.sessionKey = sessionKey;
        this.sessionTime = sessionTime;
        this.qryPriorityId.set(qryPriorityId);
        this.ssdTransId.set(ssdTransId);
        this.needSsdProc.set(needSsdProc);
        this.storeManager = storeManager;
        this.partStr = partStr;
        this.createTime = System.currentTimeMillis();
        if (filterCodes != null && !filterCodes.isEmpty()) {
            this.isFilterConsume = true;
            this.needSsdProc.set(false);
        }
        this.isSupportLimit = isSupportLimit;
    }

    // #lizard forgives
    public int getCurrentAllowedSize(final String storeKey,
                                     final FlowCtrlRuleHandler flowCtrlRuleHandler,
                                     final long currMaxDataOffset, int maxMsgTrnsSize,
                                     boolean isEscFlowCtrl) {
        if (lastDataRdOffset >= 0) {
            long curDataDlt = currMaxDataOffset - lastDataRdOffset;
            long currTime = System.currentTimeMillis();
            recalcMsgLimitValue(curDataDlt,
                    currTime, maxMsgTrnsSize, flowCtrlRuleHandler);
            if (storeManager.isSsdServiceStart()
                    && needSsdProc.get()
                    && (currTime - createTime > 2 * 60 * 1000)) {
                // get message from ssd.
                switch (ssdProcStatus.get()) {
                    case TBaseConstants.META_VALUE_UNDEFINED:
                    case 4:
                        // Request ssd sink operation, when finish first fetch operation.
                        if (curDataDlt >= curSsdDltLimit.dataStartDltInSize
                                && currTime - this.lastOpTime > 20 * 1000) {
                            if (this.storeManager.putSsdTransferReq(partStr,
                                    storeKey, lastDataRdOffset)) {
                                ssdProcStatus.set(0);
                            }
                            this.lastOpTime = System.currentTimeMillis();
                        }
                        break;
                    case 0:
                        // Request ssd sink operation, when exceed 2 minutes since last time.
                        if (curDataDlt >= curSsdDltLimit.dataStartDltInSize
                                && currTime - this.lastOpTime > 2 * 60 * 1000) {
                            if (this.storeManager.putSsdTransferReq(partStr,
                                    storeKey, lastDataRdOffset)) {
                                ssdProcStatus.set(0);
                            }
                            this.lastOpTime = System.currentTimeMillis();
                        }
                        break;
                    case 2:
                        // Set read message size, after finish ssd sink operation.
                        if (lastDataRdOffset >= startSsdDataOffset
                                && lastDataRdOffset < endSsdDataOffset) {
                            return this.sentUnit;
                        }
                        if (lastDataRdOffset >= endSsdDataOffset) {
                            // Request ssd sink operation, after read operation.
                            if (curDataDlt >= curSsdDltLimit.dataEndDLtInSz) {
                                resetSSDProcSeg(false);
                                if (this.storeManager.putSsdTransferReq(partStr,
                                        storeKey, lastDataRdOffset)) {
                                    ssdProcStatus.set(0);
                                }
                                this.lastOpTime = System.currentTimeMillis();
                            } else {
                                resetSSDProcSeg(true);
                            }
                        }
                        break;
                    case 3:
                        // Request ssd sink operation, when has been conducted or occur error in response.
                        if (curDataDlt >= curSsdDltLimit.dataEndDLtInSz
                                && currTime - this.lastOpTime > 20 * 1000) {
                            if (this.storeManager.putSsdTransferReq(partStr,
                                    storeKey, lastDataRdOffset)) {
                                ssdProcStatus.set(0);
                            }
                            this.lastOpTime = System.currentTimeMillis();
                        }
                        break;
                    default:
                        break;
                }
            }
            if (isEscFlowCtrl
                    || (totalUnitSec > sentMsgSize
                    && this.curFlowCtrlVal.dataLtInSize > totalUnitMin)) {
                return this.sentUnit;
            } else {
                if (this.isSupportLimit) {
                    return -this.curFlowCtrlVal.freqLtInMs;
                } else {
                    return 0;
                }
            }
        } else {
            return this.sentUnit;
        }
    }

    public boolean processFromSsdFile() {
        return (storeManager != null
                && storeManager.isSsdServiceStart()
                && needSsdProc.get()
                && ssdProcStatus.get() == 2
                && lastDataRdOffset >= startSsdDataOffset
                && lastDataRdOffset < endSsdDataOffset);
    }

    public String getPartStr() {
        return partStr;
    }

    public long getStartSsdDataOffset() {
        return startSsdDataOffset;
    }

    public int getSentMsgSize() {
        return sentMsgSize;
    }

    public boolean isSupportLimit() {
        return isSupportLimit;
    }

    public boolean getNeedSsdProc() {
        return needSsdProc.get();
    }

    public void setNeedSsdProc(boolean needSsdProc) {
        this.needSsdProc.set(needSsdProc);
    }

    public long getDataStartDltInM() {
        return this.curSsdDltLimit.dataStartDltInSize / 1024 / 1024;
    }

    public long getSsdDataEndDltInM() {
        return this.curSsdDltLimit.dataEndDLtInSz / 1024 / 1024;
    }

    public long getSsdTransId() {
        return ssdTransId.get();
    }

    public void setSsdTransId(long ssdTransId, boolean needSSDProc) {
        this.ssdTransId.set(ssdTransId);
        this.needSsdProc.set(needSSDProc);
    }

    public void setSSDProcing() {
        if (this.ssdProcStatus.get() == 0) {
            this.ssdProcStatus.set(1);
            this.lastOpTime = System.currentTimeMillis();
        }
    }

    public int getQryPriorityId() {
        return qryPriorityId.get();
    }

    public void setQryPriorityId(int qryPriorityId) {
        this.qryPriorityId.set(qryPriorityId);
    }

    public void setSSDTransferFinished(boolean isUse,
                                       final long startOffset,
                                       final long endOffset) {
        if (this.ssdProcStatus.get() == 1) {
            if (isUse) {
                this.ssdProcStatus.set(2);
            } else {
                this.ssdProcStatus.set(3);
            }
            this.startSsdDataOffset = startOffset;
            this.endSsdDataOffset = endOffset;
            this.lastOpTime = System.currentTimeMillis();
        }
    }

    public void resetSSDProcSeg(boolean finished) {
        if (finished) {
            this.ssdProcStatus.set(4);
        } else {
            this.ssdProcStatus.set(3);
        }
        this.startSsdDataOffset = -2;
        this.endSsdDataOffset = -2;
        this.lastOpTime = System.currentTimeMillis();
    }

    public long getNextStatTime() {
        return nextStatTime;
    }

    public long getLastDataRdOffset() {
        return lastDataRdOffset;
    }

    public int getSentUnit() {
        return sentUnit;
    }

    public long getTotalUnitSec() {
        return totalUnitSec;
    }

    public long getTotalUnitMin() {
        return totalUnitMin;
    }

    public FlowCtrlResult getCurFlowCtrlVal() {
        return curFlowCtrlVal;
    }

    public long getNextLimitUpdateTime() {
        return nextLimitUpdateTime;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
        if (consumerId.lastIndexOf("_") != -1) {
            String targetStr = consumerId.substring(consumerId.lastIndexOf("_") + 1);
            String[] strInfos = targetStr.split("-");
            if (strInfos.length > 2) {
                this.rmtAddrInfo = new StringBuilder(256)
                        .append(strInfos[0]).append("#").append(strInfos[1]).toString();
            }
        }
    }

    public Set<Integer> getFilterCondCodeSet() {
        return this.filterCondCode;
    }

    public Set<String> getFilterCondStrs() {
        return filterCondStrs;
    }

    public long getCurFlowCtrlLimitSize() {
        return this.curFlowCtrlVal.dataLtInSize / 1024 / 1024;
    }

    public int getCurFlowCtrlFreqLimit() {
        return this.curFlowCtrlVal.freqLtInMs;
    }

    public boolean isFilterConsume() {
        return isFilterConsume;
    }

    public long getLastGetTime() {
        return lastGetTime;
    }

    public String getSessionKey() {
        return sessionKey;
    }

    public long getSessionTime() {
        return sessionTime;
    }

    public void setLastProcInfo(long lastGetTime, long lastRdDataOffset, int totalMsgSize) {
        this.lastGetTime = lastGetTime;
        this.lastDataRdOffset = lastRdDataOffset;
        this.sentMsgSize += totalMsgSize;
        this.totalUnitMin += totalMsgSize;

    }

    public String getRmtAddrInfo() {
        return this.rmtAddrInfo;
    }

    /***
     * Recalculate message limit value.
     *
     * @param curDataDlt
     * @param currTime
     * @param maxMsgTrnsSize
     * @param flowCtrlRuleHandler
     */
    private void recalcMsgLimitValue(long curDataDlt, long currTime, int maxMsgTrnsSize,
                                     final FlowCtrlRuleHandler flowCtrlRuleHandler) {
        if (currTime > nextLimitUpdateTime) {
            this.curFlowCtrlVal = flowCtrlRuleHandler.getCurDataLimit(curDataDlt);
            if (this.curFlowCtrlVal == null) {
                this.curFlowCtrlVal = new FlowCtrlResult(Long.MAX_VALUE, 0);
            }
            if (storeManager.isSsdServiceStart() && needSsdProc.get()) {
                this.curSsdDltLimit = flowCtrlRuleHandler.getCurSSDStartDltInSZ();
            }
            currTime = System.currentTimeMillis();
            this.sentMsgSize = 0;
            this.totalUnitMin = 0;
            this.nextStatTime =
                    currTime + TBaseConstants.CFG_FC_MAX_SAMPLING_PERIOD;
            this.nextLimitUpdateTime =
                    currTime + TBaseConstants.CFG_FC_MAX_LIMITING_DURATION;
            this.totalUnitSec = this.curFlowCtrlVal.dataLtInSize / 12;
            this.sentUnit =
                    totalUnitSec > maxMsgTrnsSize ? maxMsgTrnsSize : (int) totalUnitSec;
        } else if (currTime > nextStatTime) {
            sentMsgSize = 0;
            nextStatTime =
                    currTime + TBaseConstants.CFG_FC_MAX_SAMPLING_PERIOD;
        }
    }

}

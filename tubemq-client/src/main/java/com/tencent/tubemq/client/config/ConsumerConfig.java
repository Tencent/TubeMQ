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

package com.tencent.tubemq.client.config;

import com.tencent.tubemq.client.common.TClientConstants;
import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.cluster.MasterInfo;
import com.tencent.tubemq.corebase.utils.TStringUtils;

/**
 * Contains configuration information of a consumer.
 */
public class ConsumerConfig extends TubeClientConfig {

    private String consumerGroup;

    /* consumeModel
     *    Set the start position of the consumer group. The value can be [-1, 0, 1]. Default value is 0.
     * -1: Start from 0 for the first time. Otherwise start from last consume position.
     *  0: Start from the latest position for the first time. Otherwise start from last consume position.
     *  1: Start from the latest consume position.
    */
    private int consumeModel = 0;
    private int maxSubInfoReportIntvlTimes =
            TClientConstants.MAX_SUBSCRIBE_REPORT_INTERVAL_TIMES;
    private long msgNotFoundWaitPeriodMs =
            TClientConstants.CFG_DEFAULT_MSG_NOTFOUND_WAIT_PERIOD_MS;
    private long shutDownRebalanceWaitPeriodMs =
            TClientConstants.CFG_DEFAULT_SHUTDOWN_REBALANCE_WAIT_PERIOD_MS;
    private int pushFetchThreadCnt =
            TClientConstants.CFG_DEFAULT_CLIENT_PUSH_FETCH_THREAD_CNT;
    private boolean pushIsListenerWaitTimeoutRollBack = true;
    private boolean pushIsListenerThrowedRollBack = false;
    private long pushListenerWaitPeriodMs =
            TClientConstants.CFG_DEFAULT_PUSH_LISTENER_WAIT_PERIOD_MS;
    private boolean pullRebConfirmTimeoutRollBack = true;
    private long pullRebConfirmWaitPeriodMs =
            TClientConstants.CFG_DEFAULT_PULL_REB_CONFIRM_WAIT_PERIOD_MS;
    private long pullProtectConfirmTimeoutMs =
            TClientConstants.CFG_DEFAULT_PULL_PROTECT_CONFIRM_WAIT_PERIOD_MS;
    private boolean pullConfirmInLocal = false;


    public ConsumerConfig(String localHostIP, String masterAddrInfo,
                          String consumerGroup) throws Exception {
        super(localHostIP, masterAddrInfo);
        validConsumerGroupParmeter(consumerGroup);
        this.consumerGroup = consumerGroup.trim();
    }

    public ConsumerConfig(String localHostIP, MasterInfo masterInfo,
                          String consumerGroup) throws Exception {
        super(localHostIP, masterInfo);
        validConsumerGroupParmeter(consumerGroup);
        this.consumerGroup = consumerGroup.trim();
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public int getConsumeModel() {
        return consumeModel;
    }

    public void setConsumeModel(int consumeModel) {
        if (consumeModel > 0) {
            this.consumeModel = 1;
        } else if (consumeModel < 0) {
            this.consumeModel = -1;
        } else {
            this.consumeModel = 0;
        }
    }

    public long getMsgNotFoundWaitPeriodMs() {
        return msgNotFoundWaitPeriodMs;
    }

    public void setMsgNotFoundWaitPeriodMs(long msgNotFoundWaitPeriodMs) {
        this.msgNotFoundWaitPeriodMs = msgNotFoundWaitPeriodMs;
    }

    public long getShutDownRebalanceWaitPeriodMs() {
        return shutDownRebalanceWaitPeriodMs;
    }

    public void setShutDownRebalanceWaitPeriodMs(long shutDownRebalanceWaitPeriodMs) {
        this.shutDownRebalanceWaitPeriodMs = shutDownRebalanceWaitPeriodMs;
    }

    public int getPushFetchThreadCnt() {
        return pushFetchThreadCnt;
    }

    public void setPushFetchThreadCnt(int pushFetchThreadCnt) {
        if (pushFetchThreadCnt <= 0) {
            this.pushFetchThreadCnt = TClientConstants.CFG_DEFAULT_CLIENT_PUSH_FETCH_THREAD_CNT;
        } else {
            this.pushFetchThreadCnt = pushFetchThreadCnt;
        }
    }

    public boolean isPushListenerWaitTimeoutRollBack() {
        return pushIsListenerWaitTimeoutRollBack;
    }

    public void setPushListenerWaitTimeoutRollBack(boolean pushIsListenerWaitTimeoutRollBack) {
        this.pushIsListenerWaitTimeoutRollBack = pushIsListenerWaitTimeoutRollBack;
    }

    public boolean isPushListenerThrowedRollBack() {
        return pushIsListenerThrowedRollBack;
    }

    public void setPushListenerThrowedRollBack(boolean pushIsListenerThrowedRollBack) {
        this.pushIsListenerThrowedRollBack = pushIsListenerThrowedRollBack;
    }

    public long getPushListenerWaitPeriodMs() {
        return pushListenerWaitPeriodMs;
    }

    public void setPushListenerWaitPeriodMs(long pushListenerWaitPeriodMs) {
        this.pushListenerWaitPeriodMs = pushListenerWaitPeriodMs;
    }

    public boolean isPullRebConfirmTimeoutRollBack() {
        return pullRebConfirmTimeoutRollBack;
    }

    public void setPullRebConfirmTimeoutRollBack(boolean pullRebConfirmTimeoutRollBack) {
        this.pullRebConfirmTimeoutRollBack = pullRebConfirmTimeoutRollBack;
    }

    public long getPullRebConfirmWaitPeriodMs() {
        return pullRebConfirmWaitPeriodMs;
    }

    public void setPullRebConfirmWaitPeriodMs(long pullRebConfirmWaitPeriodMs) {
        this.pullRebConfirmWaitPeriodMs = pullRebConfirmWaitPeriodMs;
    }

    public boolean isPullConfirmInLocal() {
        return pullConfirmInLocal;
    }

    public void setPullConfirmInLocal(boolean pullConfirmInLocal) {
        this.pullConfirmInLocal = pullConfirmInLocal;
    }

    public long getPullProtectConfirmTimeoutMs() {
        return pullProtectConfirmTimeoutMs;
    }

    public void setPullProtectConfirmTimeoutMs(long pullProtectConfirmTimeoutMs) {
        this.pullProtectConfirmTimeoutMs = pullProtectConfirmTimeoutMs;
    }

    public int getMaxSubInfoReportIntvlTimes() {
        return maxSubInfoReportIntvlTimes;
    }

    public void setMaxSubInfoReportIntvlTimes(int maxSubInfoReportIntvlTimes) {
        this.maxSubInfoReportIntvlTimes = maxSubInfoReportIntvlTimes;
    }

    private void validConsumerGroupParmeter(String consumerGroup) throws Exception {
        if (TStringUtils.isBlank(consumerGroup)) {
            throw new Exception("Illegal parameter: consumerGroup is Blank!");
        }
        String tmpConsumerGroup = String.valueOf(consumerGroup).trim();
        if (tmpConsumerGroup.length() > TBaseConstants.META_MAX_GROUPNAME_LENGTH) {
            throw new Exception(new StringBuilder(512)
                    .append("Illegal parameter: the max length of consumerGroup is ")
                    .append(TBaseConstants.META_MAX_GROUPNAME_LENGTH)
                    .append(" characters").toString());
        }
        if (!tmpConsumerGroup.matches(TBaseConstants.META_TMP_STRING_VALUE)) {
            throw new Exception(new StringBuilder(512)
                    .append("Illegal parameter: the value of consumerGroup")
                    .append(" must begin with a letter, ")
                    .append("can only contain characters,numbers,and underscores").toString());
        }
    }

    @Override
    public String toString() {
        return new StringBuilder(512).append("\"ConsumerConfig\":{")
                .append("\"consumerGroup\":\"").append(this.consumerGroup)
                .append("\",\"maxSubInfoReportIntvlTimes\":").append(this.maxSubInfoReportIntvlTimes)
                .append(",\"consumeModel\":").append(this.consumeModel)
                .append(",\"msgNotFoundWaitPeriodMs\":").append(this.msgNotFoundWaitPeriodMs)
                .append(",\"shutDownRebalanceWaitPeriodMs\":").append(this.shutDownRebalanceWaitPeriodMs)
                .append(",\"pushFetchThreadCnt\":").append(this.pushFetchThreadCnt)
                .append(",\"pushIsListenerWaitTimeoutRollBack\":").append(this.pushIsListenerWaitTimeoutRollBack)
                .append(",\"pushIsListenerThrowedRollBack\":").append(this.pushIsListenerThrowedRollBack)
                .append(",\"pushListenerWaitPeriodMs\":").append(this.pushListenerWaitPeriodMs)
                .append(",\"pullIsRebConfirmWaitTimeoutRollBack\":").append(this.pullRebConfirmTimeoutRollBack)
                .append(",\"pullConfirmWaitPeriodMs\":").append(this.pullRebConfirmWaitPeriodMs)
                .append(",\"pullProtectConfirmTimeoutPeriodMs\":").append(this.pullProtectConfirmTimeoutMs)
                .append(",\"pullConfirmInLocal\":").append(this.pullConfirmInLocal)
                .append(",\"ClientConfig\":").append(toJsonString())
                .append("}").toString();
    }
}

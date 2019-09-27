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

package com.tencent.tubemq.server.common.fileconfig;

import com.tencent.tubemq.server.common.TServerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKConfig {
    private static final Logger logger = LoggerFactory.getLogger(ZKConfig.class);
    private String zkServerAddr = "localhost:2181";
    private String zkNodeRoot = "/tubemq";
    private int zkSessionTimeoutMs = 180000;
    private int zkConnectionTimeoutMs = 600000;
    private int zkSyncTimeMs = 1000;
    private long zkCommitPeriodMs = 5000L;
    private int zkCommitFailRetries = TServerConstants.CFG_ZK_COMMIT_DEFAULT_RETRIES;

    public ZKConfig() {

    }

    public String getZkServerAddr() {
        return zkServerAddr;
    }

    public void setZkServerAddr(String zkServerAddr) {
        this.zkServerAddr = zkServerAddr;
    }

    public String getZkNodeRoot() {
        return zkNodeRoot;
    }

    public void setZkNodeRoot(String zkNodeRoot) {
        this.zkNodeRoot = zkNodeRoot;
    }

    public int getZkSessionTimeoutMs() {
        return zkSessionTimeoutMs;
    }

    public void setZkSessionTimeoutMs(int zkSessionTimeoutMs) {
        this.zkSessionTimeoutMs = zkSessionTimeoutMs;
    }

    public int getZkConnectionTimeoutMs() {
        return zkConnectionTimeoutMs;
    }

    public void setZkConnectionTimeoutMs(int zkConnectionTimeoutMs) {
        this.zkConnectionTimeoutMs = zkConnectionTimeoutMs;
    }

    public int getZkSyncTimeMs() {
        return zkSyncTimeMs;
    }

    public void setZkSyncTimeMs(int zkSyncTimeMs) {
        this.zkSyncTimeMs = zkSyncTimeMs;
    }

    public int getZkCommitFailRetries() {
        return zkCommitFailRetries;
    }

    public void setZkCommitFailRetries(int zkCommitFailRetries) {
        this.zkCommitFailRetries = zkCommitFailRetries;
    }

    public long getZkCommitPeriodMs() {
        return zkCommitPeriodMs;
    }

    public void setZkCommitPeriodMs(long zkCommitPeriodMs) {
        this.zkCommitPeriodMs = zkCommitPeriodMs;
    }

    @Override
    public String toString() {
        return new StringBuilder(512)
                .append("\"ZKConfig\":{\"zkServerAddr\":\"").append(zkServerAddr)
                .append("\",\"zkNodeRoot\":\"").append(zkNodeRoot)
                .append("\",\"zkSessionTimeoutMs\":").append(zkSessionTimeoutMs)
                .append(",\"zkConnectionTimeoutMs\":").append(zkConnectionTimeoutMs)
                .append(",\"zkSyncTimeMs\":").append(zkSyncTimeMs)
                .append(",\"zkCommitPeriodMs\":").append(zkCommitPeriodMs)
                .append(",\"zkCommitFailRetries\":").append(zkCommitFailRetries)
                .append("}").toString();
    }
}

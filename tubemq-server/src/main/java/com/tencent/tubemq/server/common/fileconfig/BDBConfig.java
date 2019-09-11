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

import com.sleepycat.je.Durability;


public class BDBConfig {
    private String bdbRepGroupName;
    private String bdbNodeName;
    private int bdbNodePort;
    private String bdbEnvHome;
    private String bdbHelperHost;
    private int bdbLocalSync;
    private int bdbReplicaSync;
    private int bdbReplicaAck;
    private long bdbStatusCheckTimeoutMs = 10000;

    public BDBConfig() {

    }

    public String getBdbRepGroupName() {
        return bdbRepGroupName;
    }

    public void setBdbRepGroupName(String bdbRepGroupName) {
        this.bdbRepGroupName = bdbRepGroupName;
    }

    public String getBdbNodeName() {
        return bdbNodeName;
    }

    public void setBdbNodeName(String bdbNodeName) {
        this.bdbNodeName = bdbNodeName;
    }

    public int getBdbNodePort() {
        return bdbNodePort;
    }

    public void setBdbNodePort(int bdbNodePort) {
        this.bdbNodePort = bdbNodePort;
    }

    public String getBdbEnvHome() {
        return bdbEnvHome;
    }

    public void setBdbEnvHome(String bdbEnvHome) {
        this.bdbEnvHome = bdbEnvHome;
    }

    public String getBdbHelperHost() {
        return bdbHelperHost;
    }

    public void setBdbHelperHost(String bdbHelperHost) {
        this.bdbHelperHost = bdbHelperHost;
    }

    public Durability.SyncPolicy getBdbLocalSync() {
        switch (bdbLocalSync) {
            case 1:
                return Durability.SyncPolicy.SYNC;
            case 2:
                return Durability.SyncPolicy.NO_SYNC;
            case 3:
                return Durability.SyncPolicy.WRITE_NO_SYNC;
            default:
                return Durability.SyncPolicy.SYNC;
        }
    }

    public void setBdbLocalSync(int bdbLocalSync) {
        this.bdbLocalSync = bdbLocalSync;
    }

    public Durability.SyncPolicy getBdbReplicaSync() {
        switch (bdbReplicaSync) {
            case 1:
                return Durability.SyncPolicy.SYNC;
            case 2:
                return Durability.SyncPolicy.NO_SYNC;
            case 3:
                return Durability.SyncPolicy.WRITE_NO_SYNC;
            default:
                return Durability.SyncPolicy.SYNC;
        }
    }

    public void setBdbReplicaSync(int bdbReplicaSync) {
        this.bdbReplicaSync = bdbReplicaSync;
    }

    public Durability.ReplicaAckPolicy getBdbReplicaAck() {
        switch (bdbReplicaAck) {
            case 1:
                return Durability.ReplicaAckPolicy.SIMPLE_MAJORITY;
            case 2:
                return Durability.ReplicaAckPolicy.ALL;
            case 3:
                return Durability.ReplicaAckPolicy.NONE;
            default:
                return Durability.ReplicaAckPolicy.SIMPLE_MAJORITY;
        }
    }

    public void setBdbReplicaAck(int bdbReplicaAck) {
        this.bdbReplicaAck = bdbReplicaAck;
    }

    public long getBdbStatusCheckTimeoutMs() {
        return bdbStatusCheckTimeoutMs;
    }

    public void setBdbStatusCheckTimeoutMs(long bdbStatusCheckTimeoutMs) {
        this.bdbStatusCheckTimeoutMs = bdbStatusCheckTimeoutMs;
    }

    @Override
    public String toString() {
        return new StringBuilder(512)
                .append("\"BDBConfig\":{\"bdbRepGroupName\":").append(bdbRepGroupName)
                .append("\",\"bdbNodeName\":\"").append(bdbNodeName)
                .append("\",\"bdbNodePort\":").append(bdbNodePort)
                .append(",\"bdbEnvHome\":\"").append(bdbEnvHome)
                .append("\",\"bdbHelperHost\":\"").append(bdbHelperHost)
                .append("\",\"bdbLocalSync\":").append(bdbLocalSync)
                .append(",\"bdbReplicaSync\":").append(bdbReplicaSync)
                .append(",\"bdbReplicaAck\":").append(bdbReplicaAck)
                .append(",\"bdbStatusCheckTimeoutMs\":").append(bdbStatusCheckTimeoutMs)
                .append("}").toString();
    }
}

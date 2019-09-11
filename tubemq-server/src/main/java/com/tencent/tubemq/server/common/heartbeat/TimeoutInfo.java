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

package com.tencent.tubemq.server.common.heartbeat;

public class TimeoutInfo {
    private long timeoutTime = 0L;
    private String secondKey = "";
    private String thirdKey = "";

    public TimeoutInfo(final String secondKey, final String thirdKey, long timeoutDelta) {
        this.secondKey = secondKey;
        this.thirdKey = thirdKey;
        this.timeoutTime = timeoutDelta + System.currentTimeMillis();
    }

    public TimeoutInfo(final String secondKey, long timeoutDelta) {
        this.secondKey = secondKey;
        this.timeoutTime = timeoutDelta + System.currentTimeMillis();
    }

    public TimeoutInfo(long timeoutDelta) {
        this.timeoutTime = timeoutDelta + System.currentTimeMillis();
    }

    public long getTimeoutTime() {
        return timeoutTime;
    }

    public void setTimeoutTime(long timeoutTime) {
        this.timeoutTime = timeoutTime;
    }

    public String getSecondKey() {
        return secondKey;
    }

    public String getThirdKey() {
        return thirdKey;
    }
}

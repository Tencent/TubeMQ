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

package com.tencent.tubemq.client.common;

public class TClientConstants {

    public static final int CFG_DEFAULT_REGISTER_RETRY_TIMES = 5;
    public static final int CFG_DEFAULT_HEARTBEAT_RETRY_TIMES = 5;
    public static final long CFG_DEFAULT_HEARTBEAT_PERIOD_MS = 13000;
    public static final long CFG_DEFAULT_REGFAIL_WAIT_PERIOD_MS = 1000;
    public static final long CFG_DEFAULT_MSG_NOTFOUND_WAIT_PERIOD_MS = 200L;
    public static final long CFG_DEFAULT_PUSH_LISTENER_WAIT_PERIOD_MS = 3000L;
    public static final long CFG_DEFAULT_PULL_REB_CONFIRM_WAIT_PERIOD_MS = 3000L;
    public static final long CFG_DEFAULT_PULL_PROTECT_CONFIRM_WAIT_PERIOD_MS = 60000L;
    public static final long CFG_DEFAULT_SHUTDOWN_REBALANCE_WAIT_PERIOD_MS = 10000L;
    public static final long CFG_DEFAULT_HEARTBEAT_PERIOD_AFTER_RETRY_FAIL = 60000;
    public static final int CFG_DEFAULT_CLIENT_PUSH_FETCH_THREAD_CNT =
            Runtime.getRuntime().availableProcessors();

    public static final int MAX_CONNECTION_FAILURE_LOG_TIMES = 10;
    public static final int MAX_SUBSCRIBE_REPORT_INTERVAL_TIMES = 6;

}

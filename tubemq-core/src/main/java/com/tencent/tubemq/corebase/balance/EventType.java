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

package com.tencent.tubemq.corebase.balance;

public enum EventType {
    CONNECT() {
        @Override
        public int getValue() {
            return 1;
        }

        @Override
        public String getDesc() {
            return "Connect to some broker after Disconnecting from some other broker.";
        }
    },

    ONLY_CONNECT() {
        @Override
        public int getValue() {
            return 10;
        }

        @Override
        public String getDesc() {
            return "Only connect to some broker";
        }
    },

    DISCONNECT() {
        @Override
        public int getValue() {
            return 2;
        }

        @Override
        public String getDesc() {
            return "Disconnect from some broker.";
        }
    },

    ONLY_DISCONNECT() {
        @Override
        public int getValue() {
            return 20;
        }

        @Override
        public String getDesc() {
            return "Disconnect from some broker,then finish.";
        }

    },

    REPORT() {
        @Override
        public int getValue() {
            return 3;
        }

        @Override
        public String getDesc() {
            return "Report current status.";
        }
    },
    REFRESH() {
        @Override
        public int getValue() {
            return 4;
        }

        @Override
        public String getDesc() {
            return "Update whole producer published topic info";
        }
    },
    STOPREBALANCE() {
        @Override
        public int getValue() {
            return 5;
        }

        @Override
        public String getDesc() {
            return "Stop rebalance thread";
        }
    },

    UNKNOWN() {
        @Override
        public int getValue() {
            return -1;
        }

        @Override
        public String getDesc() {
            return "Unknown operation type";
        }
    };

    public static EventType valueOf(int value) {
        for (EventType type : EventType.values()) {
            if (type.getValue() == value) {
                return type;
            }
        }
        return UNKNOWN;
    }

    public abstract int getValue();

    public abstract String getDesc();

}

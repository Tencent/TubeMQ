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

public enum EventStatus {
    DONE() {
        @Override
        public int getValue() {
            return 2;
        }

        @Override
        public String getDesc() {
            return "Process Done";
        }
    },

    FAILED() {
        @Override
        public int getValue() {
            return -2;
        }

        @Override
        public String getDesc() {
            return "Process failed";
        }
    },

    PROCESSING() {
        @Override
        public int getValue() {
            return 1;
        }

        @Override
        public String getDesc() {
            return "Being processed";
        }
    },


    TODO() {
        @Override
        public int getValue() {
            return 0;
        }

        @Override
        public String getDesc() {
            return "To be processed";
        }
    },

    UNKNOWN() {
        @Override
        public int getValue() {
            return -1;
        }

        @Override
        public String getDesc() {
            return "Unknown event status";
        }
    };

    public static EventStatus valueOf(int value) {
        for (EventStatus status : EventStatus.values()) {
            if (status.getValue() == value) {
                return status;
            }
        }

        return UNKNOWN;
    }

    public abstract int getValue();

    public abstract String getDesc();

}


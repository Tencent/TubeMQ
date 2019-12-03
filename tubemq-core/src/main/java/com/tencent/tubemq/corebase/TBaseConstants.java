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

package com.tencent.tubemq.corebase;

public class TBaseConstants {

    public static final int META_VALUE_UNDEFINED = -2;

    public static final int META_DEFAULT_MASTER_PORT = 8715;
    public static final int META_DEFAULT_MASTER_TLS_PORT = 8716;
    public static final int META_DEFAULT_BROKER_PORT = 8123;
    public static final int META_DEFAULT_BROKER_TLS_PORT = 8124;
    public static final int META_STORE_INS_BASE = 10000;

    public static final String META_DEFAULT_CHARSET_NAME = "UTF-8";
    public static final int META_MAX_MSGTYPE_LENGTH = 255;
    public static final int META_MAX_MESSAGEG_HEADER_SIZE = 1024;
    public static final int META_MAX_MESSAGEG_DATA_SIZE = 1024 * 1024;
    public static final int META_MAX_PARTITION_COUNT = 100;
    public static final int META_MAX_BROKER_IP_LENGTH = 32;
    public static final int META_MAX_USERNAME_LENGTH = 64;
    public static final int META_MAX_DATEVALUE_LENGTH = 14;
    public static final int META_MAX_CALLBACK_STRING_LENGTH = 128;
    public static final int META_MAX_TOPICNAME_LENGTH = 64;
    public static final int META_MAX_GROUPNAME_LENGTH = 1024;
    public static final int META_MAX_OPREASON_LENGTH = 1024;

    public static final String META_TMP_FILTER_VALUE = "^[_A-Za-z0-9]+$";
    public static final String META_TMP_STRING_VALUE = "^[a-zA-Z]\\w+$";
    public static final String META_TMP_NUMBER_VALUE = "^-?[0-9]\\d*$";
    public static final String META_TMP_GROUP_VALUE = "^[a-zA-Z][\\w-]+$";
    public static final String META_TMP_CONSUMERID_VALUE = "^[_A-Za-z0-9\\.\\-]+$";
    public static final String META_TMP_CALLBACK_STRING_VALUE = "^[_A-Za-z0-9]+$";
    public static final String META_TMP_DATE_VALUE = "yyyyMMddHHmmss";
    public static final String META_TMP_IP_ADDRESS_VALUE =
            "((?:(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d))))";
    public static final String META_TMP_PORT_REGEX = "[\\d]+";

    public static final int CONSUME_MODEL_READ_NORMAL = 0;
    public static final int CONSUME_MODEL_READ_FROM_MAX = 1;
    public static final int CONSUME_MODEL_READ_FROM_MAX_ALWAYS = 2;

    public static final long CFG_FC_MAX_LIMITING_DURATION = 60000;
    public static final long CFG_FC_MAX_SAMPLING_PERIOD = 5000;
    public static final int CFG_FLT_MAX_FILTER_ITEM_LENGTH = 256;
    public static final int CFG_FLT_MAX_FILTER_ITEM_COUNT = 500;

    public static final int META_MAX_CLIENT_HOSTNAME_LENGTH = 256;
    public static final int META_MAX_CLIENT_ID_LENGTH = 1024;
    public static final int META_MAX_BOOKED_TOPIC_COUNT = 1024;

    public static final long CFG_DEFAULT_AUTH_TIMESTAMP_VALID_INTERVAL = 20000;

}

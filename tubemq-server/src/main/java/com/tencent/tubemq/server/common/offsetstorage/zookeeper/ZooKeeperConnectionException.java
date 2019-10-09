/**
 * Tencent is pleased to support the open source community by making TubeMQ available.
 * <p>
 * Copyright (C) 2012-2019 Tencent. All Rights Reserved.
 * <p>
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.tubemq.server.common.offsetstorage.zookeeper;

import java.io.IOException;

/**
 * Thrown if the client can't connect to zookeeper
 * Copied from <a href="http://hbase.apache.org">Apache HBase Project</a>
 */
public class ZooKeeperConnectionException extends IOException {
    private static final long serialVersionUID = 1L << 23 - 1L;

    /**
     * default constructor
     */
    public ZooKeeperConnectionException() {
        super();
    }

    /**
     * Constructor
     *
     * @param s message
     */
    public ZooKeeperConnectionException(String s) {
        super(s);
    }

    /**
     * Constructor taking another exception.
     *
     * @param e Exception to grab data from.
     */
    public ZooKeeperConnectionException(String message, Exception e) {
        super(message, e);
    }
}

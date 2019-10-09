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

/**
 * Base class for internal listeners of ZooKeeper events.
 * <p/>
 * The {@link ZooKeeperWatcher} for a process will execute the appropriate methods of
 * implementations of this class. In order to receive events from the watcher, every listener must
 * register itself via {@link ZooKeeperWatcher#registerListener}.
 * <p/>
 * Subclasses need only override those methods in which they are interested.
 * <p/>
 * Note that the watcher will be blocked when invoking methods in listeners so they must not be
 * long-running.
 *
 * Copied from <a href="http://hbase.apache.org">Apache HBase Project</a>
 */
public abstract class ZooKeeperListener {

    // Reference to the zk watcher which also contains configuration and
    // constants
    protected ZooKeeperWatcher watcher;

    /**
     * Construct a ZooKeeper event listener.
     */
    public ZooKeeperListener(ZooKeeperWatcher watcher) {
        this.watcher = watcher;
    }

    /**
     * Called when a new node has been created.
     *
     * @param path full path of the new node
     */
    public void nodeCreated(String path) {
        // no-op
    }

    /**
     * Called when a node has been deleted
     *
     * @param path full path of the deleted node
     */
    public void nodeDeleted(String path) {
        // no-op
    }

    /**
     * Called when an existing node has changed data.
     *
     * @param path full path of the updated node
     */
    public void nodeDataChanged(String path) {
        // no-op
    }

    /**
     * Called when an existing node has a child node added or removed.
     *
     * @param path full path of the node whose children have changed
     */
    public void nodeChildrenChanged(String path) {
        // no-op
    }
}

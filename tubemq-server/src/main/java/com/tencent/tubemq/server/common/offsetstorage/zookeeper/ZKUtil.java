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

import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.server.common.fileconfig.ZKConfig;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Internal utility class for ZooKeeper.
 * <p>
 * <p>Contains only static methods and constants.
 * <p>
 * <p>Methods all throw {@link KeeperException} if there is an unexpected
 * zookeeper exception, so callers of these methods must handle appropriately.
 * If ZK is required for the operation, the server will need to be aborted.
 *
 * Copied from <a href="http://hbase.apache.org">Apache HBase Project</a>
 */
public class ZKUtil {
    private static final Logger logger = LoggerFactory.getLogger(ZKUtil.class);
    private static final int RETRY_TIMES = 3;
    private static final int RETRY_INTERVAL = 1000;
    private static final int SOCKET_RETRY_WAIT_MS = 200;
    private static final int DEFAULT_ZOOKEPER_CLIENT_PORT = 2181;

    // Replace this with ZooKeeper constant when ZOOKEEPER-277 is resolved.
    private static final char ZNODE_PATH_SEPARATOR = '/';

    /**
     * Creates a new connection to ZooKeeper, pulling settings and ensemble config from the
     * specified configuration object using methods from {@link ZKConfig} .
     * <p/>
     * Sets the connection status monitoring watcher to the specified watcher.
     * <p/>
     * configuration to pull ensemble and other settings from
     *
     * @param watcher watcher to monitor connection changes
     * @return connection to zookeeper
     * @throws IOException if unable to connect to zk or config problem
     */
    public static RecoverableZooKeeper connect(ZKConfig zkConfig,
                                               Watcher watcher) throws IOException {
        if (zkConfig.getZkServerAddr() == null) {
            throw new IOException("Unable to determine ZooKeeper Server Address String");
        }
        return new RecoverableZooKeeper(zkConfig.getZkServerAddr(),
                zkConfig.getZkSessionTimeoutMs(), watcher, RETRY_TIMES, RETRY_INTERVAL);
    }

    //
    // Helper methods
    //

    /**
     * Join the prefix znode name with the suffix znode name to generate a proper full znode name.
     * <p/>
     * Assumes prefix does not end with slash and suffix does not begin with it.
     *
     * @param prefix beginning of znode name
     * @param suffix ending of znode name
     * @return result of properly joining prefix with suffix
     */
    public static String joinZNode(String prefix, String suffix) {
        return prefix + ZNODE_PATH_SEPARATOR + suffix;
    }

    /**
     * Returns the full path of the immediate parent of the specified node.
     *
     * @param node path to get parent of
     * @return parent of path, null if passed the root node or an invalid node
     */
    public static String getParent(String node) {
        int idx = node.lastIndexOf(ZNODE_PATH_SEPARATOR);
        return idx <= 0 ? null : node.substring(0, idx);
    }


    /**
     * Check if the specified node exists. Sets no watches.
     * <p/>
     * Returns true if node exists, false if not. Returns an exception if there is an unexpected
     * zookeeper exception.
     *
     * @param zkw   zk reference
     * @param znode path of node to watch
     * @return version of the node if it exists, -1 if does not exist
     * @throws KeeperException if unexpected zookeeper exception
     */
    public static int checkExists(ZooKeeperWatcher zkw, String znode) throws KeeperException {
        try {
            Stat s = zkw.getRecoverableZooKeeper().exists(znode, null);
            return s != null ? s.getVersion() : -1;
        } catch (KeeperException e) {
            logger.warn(zkw.prefix("Unable to set watcher on znode (" + znode + ")"), e);
            zkw.keeperException(e);
            return -1;
        } catch (InterruptedException e) {
            logger.warn(zkw.prefix("Unable to set watcher on znode (" + znode + ")"), e);
            zkw.interruptedException(e);
            return -1;
        }
    }


    /**
     * Get the data at the specified znode and set a watch.
     * <p/>
     * Returns the data and sets a watch if the node exists. Returns null and no watch is set if the
     * node does not exist or there is an exception.
     *
     * @param zkw   zk reference
     * @param znode path of node
     * @return data of the specified znode, or null
     * @throws KeeperException if unexpected zookeeper exception
     */
    public static byte[] getDataAndWatch(ZooKeeperWatcher zkw, String znode) throws KeeperException {
        return getDataInternal(zkw, znode, null, true);
    }


    private static byte[] getDataInternal(ZooKeeperWatcher zkw, String znode, Stat stat,
                                          boolean watcherSet) throws KeeperException {
        try {
            byte[] data = zkw.getRecoverableZooKeeper().getData(znode, zkw, stat);
            logRetrievedMsg(zkw, znode, data, watcherSet);
            return data;
        } catch (KeeperException.NoNodeException e) {
            if (logger.isDebugEnabled()) {
                logger.debug(zkw.prefix("Unable to get data of znode " + znode + " "
                        + "because node does not exist (not an error)"));
            }
            return null;
        } catch (KeeperException e) {
            logger.warn(zkw.prefix("Unable to get data of znode " + znode), e);
            zkw.keeperException(e);
            return null;
        } catch (InterruptedException e) {
            logger.warn(zkw.prefix("Unable to get data of znode " + znode), e);
            zkw.interruptedException(e);
            return null;
        }
    }


    /**
     * Sets the data of the existing znode to be the specified data. Ensures that the current data
     * has the specified expected version.
     * <p/>
     * <p/>
     * If the node does not exist, a {@link NoNodeException} will be thrown.
     * <p/>
     * <p/>
     * If their is a version mismatch, method returns null.
     * <p/>
     * <p/>
     * No watches are set but setting data will trigger other watchers of this node.
     * <p/>
     * <p/>
     * If there is another problem, a KeeperException will be thrown.
     *
     * @param zkw             zk reference
     * @param znode           path of node
     * @param data            data to set for node
     * @param expectedVersion version expected when setting data
     * @return true if data set, false if version mismatch
     * @throws KeeperException if unexpected zookeeper exception
     */
    public static boolean setData(ZooKeeperWatcher zkw, String znode, byte[] data, int expectedVersion)
            throws KeeperException, KeeperException.NoNodeException {
        try {
            return zkw.getRecoverableZooKeeper().setData(znode, data, expectedVersion) != null;
        } catch (InterruptedException e) {
            zkw.interruptedException(e);
            return false;
        }
    }

    /**
     * Sets the data of the existing znode to be the specified data. The node must exist but no
     * checks are done on the existing data or version.
     * <p/>
     * <p/>
     * If the node does not exist, a {@link NoNodeException} will be thrown.
     * <p/>
     * <p/>
     * No watches are set but setting data will trigger other watchers of this node.
     * <p/>
     * <p/>
     * If there is another problem, a KeeperException will be thrown.
     *
     * @param zkw   zk reference
     * @param znode path of node
     * @param data  data to set for node
     * @throws KeeperException if unexpected zookeeper exception
     */
    public static void setData(ZooKeeperWatcher zkw, String znode, byte[] data)
            throws KeeperException {
        setData(zkw, znode, data, -1);
    }

    //
    // Data setting
    //

    /**
     * Set data into node creating node if it doesn't yet exist. Does not set watch.
     *
     * @param zkw   zk reference
     * @param znode path of node
     * @param data  data to set for node
     */
    public static void createSetData(final ZooKeeperWatcher zkw, final String znode, final byte[] data)
            throws KeeperException {
        if (checkExists(zkw, znode) == -1) {
            createWithParents(zkw, znode);
        }
        setData(zkw, znode, data);
    }

    public static boolean isSecureZooKeeper() {
        return (System.getProperty("java.security.auth.login.config") != null
                && System.getProperty("zookeeper.sasl.clientconfig") != null);
    }

    private static ArrayList<ACL> createACL(ZooKeeperWatcher zkw, String node) {
        if (isSecureZooKeeper()) {
            if (node.equals(zkw.getBaseZNode())) {
                return ZooKeeperWatcher.CREATOR_ALL_AND_WORLD_READABLE;
            }
            return Ids.CREATOR_ALL_ACL;
        } else {
            return Ids.OPEN_ACL_UNSAFE;
        }
    }

    public static void waitForZKConnectionIfAuthenticating(ZooKeeperWatcher zkw)
            throws InterruptedException {
        if (isSecureZooKeeper()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Waiting for ZooKeeperWatcher to authenticate");
            }
            zkw.saslLatchAwait();
            if (logger.isDebugEnabled()) {
                logger.debug("Done waiting.");
            }
        }
    }


    /**
     * Creates the specified node with the specified data and watches it.
     * <p/>
     * <p/>
     * Throws an exception if the node already exists.
     * <p/>
     * <p/>
     * The node created is persistent and open access.
     * <p/>
     * <p/>
     * Returns the version number of the created node if successful.
     *
     * @param zkw   zk reference
     * @param znode path of node to create
     * @param data  data of node to create
     * @return version of node created
     * @throws KeeperException                     if unexpected zookeeper exception
     * @throws KeeperException.NodeExistsException if node already exists
     */
    public static int createAndWatch(ZooKeeperWatcher zkw, String znode, byte[] data)
            throws KeeperException, KeeperException.NodeExistsException {
        try {
            waitForZKConnectionIfAuthenticating(zkw);
            zkw.getRecoverableZooKeeper().create(znode, data, createACL(zkw, znode),
                    CreateMode.PERSISTENT);
            Stat stat = zkw.getRecoverableZooKeeper().exists(znode, zkw);
            if (stat == null) {
                return -1;
            }
            return stat.getVersion();
        } catch (InterruptedException e) {
            zkw.interruptedException(e);
            return -1;
        }
    }


    /**
     * Creates the specified node, if the node does not exist. Does not set a watch and fails
     * silently if the node already exists.
     * <p/>
     * The node created is persistent and open access.
     *
     * @param zkw   zk reference
     * @param znode path of node
     * @throws KeeperException if unexpected zookeeper exception
     */
    public static void createAndFailSilent(ZooKeeperWatcher zkw, String znode) throws KeeperException {
        try {
            RecoverableZooKeeper zk = zkw.getRecoverableZooKeeper();
            waitForZKConnectionIfAuthenticating(zkw);
            if (zk.exists(znode, false) == null) {
                zk.create(znode, new byte[0], createACL(zkw, znode), CreateMode.PERSISTENT);
            }
        } catch (KeeperException.NodeExistsException nee) {
            //
        } catch (KeeperException.NoAuthException nee) {
            try {
                if (null == zkw.getRecoverableZooKeeper().exists(znode, false)) {
                    // If we failed to create the file and it does not already
                    // exist.
                    throw (nee);
                }
            } catch (InterruptedException ie) {
                zkw.interruptedException(ie);
            }

        } catch (InterruptedException ie) {
            zkw.interruptedException(ie);
        }
    }

    /**
     * Creates the specified node and all parent nodes required for it to exist.
     * <p/>
     * No watches are set and no errors are thrown if the node already exists.
     * <p/>
     * The nodes created are persistent and open access.
     *
     * @param zkw   zk reference
     * @param znode path of node
     * @throws KeeperException if unexpected zookeeper exception
     */
    public static void createWithParents(ZooKeeperWatcher zkw, String znode) throws KeeperException {
        try {
            if (znode == null) {
                return;
            }
            waitForZKConnectionIfAuthenticating(zkw);
            zkw.getRecoverableZooKeeper().create(znode, new byte[0], createACL(zkw, znode),
                    CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException nee) {
            return;
        } catch (KeeperException.NoNodeException nne) {
            createWithParents(zkw, getParent(znode));
            createWithParents(zkw, znode);
        } catch (InterruptedException ie) {
            zkw.interruptedException(ie);
        }
    }


    private static void logRetrievedMsg(final ZooKeeperWatcher zkw, final String znode,
                                        final byte[] data, final boolean watcherSet) {
        if (!logger.isDebugEnabled()) {
            return;
        }
        logger.debug(zkw.prefix("Retrieved " + ((data == null) ? 0 : data.length)
                + " byte(s) of data from znode " + znode + (watcherSet ? " and set watcher; " : "; data=")
                + (data == null ? "null" : data.length == 0 ? "empty" : new String(data))));
    }


    /**
     * Create a persistent node.
     *
     * @param createParents if true all parent dirs are created as well and no {@link
     *                      NodeExistsException} is thrown in case the path already exists
     * @throws InterruptedException     if operation was interrupted, or a required reconnection got
     *                                  interrupted
     * @throws IllegalArgumentException if called from anything except the ZooKeeper event thread
     * @throws KeeperException          if any ZooKeeper exception occurred
     * @throws RuntimeException         if any other exception occurs
     */
    public static void createPersistent(ZooKeeperWatcher zkw, String path, boolean createParents)
            throws KeeperException {
        try {
            ZKUtil.createAndWatch(zkw, path, null);
        } catch (NodeExistsException e) {
            if (!createParents) {
                throw e;
            }
        } catch (NoNodeException e) {
            if (!createParents) {
                throw e;
            }
            String parentDir = path.substring(0, path.lastIndexOf('/'));
            createPersistent(zkw, parentDir, createParents);
            createPersistent(zkw, path, createParents);
        }
    }

    // TODO: Double check the replacement
  /*---------------------------------------------------------*/
  /*---------------------------------------------------------*/
  /* Following APIs added by Denny */
  /* The APIs are nearly compatible with old tube */
  /*---------------------------------------------------------*/
  /*---------------------------------------------------------*/


    /**
     * create the parent path
     */
    public static void createParentPath(final ZooKeeperWatcher zkw, final String path)
            throws Exception {
        final String parentDir = path.substring(0, path.lastIndexOf('/'));
        if (parentDir.length() != 0) {
            ZKUtil.createPersistent(zkw, parentDir, true);
        }
    }

    /**
     * Update the value of a persistent node with the given path and data. create parent directory
     * if necessary. Never throw NodeExistException.
     */
    public static void updatePersistentPath(final ZooKeeperWatcher zkw, final String path,
                                            final String data) throws Exception {
        byte[] bytes = (data == null ? null : StringUtils.getBytesUtf8(data));
        try {
            ZKUtil.setData(zkw, path, bytes);
        } catch (final NoNodeException e) {
            createParentPath(zkw, path);
            ZKUtil.createAndWatch(zkw, path, bytes);
        } catch (final Exception e) {
            throw e;
        }
    }

    public static String readData(final ZooKeeperWatcher zkw, final String path)
            throws KeeperException {
        byte[] bytes = ZKUtil.getDataAndWatch(zkw, path);
        if (bytes == null) {
            return null;
        }
        try {
            return new String(bytes, TBaseConstants.META_DEFAULT_CHARSET_NAME);
        } catch (Throwable e) {
            logger.error("readData from " + path + " error! bytes is " + new String(bytes), e);
        }
        return null;
    }

    public static String readDataMaybeNull(final ZooKeeperWatcher zkw, final String path)
            throws KeeperException {
        try {
            return readData(zkw, path);
        } catch (NoNodeException e) {
            return null;
        }
    }

}

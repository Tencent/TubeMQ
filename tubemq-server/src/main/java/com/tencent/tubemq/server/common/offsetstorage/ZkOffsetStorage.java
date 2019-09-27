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

package com.tencent.tubemq.server.common.offsetstorage;

import com.tencent.tubemq.corebase.TokenConstants;
import com.tencent.tubemq.server.broker.exception.OffsetStoreException;
import com.tencent.tubemq.server.common.TServerConstants;
import com.tencent.tubemq.server.common.fileconfig.ZKConfig;
import com.tencent.tubemq.server.common.offsetstorage.zookeeper.ZKUtil;
import com.tencent.tubemq.server.common.offsetstorage.zookeeper.ZooKeeperWatcher;
import java.util.Collection;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A offset storage implementation with zookeeper
 */
public class ZkOffsetStorage implements OffsetStorage {
    private static final Logger logger = LoggerFactory.getLogger(ZkOffsetStorage.class);

    static {
        if (Thread.getDefaultUncaughtExceptionHandler() == null) {
            Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    if (e instanceof IllegalStateException
                            && e.getMessage().contains("Shutdown in progress")) {
                        return;
                    }
                    logger.warn("Thread terminated with exception: " + t.getName(), e);
                }
            });
        }
    }

    private final String tubeZkRoot;
    private final String consumerZkDir;
    private ZKConfig zkConfig;
    private ZooKeeperWatcher zkw;

    /**
     * Constructor of ZkOffsetStorage
     *
     * @param zkConfig the zookeeper configuration
     */
    public ZkOffsetStorage(final ZKConfig zkConfig) {
        this.zkConfig = zkConfig;
        this.tubeZkRoot = normalize(this.zkConfig.getZkNodeRoot());
        this.consumerZkDir = this.tubeZkRoot + "/consumers-v3";
        try {
            this.zkw = new ZooKeeperWatcher(zkConfig);
        } catch (Throwable e) {
            logger.error(new StringBuilder(256)
                    .append("Failed to connect ZooKeeper server (")
                    .append(this.zkConfig.getZkServerAddr()).append(") !").toString(), e);
            System.exit(1);
        }
        logger.info("ZooKeeper Offset Storage initiated!");
    }

    @Override
    public void close() {
        if (this.zkw != null) {
            logger.info("ZooKeeper Offset Storage closing .......");
            this.zkw.close();
            this.zkw = null;
            logger.info("ZooKeeper Offset Storage closed!");
        }
    }

    @Override
    public void commitOffset(final String group,
                             final Collection<OffsetStorageInfo> offsetInfoList,
                             boolean isFailRetry) {
        if (this.zkw == null
                || offsetInfoList == null
                || offsetInfoList.isEmpty()) {
            return;
        }
        StringBuilder sBuilder = new StringBuilder(512);
        if (isFailRetry) {
            for (int i = 0; i < TServerConstants.CFG_ZK_COMMIT_DEFAULT_RETRIES; i++) {
                try {
                    cfmOffset(sBuilder, group, offsetInfoList);
                    break;
                } catch (Exception e) {
                    logger.error("Error found when commit offsets to ZooKeeper with retry " + i, e);
                    try {
                        Thread.sleep(this.zkConfig.getZkSyncTimeMs());
                    } catch (InterruptedException ie) {
                        logger.error(
                                "InterruptedException when commit offset to ZooKeeper with retry " + i, ie);
                        return;
                    }
                }
            }
        } else {
            try {
                cfmOffset(sBuilder, group, offsetInfoList);
            } catch (OffsetStoreException e) {
                logger.error("Error when commit offsets to ZooKeeper", e);
            }
        }
    }

    @Override
    public OffsetStorageInfo loadOffset(final String group, final String topic, int brokerId, int partitionId) {
        String znode = new StringBuilder(512).append(this.consumerZkDir).append("/")
                .append(group).append("/offsets/").append(topic).append("/")
                .append(brokerId).append(TokenConstants.HYPHEN).append(partitionId).toString();
        String offsetZkInfo;
        try {
            offsetZkInfo = ZKUtil.readDataMaybeNull(this.zkw, znode);
        } catch (KeeperException e) {
            logger.error("KeeperException during load offsets from ZooKeeper", e);
            return null;
        }
        if (offsetZkInfo == null) {
            return null;
        }
        String[] offsetInfoStrs =
                offsetZkInfo.split(TokenConstants.HYPHEN);
        return new OffsetStorageInfo(topic, brokerId, partitionId,
                Long.parseLong(offsetInfoStrs[1]), Long.parseLong(offsetInfoStrs[0]), false);

    }

    private void cfmOffset(final StringBuilder sb, final String group,
                           final Collection<OffsetStorageInfo> infoList) throws OffsetStoreException {
        sb.delete(0, sb.length());
        for (final OffsetStorageInfo info : infoList) {
            long newOffset = -1;
            long msgId = -1;
            synchronized (info) {
                if (!info.isModified()) {
                    continue;
                }
                newOffset = info.getOffset();
                msgId = info.getMessageId();
                info.setModified(false);
            }
            final String topic = info.getTopic();
            String offsetPath = sb.append(this.consumerZkDir).append("/")
                    .append(group).append("/offsets/").append(topic).append("/")
                    .append(info.getBrokerId()).append(TokenConstants.HYPHEN)
                    .append(info.getPartitionId()).toString();
            sb.delete(0, sb.length());
            String offsetData =
                    sb.append(msgId).append(TokenConstants.HYPHEN).append(newOffset).toString();
            sb.delete(0, sb.length());
            try {
                ZKUtil.updatePersistentPath(this.zkw, offsetPath, offsetData);
            } catch (final Throwable t) {
                logger.error("Exception during commit offsets to ZooKeeper", t);
                throw new OffsetStoreException(t);
            }
            if (logger.isDebugEnabled()) {
                logger.debug(sb.append("Committed offset, path=")
                        .append(offsetPath).append(", data=").append(offsetData).toString());
                sb.delete(0, sb.length());
            }
        }
    }

    private String normalize(final String root) {
        if (root.startsWith("/")) {
            return this.removeLastSlash(root);
        } else {
            return "/" + this.removeLastSlash(root);
        }
    }

    private String removeLastSlash(final String root) {
        if (root.endsWith("/")) {
            return root.substring(0, root.lastIndexOf("/"));
        } else {
            return root;
        }
    }

}

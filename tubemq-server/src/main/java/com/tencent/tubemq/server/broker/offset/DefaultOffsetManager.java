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

package com.tencent.tubemq.server.broker.offset;

import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.daemon.AbstractDaemonService;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.server.broker.BrokerConfig;
import com.tencent.tubemq.server.broker.msgstore.MessageStore;
import com.tencent.tubemq.server.broker.utils.DataStoreUtils;
import com.tencent.tubemq.server.common.offsetstorage.OffsetStorage;
import com.tencent.tubemq.server.common.offsetstorage.OffsetStorageInfo;
import com.tencent.tubemq.server.common.offsetstorage.ZkOffsetStorage;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Default offset manager.
 * Conduct consumer's commit offset operation and consumer's offset that has consumed but not committed.
 */
public class DefaultOffsetManager extends AbstractDaemonService implements OffsetService {
    private static final Logger logger = LoggerFactory.getLogger(DefaultOffsetManager.class);
    private final BrokerConfig brokerConfig;
    private final OffsetStorage zkOffsetStorage;
    private final ConcurrentHashMap<String/* group */,
            ConcurrentHashMap<String/* topic - partitionId*/, OffsetStorageInfo>> cfmOffsetMap =
            new ConcurrentHashMap<String, ConcurrentHashMap<String/* topic */, OffsetStorageInfo>>();
    private final ConcurrentHashMap<String/* group */,
            ConcurrentHashMap<String/* topic - partitionId*/, Long>> tmpOffsetMap =
            new ConcurrentHashMap<String, ConcurrentHashMap<String, Long>>();

    public DefaultOffsetManager(final BrokerConfig brokerConfig) {
        super("[Offset Manager]", brokerConfig.getZkConfig().getZkCommitPeriodMs());
        this.brokerConfig = brokerConfig;
        zkOffsetStorage = new ZkOffsetStorage(brokerConfig.getZkConfig());
        super.start();
    }


    @Override
    protected void loopProcess(long intervalMs) {
        while (!super.isStopped()) {
            try {
                Thread.sleep(intervalMs);
                commitCfmOffsets(false);
            } catch (InterruptedException e) {
                logger.warn("[Offset Manager] Daemon commit thread has been interrupted");
                return;
            } catch (Throwable t) {
                logger.error("[Offset Manager] Daemon commit thread throw error ", t);
            }
        }
    }

    @Override
    public void close(long waitTimeMs) {
        if (super.stop()) {
            return;
        }
        logger.info("[Offset Manager] begin reserve temporary Offset.....");
        this.commitTmpOffsets();
        logger.info("[Offset Manager] begin reserve final Offset.....");
        this.commitCfmOffsets(true);
        this.zkOffsetStorage.close();
        logger.info("[Offset Manager] Offset Manager service stopped!");
    }

    /***
     * Load offset.
     *
     * @param msgStore
     * @param group
     * @param topic
     * @param partitionId
     * @param readStatus
     * @param reqOffset
     * @param sBuilder
     * @return
     */
    @Override
    public OffsetStorageInfo loadOffset(final MessageStore msgStore, final String group,
                                        final String topic, int partitionId, int readStatus,
                                        long reqOffset, final StringBuilder sBuilder) {
        OffsetStorageInfo regInfo;
        long indexMaxOffset = msgStore.getIndexMaxOffset();
        long indexMinOffset = msgStore.getIndexMinOffset();
        long defOffset =
                (readStatus == TBaseConstants.CONSUME_MODEL_READ_NORMAL)
                        ? indexMinOffset : indexMaxOffset;
        String offsetCacheKey = getOffsetCacheKey(topic, partitionId);
        regInfo = loadOrCreateOffset(group, topic, partitionId, offsetCacheKey, defOffset);
        getAndResetTmpOffset(group, offsetCacheKey);
        final long curOffset = regInfo.getOffset();
        final boolean isFirstCreate = regInfo.isFirstCreate();
        if ((reqOffset >= 0)
                || (readStatus == TBaseConstants.CONSUME_MODEL_READ_FROM_MAX_ALWAYS)) {
            long adjOffset = indexMaxOffset;
            if (readStatus != TBaseConstants.CONSUME_MODEL_READ_FROM_MAX_ALWAYS) {
                adjOffset = reqOffset > indexMaxOffset ? indexMaxOffset : reqOffset;
                adjOffset = adjOffset < indexMinOffset ? indexMinOffset : adjOffset;
            }
            regInfo.getAndSetOffset(adjOffset);
        }
        sBuilder.append("[Offset Manager]");
        switch (readStatus) {
            case TBaseConstants.CONSUME_MODEL_READ_FROM_MAX_ALWAYS:
                sBuilder.append(" Consume From Max Offset Always");
                break;
            case TBaseConstants.CONSUME_MODEL_READ_FROM_MAX:
                sBuilder.append(" Consume From Max Offset");
                break;
            default: {
                sBuilder.append(" Normal Offset");
            }
        }
        if (!isFirstCreate) {
            sBuilder.append(",Continue");
        }
        logger.info(sBuilder.append(",loaded offset=").append(curOffset)
                .append(",required offset=").append(reqOffset)
                .append(",current offset=").append(regInfo.getOffset())
                .append(",maxOffset=").append(indexMaxOffset)
                .append(",offset delta=").append(indexMaxOffset - regInfo.getOffset())
                .append(",group=").append(group).append(",topic=").append(topic)
                .append(",partitionId=").append(partitionId).toString());
        sBuilder.delete(0, sBuilder.length());
        return regInfo;
    }

    /***
     * Get offset by parameters.
     *
     * @param msgStore
     * @param group
     * @param topic
     * @param partitionId
     * @param isManCommit
     * @param lastConsumed
     * @param sb
     * @return
     */
    @Override
    public long getOffset(final MessageStore msgStore, final String group,
                          final String topic, int partitionId,
                          boolean isManCommit, boolean lastConsumed,
                          final StringBuilder sb) {
        String offsetCacheKey = getOffsetCacheKey(topic, partitionId);
        OffsetStorageInfo regInfo =
                loadOrCreateOffset(group, topic, partitionId, offsetCacheKey, 0);
        long requestOffset = regInfo.getOffset();
        if (isManCommit) {
            requestOffset = requestOffset + getTmpOffset(group, topic, partitionId);
        } else {
            if (lastConsumed) {
                requestOffset = commitOffset(group, topic, partitionId, true);
            }
        }
        final long maxOffset = msgStore.getIndexMaxOffset();
        final long minOffset = msgStore.getIndexMinOffset();
        if (requestOffset >= maxOffset) {
            if (requestOffset > maxOffset && brokerConfig.isUpdateConsumerOffsets()) {
                logger.warn(sb
                        .append("[Offset Manager] Offset is bigger than current max offset, reset! requestOffset=")
                        .append(requestOffset).append(",maxOffset=").append(maxOffset)
                        .append(",group=").append(group).append(",topic=").append(topic)
                        .append(",partitionId=").append(partitionId).toString());
                sb.delete(0, sb.length());
                setTmpOffset(group, offsetCacheKey, maxOffset - requestOffset);
                if (!isManCommit) {
                    requestOffset = commitOffset(group, topic, partitionId, true);
                }
            }
            return -requestOffset;
        } else if (requestOffset < minOffset) {
            logger.warn(sb
                    .append("[Offset Manager] Offset is lower than current min offset, reset! requestOffset=")
                    .append(requestOffset).append(",minOffset=").append(minOffset)
                    .append(",group=").append(group).append(",topic=").append(topic)
                    .append(",partitionId=").append(partitionId).toString());
            sb.delete(0, sb.length());
            setTmpOffset(group, offsetCacheKey, minOffset - requestOffset);
            requestOffset = commitOffset(group, topic, partitionId, true);
        }
        return requestOffset;
    }

    @Override
    public long getOffset(final String group, final String topic, int partitionId) {
        OffsetStorageInfo regInfo =
                loadOrCreateOffset(group, topic, partitionId,
                        getOffsetCacheKey(topic, partitionId), 0);
        return regInfo.getOffset();
    }

    @Override
    public void bookOffset(final String group, final String topic, int partitionId,
                           int readDalt, boolean isManCommit, boolean isMsgEmpty,
                           final StringBuilder sb) {
        if (readDalt == 0) {
            return;
        }
        String offsetCacheKey = getOffsetCacheKey(topic, partitionId);
        if (isManCommit) {
            long tmpOffset = getTmpOffset(group, topic, partitionId);
            setTmpOffset(group, offsetCacheKey, readDalt + tmpOffset);
        } else {
            setTmpOffset(group, offsetCacheKey, readDalt);
            if (isMsgEmpty) {
                commitOffset(group, topic, partitionId, true);
            }
        }
    }

    /***
     * Commit offset.
     *
     * @param group
     * @param topic
     * @param partitionId
     * @param isConsumed
     * @return
     */
    @Override
    public long commitOffset(final String group, final String topic,
                             int partitionId, boolean isConsumed) {
        long updatedOffset = -1;
        String offsetCacheKey = getOffsetCacheKey(topic, partitionId);
        long tmpOffset = getAndResetTmpOffset(group, offsetCacheKey);
        if (!isConsumed) {
            tmpOffset = 0;
        }
        OffsetStorageInfo regInfo =
                loadOrCreateOffset(group, topic, partitionId, offsetCacheKey, 0);
        if ((tmpOffset == 0) && (!regInfo.isFirstCreate())) {
            updatedOffset = regInfo.getOffset();
            return updatedOffset;
        }
        updatedOffset = regInfo.addAndGetOffset(tmpOffset);
        if (logger.isDebugEnabled()) {
            logger.debug(new StringBuilder(512)
                    .append("[Offset Manager] Update offset finished, offset=").append(updatedOffset)
                    .append(",group=").append(group).append(",topic=").append(topic)
                    .append(",partitionId=").append(partitionId).toString());
        }
        return updatedOffset;
    }

    /***
     * Reset offset.
     *
     * @param store
     * @param group
     * @param topic
     * @param partitionId
     * @param reSetOffset
     * @param modifyer
     * @return
     */
    @Override
    public long resetOffset(final MessageStore store, final String group,
                            final String topic, int partitionId,
                            long reSetOffset, final String modifyer) {
        long oldOffset = -1;
        if (store != null) {
            long firstOffset = store.getIndexMinOffset();
            long lastOffset = store.getIndexMaxOffset();
            reSetOffset = reSetOffset < firstOffset
                    ? firstOffset : reSetOffset > lastOffset ? lastOffset : reSetOffset;
            String offsetCacheKey = getOffsetCacheKey(topic, partitionId);
            getAndResetTmpOffset(group, offsetCacheKey);
            OffsetStorageInfo regInfo =
                    loadOrCreateOffset(group, topic, partitionId, offsetCacheKey, 0);
            oldOffset = regInfo.getAndSetOffset(reSetOffset);
            logger.info(new StringBuilder(512)
                    .append("[Offset Manager] Manual update offset by modifyer=")
                    .append(modifyer).append(",reset offset=").append(reSetOffset)
                    .append(",old offset=").append(oldOffset)
                    .append(",updated offset=").append(regInfo.getOffset())
                    .append(",group=").append(group).append(",topic=").append(topic)
                    .append(",partitionId=").append(partitionId).toString());
        }
        return oldOffset;
    }

    /***
     * Get temp offset.
     *
     * @param group
     * @param topic
     * @param partitionId
     * @return
     */
    @Override
    public long getTmpOffset(final String group, final String topic, int partitionId) {
        String offsetCacheKey = getOffsetCacheKey(topic, partitionId);
        ConcurrentHashMap<String, Long> partTmpOffsetMap = tmpOffsetMap.get(group);
        if (partTmpOffsetMap != null) {
            Long tmpOffset = partTmpOffsetMap.get(offsetCacheKey);
            if (tmpOffset == null) {
                return 0;
            } else {
                return tmpOffset - tmpOffset % DataStoreUtils.STORE_INDEX_HEAD_LEN;
            }
        }
        return 0;
    }

    /***
     * Set temp offset.
     *
     * @param group
     * @param offsetCacheKey
     * @param origOffset
     * @return
     */
    private long setTmpOffset(final String group, final String offsetCacheKey, long origOffset) {
        long tmpOffset = origOffset - origOffset % DataStoreUtils.STORE_INDEX_HEAD_LEN;
        ConcurrentHashMap<String, Long> partTmpOffsetMap = tmpOffsetMap.get(group);
        if (partTmpOffsetMap == null) {
            ConcurrentHashMap<String, Long> tmpMap = new ConcurrentHashMap<String, Long>();
            partTmpOffsetMap = tmpOffsetMap.putIfAbsent(group, tmpMap);
            if (partTmpOffsetMap == null) {
                partTmpOffsetMap = tmpMap;
            }
        }
        Long befOffset = partTmpOffsetMap.put(offsetCacheKey, tmpOffset);
        if (befOffset == null) {
            return 0;
        } else {
            return (befOffset - befOffset % DataStoreUtils.STORE_INDEX_HEAD_LEN);
        }
    }

    private long getAndResetTmpOffset(final String group, final String offsetCacheKey) {
        ConcurrentHashMap<String, Long> partTmpOffsetMap = tmpOffsetMap.get(group);
        if (partTmpOffsetMap == null) {
            ConcurrentHashMap<String, Long> tmpMap = new ConcurrentHashMap<String, Long>();
            partTmpOffsetMap = tmpOffsetMap.putIfAbsent(group, tmpMap);
            if (partTmpOffsetMap == null) {
                partTmpOffsetMap = tmpMap;
            }
        }
        Long tmpOffset = partTmpOffsetMap.put(offsetCacheKey, 0L);
        if (tmpOffset == null) {
            return 0;
        } else {
            return (tmpOffset - tmpOffset % DataStoreUtils.STORE_INDEX_HEAD_LEN);
        }
    }

    /***
     * Commit temp offsets.
     */
    private void commitTmpOffsets() {
        for (Map.Entry<String, ConcurrentHashMap<String, Long>> entry : tmpOffsetMap.entrySet()) {
            if (TStringUtils.isBlank(entry.getKey())
                    || entry.getValue() == null || entry.getValue().isEmpty()) {
                continue;
            }
            for (Map.Entry<String, Long> topicEntry : entry.getValue().entrySet()) {
                if (TStringUtils.isBlank(topicEntry.getKey())) {
                    continue;
                }
                String[] topicPartStrs = topicEntry.getKey().split("-");
                String topic = topicPartStrs[0];
                int partitionId = Integer.parseInt(topicPartStrs[1]);
                try {
                    commitOffset(entry.getKey(), topic, partitionId, true);
                } catch (Exception e) {
                    logger.warn("[Offset Manager] Commit tmp offset error!", e);
                }
            }
        }
    }

    private void commitCfmOffsets(boolean retryable) {
        for (Map.Entry<String, ConcurrentHashMap<String, OffsetStorageInfo>> entry : cfmOffsetMap.entrySet()) {
            if (TStringUtils.isBlank(entry.getKey())
                    || entry.getValue() == null || entry.getValue().isEmpty()) {
                continue;
            }
            zkOffsetStorage.commitOffset(entry.getKey(), entry.getValue().values(), retryable);
        }
    }

    /***
     * Load or create offset.
     *
     * @param group
     * @param topic
     * @param partitionId
     * @param offsetCacheKey
     * @param defOffset
     * @return
     */
    private OffsetStorageInfo loadOrCreateOffset(final String group, final String topic,
                                                 int partitionId, final String offsetCacheKey,
                                                 long defOffset) {
        ConcurrentHashMap<String, OffsetStorageInfo> regInfoMap = cfmOffsetMap.get(group);
        if (regInfoMap == null) {
            ConcurrentHashMap<String, OffsetStorageInfo> tmpRegInfoMap
                    = new ConcurrentHashMap<String, OffsetStorageInfo>();
            regInfoMap = cfmOffsetMap.putIfAbsent(group, tmpRegInfoMap);
            if (regInfoMap == null) {
                regInfoMap = tmpRegInfoMap;
            }
        }
        OffsetStorageInfo regInfo = regInfoMap.get(offsetCacheKey);
        if (regInfo == null) {
            OffsetStorageInfo tmpRegInfo =
                    zkOffsetStorage.loadOffset(group, topic, brokerConfig.getBrokerId(), partitionId);
            if (tmpRegInfo == null) {
                tmpRegInfo =
                        new OffsetStorageInfo(topic, brokerConfig.getBrokerId(), partitionId, defOffset, 0);
            }
            regInfo = regInfoMap.putIfAbsent(offsetCacheKey, tmpRegInfo);
            if (regInfo == null) {
                regInfo = tmpRegInfo;
            }
        }
        return regInfo;
    }

    private String getOffsetCacheKey(String topic, int partitionId) {
        return new StringBuilder(256).append(topic)
                .append("-").append(partitionId).toString();
    }

}

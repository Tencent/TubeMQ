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

package com.tencent.tubemq.server.master.bdbstore;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.sleepycat.je.rep.NodeState;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationGroup;
import com.sleepycat.je.rep.ReplicationMutableConfig;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;
import com.sleepycat.je.rep.TimeConsistencyPolicy;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.tencent.tubemq.corebase.TokenConstants;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.server.Server;
import com.tencent.tubemq.server.common.fileconfig.BDBConfig;
import com.tencent.tubemq.server.master.MasterConfig;
import com.tencent.tubemq.server.master.TMaster;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbBlackGroupEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbBrokerConfEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbConsumeGroupSettingEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbConsumerGroupEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbGroupFilterCondEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbGroupFlowCtrlEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbTopicAuthControlEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbTopicConfEntity;
import com.tencent.tubemq.server.master.utils.BdbStoreSamplePrint;
import com.tencent.tubemq.server.master.web.model.ClusterGroupVO;
import com.tencent.tubemq.server.master.web.model.ClusterNodeVO;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bdb store service
 * like a local database manager, according to database table name, store instance, primary key, memory cache
 * organize table structure
 */
public class DefaultBdbStoreService implements BdbStoreService, Server {
    private static final Logger logger = LoggerFactory.getLogger(DefaultBdbStoreService.class);

    private static final String BDB_TOPIC_CONFIG_STORE_NAME = "bdbTopicConfig";
    private static final String BDB_BROKER_CONFIG_STORE_NAME = "bdbBrokerConfig";
    private static final String BDB_CONSUMER_GROUP_STORE_NAME = "bdbConsumerGroup";
    private static final String BDB_TOPIC_AUTHCONTROL_STORE_NAME = "bdbTopicAuthControl";
    private static final String BDB_BLACK_GROUP_STORE_NAME = "bdbBlackGroup";
    private static final String BDB_GROUP_FILTER_COND_STORE_NAME = "bdbGroupFilterCond";
    private static final String BDB_GROUP_FLOW_CONTROL_STORE_NAME = "bdbGroupFlowCtrlCfg";
    private static final String BDB_CONSUME_GROUP_SETTING_STORE_NAME = "bdbConsumeGroupSetting";
    private static final int REP_HANDLE_RETRY_MAX = 1;
    private final TMaster tMaster;
    // simple log print
    private final BdbStoreSamplePrint bdbStoreSamplePrint =
            new BdbStoreSamplePrint(logger);
    private Set<String> replicas4Transfer = new HashSet<String>();
    private String masterNodeName;
    private int connectNodeFailCount = 0;
    private long masterStartTime = Long.MAX_VALUE;
    private File envHome;
    private EnvironmentConfig envConfig;
    private ReplicationConfig repConfig;
    private ReplicatedEnvironment repEnv;
    private ReplicationGroupAdmin replicationGroupAdmin;
    private StoreConfig storeConfig = new StoreConfig();
    // broker config store
    private EntityStore brokerConfStore;
    private PrimaryIndex<Integer/* brokerId */, BdbBrokerConfEntity> brokerConfIndex;
    private ConcurrentHashMap<Integer/* brokerId */, BdbBrokerConfEntity> brokerConfigMap =
            new ConcurrentHashMap<Integer, BdbBrokerConfEntity>();
    // topic config store
    private EntityStore topicConfStore;
    private PrimaryIndex<String/* recordKey */, BdbTopicConfEntity> topicConfIndex;
    private ConcurrentHashMap<Integer/* brokerId */, ConcurrentHashMap<String/* topicName */, BdbTopicConfEntity>>
            brokerIdTopicEntityMap = new ConcurrentHashMap<Integer, ConcurrentHashMap<String, BdbTopicConfEntity>>();
    // consumer group store
    private EntityStore consumerGroupStore;
    private PrimaryIndex<String/* recordKey */, BdbConsumerGroupEntity> consumerGroupIndex;
    private ConcurrentHashMap<
            String/* topicName */,
            ConcurrentHashMap<String /* consumerGroup */, BdbConsumerGroupEntity>>
            consumerGroupTopicMap =
            new ConcurrentHashMap<String, ConcurrentHashMap<String, BdbConsumerGroupEntity>>();
    //consumer group black list store
    private EntityStore blackGroupStore;
    private PrimaryIndex<String/* recordKey */, BdbBlackGroupEntity> blackGroupIndex;
    private ConcurrentHashMap<
            String/* consumerGroup */,
            ConcurrentHashMap<String /* topicName */, BdbBlackGroupEntity>>
            blackGroupTopicMap =
            new ConcurrentHashMap<String, ConcurrentHashMap<String, BdbBlackGroupEntity>>();
    // topic auth config store
    private EntityStore topicAuthControlStore;
    private PrimaryIndex<String/* recordKey */, BdbTopicAuthControlEntity> topicAuthControlIndex;
    private ConcurrentHashMap<String/* topicName */, BdbTopicAuthControlEntity> topicAuthControlMap =
            new ConcurrentHashMap<String, BdbTopicAuthControlEntity>();
    // consumer group filter condition store
    private EntityStore groupFilterCondStore;
    private PrimaryIndex<String/* recordKey */, BdbGroupFilterCondEntity> groupFilterCondIndex;
    private ConcurrentHashMap<
            String/* topicName */,
            ConcurrentHashMap<String /* consumerGroup */, BdbGroupFilterCondEntity>>
            groupFilterCondMap =
            new ConcurrentHashMap<String, ConcurrentHashMap<String, BdbGroupFilterCondEntity>>();
    // consumer group flow control store
    private EntityStore groupFlowCtrlStore;
    private PrimaryIndex<String/* groupName */, BdbGroupFlowCtrlEntity> groupFlowCtrlIndex;
    private ConcurrentHashMap<String/* groupName */, BdbGroupFlowCtrlEntity> groupFlowCtrlMap =
            new ConcurrentHashMap<String, BdbGroupFlowCtrlEntity>();
    // consumer group setting store
    private EntityStore consumeGroupSettingStore;
    private PrimaryIndex<String/* recordKey */, BdbConsumeGroupSettingEntity> consumeGroupSettingIndex;
    private ConcurrentHashMap<String/* consumeGroup */, BdbConsumeGroupSettingEntity> consumeGroupSettingMap =
            new ConcurrentHashMap<String, BdbConsumeGroupSettingEntity>();
    // service status, started or stopped
    private AtomicBoolean isStarted = new AtomicBoolean(false);
    private AtomicBoolean isStopped = new AtomicBoolean(false);
    // master role flag
    private boolean isMaster;
    // master node list
    private ConcurrentHashMap<String/* nodeName */, MasterNodeInfo> masterNodeInfoMap =
            new ConcurrentHashMap<String, MasterNodeInfo>();
    private String nodeHost;
    private BDBConfig bdbConfig;
    private Listener listener = new Listener();
    private ExecutorService executorService = null;

    public DefaultBdbStoreService(MasterConfig masterConfig, final TMaster tMaster) {
        this.tMaster = tMaster;
        this.nodeHost = masterConfig.getHostName();
        this.bdbConfig = masterConfig.getBdbConfig();
        Set<InetSocketAddress> helpers = new HashSet<InetSocketAddress>();
        InetSocketAddress helper1 = new InetSocketAddress(this.nodeHost, 9005);
        InetSocketAddress helper2 = new InetSocketAddress(this.nodeHost, 9006);
        InetSocketAddress helper3 = new InetSocketAddress(this.nodeHost, 9007);
        helpers.add(helper1);
        helpers.add(helper2);
        helpers.add(helper3);
        this.replicationGroupAdmin =
                new ReplicationGroupAdmin(this.bdbConfig.getBdbRepGroupName(), helpers);
    }

    public boolean isMaster() {
        return isMaster;
    }

    /**
     * Get current master address
     *
     * @return
     */
    public InetSocketAddress getMasterAddress() {
        ReplicationGroup replicationGroup = null;
        try {
            replicationGroup = repEnv.getGroup();
        } catch (Throwable e) {
            logger.error("[BDB Error] GetMasterGroup info error", e);
            return null;
        }
        if (replicationGroup == null) {
            logger.info("[BDB Error] ReplicationGroup is null...please check the status of the group!");
            return null;
        }
        for (ReplicationNode node : replicationGroup.getNodes()) {
            try {
                NodeState nodeState = replicationGroupAdmin.getNodeState(node, 2000);
                if (nodeState != null) {
                    if (nodeState.getNodeState().isMaster()) {
                        return node.getSocketAddress();
                    }
                }
            } catch (IOException e) {
                logger.error("[BDB Error] Get nodeState IOException error", e);
                continue;
            } catch (ServiceDispatcher.ServiceConnectFailedException e) {
                logger.error("[BDB Error] Get nodeState ServiceConnectFailedException error", e);
                continue;
            } catch (Throwable e2) {
                logger.error("[BDB Error] Get nodeState Throwable error", e2);
                continue;
            }
        }
        return null;
    }

    /**
     * Get bdb config
     *
     * @return
     */
    public BDBConfig getBdbConfig() {
        return this.bdbConfig;
    }

    /**
     * Get group address info
     *
     * @return
     */
    public ClusterGroupVO getGroupAddressStrInfo() {
        ClusterGroupVO clusterGroupVO = new ClusterGroupVO();
        ReplicationGroup replicationGroup = null;
        try {
            clusterGroupVO.setGroupStatus("Abnormal");
            clusterGroupVO.setGroupName(replicationGroupAdmin.getGroupName());
            replicationGroup = repEnv.getGroup();
        } catch (Throwable e) {
            logger.error("[BDB Error] getGroupAddressStrInfo error", e);
            return null;
        }
        if (replicationGroup != null) {
            clusterGroupVO.setPrimaryNodeActived(isPrimaryNodeActived());
            int count = 0;
            boolean hasMaster = false;
            List<ClusterNodeVO> clusterNodeVOList = new ArrayList<ClusterNodeVO>();
            for (ReplicationNode node : replicationGroup.getNodes()) {
                ClusterNodeVO clusterNodeVO = new ClusterNodeVO();
                clusterNodeVO.setHostName(node.getHostName());
                clusterNodeVO.setNodeName(node.getName());
                clusterNodeVO.setPort(node.getPort());
                try {
                    NodeState nodeState = replicationGroupAdmin.getNodeState(node, 2000);
                    if (nodeState != null) {
                        if (nodeState.getNodeState() == ReplicatedEnvironment.State.MASTER) {
                            hasMaster = true;
                        }
                        clusterNodeVO.setNodeStatus(nodeState.getNodeState().toString());
                        clusterNodeVO.setJoinTime(nodeState.getJoinTime());
                    } else {
                        clusterNodeVO.setNodeStatus("Not-found");
                        clusterNodeVO.setJoinTime(0);
                    }
                } catch (IOException e) {
                    clusterNodeVO.setNodeStatus("Error");
                    clusterNodeVO.setJoinTime(0);
                } catch (ServiceDispatcher.ServiceConnectFailedException e) {
                    clusterNodeVO.setNodeStatus("Unconnected");
                    clusterNodeVO.setJoinTime(0);
                }
                clusterNodeVOList.add(clusterNodeVO);
            }
            clusterGroupVO.setNodeData(clusterNodeVOList);
            if (hasMaster) {
                if (isPrimaryNodeActived()) {
                    clusterGroupVO.setGroupStatus("Running-ReadOnly");
                } else {
                    clusterGroupVO.setGroupStatus("Running-ReadWrite");
                }
            }
        }
        return clusterGroupVO;
    }


    @Override
    public void start() throws Exception {
        if (!isStarted.compareAndSet(false, true)) {
            return;
        }
        try {
            if (executorService != null) {
                executorService.shutdownNow();
                executorService = null;
            }
            executorService = Executors.newSingleThreadExecutor();
            initEnvConfig();
            repEnv = getEnvironment(envHome);
            initMetaStore();
            repEnv.setStateChangeListener(listener);
            isStopped.set(false);
        } catch (Throwable ee) {
            logger.error("[BDB Error] start StoreManagerService failure, error", ee);
            return;
        }
        logger.info("[BDB Status] start StoreManagerService success");
    }

    @Override
    public void stop() throws Exception {
        // #lizard forgives
        if (!isStopped.compareAndSet(false, true)) {
            return;
        }
        logger.info("[BDB Status] Stopping StoreManagerService...");
        if (brokerConfStore != null) {
            try {
                brokerConfStore.close();
                brokerConfStore = null;
            } catch (Throwable e) {
                logger.error("[BDB Error] Close brokerConfStore error ", e);
            }
        }
        if (topicConfStore != null) {
            try {
                topicConfStore.close();
                topicConfStore = null;
            } catch (Throwable e) {
                logger.error("[BDB Error] Close topicConfigStore error ", e);
            }
        }
        if (blackGroupStore != null) {
            try {
                blackGroupStore.close();
                blackGroupStore = null;
            } catch (Throwable e) {
                logger.error("[BDB Error] Close blackGroupStore error ", e);
            }
        }
        if (consumeGroupSettingStore != null) {
            try {
                consumeGroupSettingStore.close();
                consumeGroupSettingStore = null;
            } catch (Throwable e) {
                logger.error("[BDB Error] Close consumeGroupSettingStore error ", e);
            }
        }
        if (consumerGroupStore != null) {
            try {
                consumerGroupStore.close();
                consumerGroupStore = null;
            } catch (DatabaseException e) {
                logger.error("[BDB Error] Close consumerGroupStore error ", e);
            }
        }
        if (groupFilterCondStore != null) {
            try {
                groupFilterCondStore.close();
                groupFilterCondStore = null;
            } catch (Throwable e) {
                logger.error("[BDB Error] Close groupFilterCondStore error ", e);
            }
        }
        if (topicAuthControlStore != null) {
            try {
                topicAuthControlStore.close();
                topicAuthControlStore = null;
            } catch (Throwable e) {
                logger.error("[BDB Error] Close topicFlowControlStore error ", e);
            }
        }
        if (groupFlowCtrlStore != null) {
            try {
                groupFlowCtrlStore.close();
                groupFlowCtrlStore = null;
            } catch (Throwable e) {
                logger.error("[BDB Error] Close groupFlowCtrlStore error ", e);
            }
        }
        /* evn close */
        if (repEnv != null) {
            try {
                repEnv.close();
                repEnv = null;
            } catch (Throwable ee) {
                logger.error("[BDB Error] Close repEnv throw error ", ee);
            }
        }
        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
        }
        isStarted.set(false);
        logger.info("[BDB Status] Stopping StoreManagerService successfully...");
    }

    @Override
    public void cleanData() {
        // TODO
    }

    /**
     * Get master start time
     *
     * @return
     */
    public long getMasterStartTime() {
        return masterStartTime;
    }

    /**
     * Get broker config bdb entity
     *
     * @param bdbBrokerConfEntity
     * @param isNew
     * @return
     */
    @Override
    public boolean putBdbBrokerConfEntity(BdbBrokerConfEntity bdbBrokerConfEntity, boolean isNew) {
        BdbBrokerConfEntity result = null;
        try {
            result = brokerConfIndex.put(bdbBrokerConfEntity);
        } catch (Throwable e) {
            logger.error("[BDB Error] PutTopicConf Error ", e);
            return false;
        }
        if (isNew) {
            return result == null;
        }
        return result != null;
    }

    /**
     * Delete broker config bdb entity
     *
     * @param brokerId
     * @return
     */
    @Override
    public boolean delBdbBrokerConfEntity(int brokerId) {
        try {
            brokerConfIndex.delete(brokerId);
        } catch (Throwable e) {
            logger.error("[BDB Error] delBdbBrokerConfEntity Error ", e);
            return false;
        }
        return true;
    }

    /**
     * Get broker config map
     *
     * @return
     */
    @Override
    public ConcurrentHashMap<Integer/* brokerId */, BdbBrokerConfEntity> getBrokerConfigMap() {
        return this.brokerConfigMap;
    }


    /**
     * Put topic config bdb entity
     *
     * @param bdbTopicConfEntity
     * @param isNew
     * @return
     */
    @Override
    public boolean putBdbTopicConfEntity(BdbTopicConfEntity bdbTopicConfEntity, boolean isNew) {
        BdbTopicConfEntity result = null;
        try {
            result = topicConfIndex.put(bdbTopicConfEntity);
        } catch (Throwable e) {
            logger.error("[BDB Error] PutTopicConf Error ", e);
            return false;
        }
        if (isNew) {
            return result == null;
        }
        return result != null;
    }

    /**
     * Delete topic config bdb entity
     *
     * @param recordKey
     * @param topicName
     * @return
     */
    @Override
    public boolean delBdbTopicConfEntity(String recordKey, String topicName) {
        try {
            topicConfIndex.delete(recordKey);
        } catch (Throwable e) {
            logger.error("[BDB Error] delBdbTopicConfEntity Error ", e);
            return false;
        }
        return true;
    }

    /**
     * Get topic entity map
     *
     * @return
     */
    @Override
    public ConcurrentHashMap<Integer, ConcurrentHashMap<String, BdbTopicConfEntity>> getBrokerTopicEntityMap() {
        return this.brokerIdTopicEntityMap;
    }

    /**
     * Get master group info
     *
     * @return
     */
    @Override
    public ConcurrentHashMap<String/* nodeName */, MasterNodeInfo> getMasterGroupNodeInfo() {
        getMasterGroupStatus(false);
        return this.masterNodeInfoMap;
    }

    /**
     * Put topic auth control bdb entity
     *
     * @param bdbTopicAuthControlEntity
     * @param isNew
     * @return
     */
    @Override
    public boolean putBdbTopicAuthControlEntity(BdbTopicAuthControlEntity bdbTopicAuthControlEntity,
                                                boolean isNew) {
        BdbTopicAuthControlEntity result = null;
        try {
            result = topicAuthControlIndex.put(bdbTopicAuthControlEntity);
        } catch (Throwable e) {
            logger.error("[BDB Error] PutTopicAuthControl Error ", e);
            return false;
        }
        if (isNew) {
            return result == null;
        }
        return result != null;
    }

    /**
     * Delete topic auth control bdb entity
     *
     * @param topicName
     * @return
     */
    @Override
    public boolean delBdbTopicAuthControlEntity(String topicName) {
        try {
            topicAuthControlIndex.delete(topicName);
        } catch (Throwable e) {
            logger.error("[BDB Error] delTopicAuthControl Error ", e);
            return false;
        }
        return true;
    }

    /**
     * Get topic auth control map
     *
     * @return
     */
    @Override
    public ConcurrentHashMap<String, BdbTopicAuthControlEntity> getTopicAuthControlMap() {
        return this.topicAuthControlMap;
    }

    /**
     * Put consumer group config bdb entity
     *
     * @param bdbConsumerGroupEntity
     * @param isNew
     * @return
     */
    @Override
    public boolean putBdbConsumerGroupConfEntity(BdbConsumerGroupEntity bdbConsumerGroupEntity,
                                                 boolean isNew) {
        BdbConsumerGroupEntity result = null;
        try {
            result = consumerGroupIndex.put(bdbConsumerGroupEntity);
        } catch (Throwable e) {
            logger.error("[BDB Error] PutConsumerGroup Error ", e);
            return false;
        }
        if (isNew) {
            return result == null;
        }
        return result != null;
    }

    /**
     * Put group filter condition bdb entity
     *
     * @param bdbGroupFilterCondEntity
     * @param isNew
     * @return
     */
    @Override
    public boolean putBdbGroupFilterCondConfEntity(BdbGroupFilterCondEntity bdbGroupFilterCondEntity, boolean isNew) {
        BdbGroupFilterCondEntity result = null;
        try {
            result = groupFilterCondIndex.put(bdbGroupFilterCondEntity);
        } catch (Throwable e) {
            logger.error("[BDB Error] PutGroupFilterCond Error ", e);
            return false;
        }
        if (isNew) {
            return result == null;
        }
        return result != null;
    }

    /**
     * Put group flow control config bdb entity
     *
     * @param bdbGroupFlowCtrlEntity
     * @param isNew
     * @return
     */
    @Override
    public boolean putBdbGroupFlowCtrlConfEntity(BdbGroupFlowCtrlEntity bdbGroupFlowCtrlEntity, boolean isNew) {
        BdbGroupFlowCtrlEntity result = null;
        try {
            result = groupFlowCtrlIndex.put(bdbGroupFlowCtrlEntity);
        } catch (Throwable e) {
            logger.error("[BDB Error] putBdbGroupFlowCtrlConfEntity Error ", e);
            return false;
        }
        if (isNew) {
            return result == null;
        }
        return result != null;
    }

    /**
     * Put black group config bdb entity
     *
     * @param bdbBlackGroupEntity
     * @param isNew
     * @return
     */
    @Override
    public boolean putBdbBlackGroupConfEntity(BdbBlackGroupEntity bdbBlackGroupEntity, boolean isNew) {
        BdbBlackGroupEntity result = null;
        try {
            result = blackGroupIndex.put(bdbBlackGroupEntity);
        } catch (Throwable e) {
            logger.error("[BDB Error] PutBlackGroup Error ", e);
            return false;
        }
        if (isNew) {
            return result == null;
        }
        return result != null;
    }

    /**
     * Put consumer group setting bdb entity
     *
     * @param offsetResetGroupEntity
     * @param isNew
     * @return
     */
    @Override
    public boolean putBdbConsumeGroupSettingEntity(BdbConsumeGroupSettingEntity offsetResetGroupEntity, boolean isNew) {
        BdbConsumeGroupSettingEntity result = null;
        try {
            result = consumeGroupSettingIndex.put(offsetResetGroupEntity);
        } catch (Throwable e) {
            logger.error("[BDB Error] Put ConsumeGroupSetting Error ", e);
            return false;
        }
        if (isNew) {
            return result == null;
        }
        return result != null;
    }

    /**
     * Delete  consumer group bdb entity
     *
     * @param recordKey
     * @return
     */
    @Override
    public boolean delBdbConsumerGroupEntity(String recordKey) {
        try {
            consumerGroupIndex.delete(recordKey);
        } catch (Throwable e) {
            logger.error("[BDB Error] delBdbConsumerGroupEntity Error ", e);
            return false;
        }
        return true;
    }

    /**
     * Delete group filter condition bdb entity
     *
     * @param recordKey
     * @return
     */
    @Override
    public boolean delBdbGroupFilterCondEntity(String recordKey) {
        try {
            groupFilterCondIndex.delete(recordKey);
        } catch (Throwable e) {
            logger.error("[BDB Error] delBdbGroupFilterCondEntity Error ", e);
            return false;
        }
        return true;
    }

    /**
     * Delete group flow control store entity
     *
     * @param groupName
     * @return
     */
    @Override
    public boolean delBdbGroupFlowCtrlStoreEntity(String groupName) {
        try {
            groupFlowCtrlIndex.delete(groupName);
        } catch (Throwable e) {
            logger.error("[BDB Error] delBdbGroupFlowCtrlStoreEntity Error ", e);
            return false;
        }
        return true;
    }

    /**
     * Delete bdb black group entity
     *
     * @param recordKey
     * @return
     */
    @Override
    public boolean delBdbBlackGroupEntity(String recordKey) {
        try {
            blackGroupIndex.delete(recordKey);
        } catch (Throwable e) {
            logger.error("[BDB Error] delBdbBlackGroupEntity Error ", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean delBdbConsumeGroupSettingEntity(String consumeGroupName) {
        try {
            consumeGroupSettingIndex.delete(consumeGroupName);
        } catch (Throwable e) {
            logger.error("[BDB Error] delBdbConsumeGroupSettingEntity Error ", e);
            return false;
        }
        return true;
    }

    @Override
    public ConcurrentHashMap<String,
            ConcurrentHashMap<String, BdbConsumerGroupEntity>> getConsumerGroupNameAccControlMap() {
        return this.consumerGroupTopicMap;
    }

    @Override
    public ConcurrentHashMap<String,
            ConcurrentHashMap<String, BdbBlackGroupEntity>> getBlackGroupNameAccControlMap() {
        return this.blackGroupTopicMap;
    }

    @Override
    public ConcurrentHashMap<String,
            ConcurrentHashMap<String, BdbGroupFilterCondEntity>> getGroupFilterCondAccControlMap() {
        return this.groupFilterCondMap;
    }

    @Override
    public ConcurrentHashMap<String, BdbGroupFlowCtrlEntity> getGroupFlowCtrlMap() {
        return this.groupFlowCtrlMap;
    }

    @Override
    public ConcurrentHashMap<String, BdbConsumeGroupSettingEntity> getConsumeGroupSettingMap() {
        return this.consumeGroupSettingMap;
    }

    /**
     * Get master group status
     *
     * @param isFromHeartbeat
     * @return
     */
    @Override
    public MasterGroupStatus getMasterGroupStatus(boolean isFromHeartbeat) {
        // #lizard forgives
        if (repEnv == null) {
            return null;
        }
        ReplicationGroup replicationGroup = null;
        try {
            replicationGroup = repEnv.getGroup();
        } catch (DatabaseException e) {
            if (e instanceof EnvironmentFailureException) {
                if (isFromHeartbeat) {
                    logger.error("[BDB Error] Check found EnvironmentFailureException", e);
                    try {
                        stop();
                        start();
                        replicationGroup = repEnv.getGroup();
                    } catch (Throwable e1) {
                        logger.error("[BDB Error] close and reopen storeManager error", e1);
                    }
                } else {
                    logger.error(
                            "[BDB Error] Get EnvironmentFailureException error while non heartBeat request", e);
                }
            } else {
                logger.error("[BDB Error] Get replication group info error", e);
            }
        } catch (Throwable ee) {
            logger.error("[BDB Error] Get replication group throw error", ee);
        }
        if (replicationGroup == null) {
            logger.error(
                    "[BDB Error] ReplicationGroup is null...please check the status of the group!");
            return null;
        }
        int activeNodes = 0;
        boolean isMasterActive = false;
        Set<String> tmp = new HashSet<String>();
        for (ReplicationNode node : replicationGroup.getNodes()) {
            MasterNodeInfo masterNodeInfo =
                    new MasterNodeInfo(replicationGroup.getName(),
                            node.getName(), node.getHostName(), node.getPort());
            if (!masterNodeInfoMap.containsKey(masterNodeInfo.getNodeName())) {
                masterNodeInfoMap.put(masterNodeInfo.getNodeName(), masterNodeInfo);
            }
            try {
                NodeState nodeState = replicationGroupAdmin.getNodeState(node, 2000);
                if (nodeState != null) {
                    if (nodeState.getNodeState().isActive()) {
                        activeNodes++;
                        if (nodeState.getNodeName().equals(masterNodeName)) {
                            isMasterActive = true;
                            masterNodeInfo.setNodeStatus(1);
                        }
                    }
                    if (nodeState.getNodeState().isReplica()) {
                        tmp.add(nodeState.getNodeName());
                        replicas4Transfer = tmp;
                        masterNodeInfo.setNodeStatus(0);
                    }
                }
            } catch (IOException e) {
                connectNodeFailCount++;
                masterNodeInfo.setNodeStatus(-1);
                bdbStoreSamplePrint.printExceptionCaught(e, node.getHostName(), node.getName());
                continue;
            } catch (ServiceDispatcher.ServiceConnectFailedException e) {
                masterNodeInfo.setNodeStatus(-2);
                bdbStoreSamplePrint.printExceptionCaught(e, node.getHostName(), node.getName());
                continue;
            } catch (Throwable ee) {
                masterNodeInfo.setNodeStatus(-3);
                bdbStoreSamplePrint.printExceptionCaught(ee, node.getHostName(), node.getName());
                continue;
            }
        }
        MasterGroupStatus masterGroupStatus = new MasterGroupStatus(isMasterActive);
        int groupSize = replicationGroup.getElectableNodes().size();
        int majoritySize = groupSize / 2 + 1;
        if ((activeNodes >= majoritySize) && isMasterActive) {
            masterGroupStatus.setMasterGroupStatus(true, true, true);
            connectNodeFailCount = 0;
            if (isPrimaryNodeActived()) {
                repEnv.setRepMutableConfig(repEnv.getRepMutableConfig().setDesignatedPrimary(false));
            }
        }
        if (groupSize == 2 && connectNodeFailCount >= 3) {
            masterGroupStatus.setMasterGroupStatus(true, false, true);
            if (connectNodeFailCount > 1000) {
                connectNodeFailCount = 3;
            }
            if (!isPrimaryNodeActived()) {
                logger.error("[BDB Error] DesignatedPrimary happened...please check if the other member is down");
                repEnv.setRepMutableConfig(repEnv.getRepMutableConfig().setDesignatedPrimary(true));
            }
        }
        return masterGroupStatus;
    }

    /**
     * Check if primary node is active
     *
     * @return
     */
    @Override
    public boolean isPrimaryNodeActived() {
        if (repEnv == null) {
            return false;
        }
        ReplicationMutableConfig tmpConfig = repEnv.getRepMutableConfig();
        return tmpConfig == null ? false : tmpConfig.getDesignatedPrimary();
    }

    /**
     * Transfer master role to other replica node
     *
     * @throws Exception
     */
    @Override
    public void transferMaster() throws Exception {
        if (!this.isStarted.get()) {
            throw new Exception("The BDB store StoreService is reboot now!");
        }
        if (isMaster()) {
            if (!isPrimaryNodeActived()) {
                if ((replicas4Transfer != null) && (!replicas4Transfer.isEmpty())) {
                    logger.info("start transferMaster to replicas: " + replicas4Transfer);
                    if ((replicas4Transfer != null) && (!replicas4Transfer.isEmpty())) {
                        repEnv.transferMaster(replicas4Transfer, 5, TimeUnit.MINUTES);
                    }
                    logger.info("transferMaster end...");
                } else {
                    throw new Exception("The replicate nodes is empty!");
                }
            } else {
                throw new Exception("DesignatedPrimary happened...please check if the other member is down!");
            }
        } else {
            throw new Exception("Please send your request to the master Node!");
        }
    }

    /* initial metadata */
    private void initMetaStore() {
        brokerConfStore =
                new EntityStore(repEnv, BDB_BROKER_CONFIG_STORE_NAME, storeConfig);
        brokerConfIndex =
                brokerConfStore.getPrimaryIndex(Integer.class, BdbBrokerConfEntity.class);
        topicConfStore =
                new EntityStore(repEnv, BDB_TOPIC_CONFIG_STORE_NAME, storeConfig);
        topicConfIndex =
                topicConfStore.getPrimaryIndex(String.class, BdbTopicConfEntity.class);
        consumerGroupStore =
                new EntityStore(repEnv, BDB_CONSUMER_GROUP_STORE_NAME, storeConfig);
        consumerGroupIndex =
                consumerGroupStore.getPrimaryIndex(String.class, BdbConsumerGroupEntity.class);
        topicAuthControlStore =
                new EntityStore(repEnv, BDB_TOPIC_AUTHCONTROL_STORE_NAME, storeConfig);
        topicAuthControlIndex =
                topicAuthControlStore.getPrimaryIndex(String.class, BdbTopicAuthControlEntity.class);
        blackGroupStore =
                new EntityStore(repEnv, BDB_BLACK_GROUP_STORE_NAME, storeConfig);
        blackGroupIndex =
                blackGroupStore.getPrimaryIndex(String.class, BdbBlackGroupEntity.class);
        groupFilterCondStore =
                new EntityStore(repEnv, BDB_GROUP_FILTER_COND_STORE_NAME, storeConfig);
        groupFilterCondIndex =
                groupFilterCondStore.getPrimaryIndex(String.class, BdbGroupFilterCondEntity.class);
        groupFlowCtrlStore =
                new EntityStore(repEnv, BDB_GROUP_FLOW_CONTROL_STORE_NAME, storeConfig);
        groupFlowCtrlIndex =
                groupFlowCtrlStore.getPrimaryIndex(String.class, BdbGroupFlowCtrlEntity.class);
        consumeGroupSettingStore =
                new EntityStore(repEnv, BDB_CONSUME_GROUP_SETTING_STORE_NAME, storeConfig);
        consumeGroupSettingIndex =
                consumeGroupSettingStore.getPrimaryIndex(String.class, BdbConsumeGroupSettingEntity.class);
    }

    /**
     * Initialize configuration for BDB-JE replication environment.
     *
     * */
    private void initEnvConfig() throws InterruptedException {

        //Set envHome and generate a ReplicationConfig. Note that ReplicationConfig and
        //EnvironmentConfig values could all be specified in the je.properties file, as is shown in the
        //properties file included in the example.
        repConfig = new ReplicationConfig();
        // Set consistency policy for replica.
        TimeConsistencyPolicy consistencyPolicy = new TimeConsistencyPolicy(3, TimeUnit.SECONDS,
                3, TimeUnit.SECONDS);
        repConfig.setConsistencyPolicy(consistencyPolicy);
        // Wait up to 3 seconds for commitConsumed acknowledgments.
        repConfig.setReplicaAckTimeout(3, TimeUnit.SECONDS);
        repConfig.setConfigParam(ReplicationConfig.TXN_ROLLBACK_LIMIT, "1000");
        repConfig.setGroupName(bdbConfig.getBdbRepGroupName());
        repConfig.setNodeName(bdbConfig.getBdbNodeName());
        repConfig.setNodeHostPort(this.nodeHost + TokenConstants.ATTR_SEP
                + bdbConfig.getBdbNodePort());
        if (TStringUtils.isNotEmpty(bdbConfig.getBdbHelperHost())) {
            logger.info("ADD HELP HOST");
            repConfig.setHelperHosts(bdbConfig.getBdbHelperHost());
        }

        //A replicated environment must be opened with transactions enabled. Environments on a master
        //must be read/write, while environments on a client can be read/write or read/only. Since the
        //master's identity may change, it's most convenient to open the environment in the default
        //read/write mode. All write operations will be refused on the client though.
        envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        Durability durability =
                new Durability(bdbConfig.getBdbLocalSync(), bdbConfig.getBdbReplicaSync(),
                        bdbConfig.getBdbReplicaAck());
        envConfig.setDurability(durability);
        envConfig.setAllowCreate(true);

        envHome = new File(bdbConfig.getBdbEnvHome());

        // An Entity Store in a replicated environment must be transactional.
        storeConfig.setTransactional(true);
        // Note that both Master and Replica open the store for write.
        storeConfig.setReadOnly(false);
        storeConfig.setAllowCreate(true);
    }

    /**
     * Creates the replicated environment handle and returns it. It will retry indefinitely if a
     * master could not be established because a sufficient number of nodes were not available, or
     * there were networking issues, etc.
     *
     * @return the newly created replicated environment handle
     * @throws InterruptedException if the operation was interrupted
     */
    private ReplicatedEnvironment getEnvironment(File envHome) throws InterruptedException {
        DatabaseException exception = null;

        //In this example we retry REP_HANDLE_RETRY_MAX times, but a production HA application may
        //retry indefinitely.
        for (int i = 0; i < REP_HANDLE_RETRY_MAX; i++) {
            try {
                return new ReplicatedEnvironment(envHome, repConfig, envConfig);
            } catch (UnknownMasterException unknownMaster) {
                exception = unknownMaster;
                //Indicates there is a group level problem: insufficient nodes for an election, network
                //connectivity issues, etc. Wait and retry to allow the problem to be resolved.
                logger.error("Master could not be established. " + "Exception message:"
                        + unknownMaster.getMessage() + " Will retry after 5 seconds.");
                Thread.sleep(5 * 1000);
                continue;
            } catch (InsufficientLogException insufficientLogEx) {
                logger.error("[Restoring data please wait....] " +
                        "Obtains logger files for a Replica from other members of the replication group. " +
                        "A Replica may need to do so if it has been offline for some time, " +
                        "and has fallen behind in its execution of the replication stream.");
                NetworkRestore restore = new NetworkRestore();
                NetworkRestoreConfig config = new NetworkRestoreConfig();
                // delete obsolete logger files.
                config.setRetainLogFiles(false);
                restore.execute(insufficientLogEx, config);
                // retry
                return new ReplicatedEnvironment(envHome, repConfig, envConfig);
            }
        }
        // Failed despite retries.
        if (exception != null) {
            throw exception;
        }
        // Don't expect to get here.
        throw new IllegalStateException("Failed despite retries");
    }

    private void clearCachedRunData() {
        if (tMaster != null && tMaster.getMasterTopicManage() != null) {
            tMaster.getMasterTopicManage().clearBrokerRunSyncManageData();
        }
    }

    private void loadBrokerConfUnits() throws Exception {
        long count = 0L;
        EntityCursor<BdbBrokerConfEntity> cursor = null;
        logger.info("loadBrokerConfUnits start...");
        try {
            cursor = brokerConfIndex.entities();
            brokerConfigMap.clear();
            StringBuilder sBuilder = logger.isDebugEnabled() ? new StringBuilder(512) : null;
            logger.debug("[loadBrokerConfUnits] Load broker default configure start:");
            for (BdbBrokerConfEntity bdbEntity : cursor) {
                if (bdbEntity == null) {
                    logger.warn("[BDB Error] Found Null data while loading from brokerConfIndex!");
                    continue;
                }
                BdbBrokerConfEntity tmpbdbEntity = brokerConfigMap.get(bdbEntity.getBrokerId());
                if (tmpbdbEntity == null) {
                    brokerConfigMap.put(bdbEntity.getBrokerId(), bdbEntity);
                    if (tMaster != null && tMaster.getMasterTopicManage() != null) {
                        tMaster.getMasterTopicManage().updateBrokerMaps(bdbEntity);
                    }
                }
                count++;
                if (logger.isDebugEnabled()) {
                    logger.debug(bdbEntity.toJsonString(sBuilder).toString());
                    sBuilder.delete(0, sBuilder.length());
                }
            }
            logger.debug("[loadBrokerConfUnits] Load broker default configure finished.");
            logger.info("[loadBrokerConfUnits] total load records are {} ", count);
        } catch (Exception e) {
            logger.error("[loadBrokerConfUnits error] ", e);
            throw e;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        logger.info("loadBrokerConfUnits successfully...");
    }

    private void loadTopicConfUnits() throws Exception {
        long count = 0L;
        EntityCursor<BdbTopicConfEntity> cursor = null;
        logger.info("LoadTopicConfUnits start...");
        try {
            cursor = topicConfIndex.entities();
            brokerIdTopicEntityMap.clear();
            StringBuilder sBuilder = logger.isDebugEnabled() ? new StringBuilder(512) : null;
            for (BdbTopicConfEntity bdbEntity : cursor) {
                if (bdbEntity == null) {
                    logger.warn("[BDB Error] Found Null data while loading from topicConfIndex!");
                    continue;
                }
                ConcurrentHashMap<String/* topicName */, BdbTopicConfEntity> brokerTopicMap =
                        brokerIdTopicEntityMap.get(bdbEntity.getBrokerId());
                if (brokerTopicMap == null) {
                    brokerTopicMap =
                            new ConcurrentHashMap<String, BdbTopicConfEntity>();
                    brokerIdTopicEntityMap.put(bdbEntity.getBrokerId(), brokerTopicMap);
                }
                brokerTopicMap.put(bdbEntity.getTopicName(), bdbEntity);
                count++;
                if (logger.isDebugEnabled()) {
                    logger.debug(bdbEntity.toJsonString(sBuilder).toString());
                    sBuilder.delete(0, sBuilder.length());
                }
            }
            logger.debug("[Load topic config] load broker topic record finished!");
            logger.info("[loadTopicConfUnits] total load records are {}", count);
        } catch (Exception e) {
            logger.error("[loadTopicConfUnits error] ", e);
            throw e;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        logger.info("loadTopicConfUnits successfully...");
    }

    private void loadConsumerGroupUnits() throws Exception {
        long count = 0L;
        EntityCursor<BdbConsumerGroupEntity> cursor = null;
        logger.info("loadConsumerGroupUnits start...");
        try {
            cursor = consumerGroupIndex.entities();
            consumerGroupTopicMap.clear();
            StringBuilder sBuilder = logger.isDebugEnabled() ? new StringBuilder(512) : null;
            logger.debug("[loadConsumerGroupUnits] Load consumer group begin:");
            for (BdbConsumerGroupEntity bdbEntity : cursor) {
                if (bdbEntity == null) {
                    logger.warn("[BDB Error] Found Null data while loading from consumerGroupIndex!");
                    continue;
                }
                String topicName = bdbEntity.getGroupTopicName();
                String consumerGroupName = bdbEntity.getConsumerGroupName();
                ConcurrentHashMap<String/* groupName */, BdbConsumerGroupEntity> consumerGroupMap =
                        consumerGroupTopicMap.get(topicName);
                if (consumerGroupMap == null) {
                    consumerGroupMap =
                            new ConcurrentHashMap<String, BdbConsumerGroupEntity>();
                    consumerGroupTopicMap.put(topicName, consumerGroupMap);
                }
                consumerGroupMap.put(consumerGroupName, bdbEntity);
                count++;
                if (logger.isDebugEnabled()) {
                    logger.debug(bdbEntity.toJsonString(sBuilder).toString());
                    sBuilder.delete(0, sBuilder.length());
                }
            }
            logger.debug("[loadConsumerGroupUnits] Load consumer group finished!");
            logger.info("[loadConsumerGroupUnits] total load records are {}", count);
        } catch (Exception e) {
            logger.error("[loadConsumerGroupUnits error] ", e);
            throw e;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        logger.info("loadConsumerGroupUnits successfully...");
    }

    private void loadGroupFilterCondUnits() throws Exception {
        long count = 0L;
        EntityCursor<BdbGroupFilterCondEntity> cursor = null;
        logger.info("loadGroupFilterCondUnits start...");
        try {
            cursor = groupFilterCondIndex.entities();
            groupFilterCondMap.clear();
            StringBuilder sBuilder = logger.isDebugEnabled() ? new StringBuilder(512) : null;
            logger.debug("[loadGroupFilterCondUnits] Load consumer group start:");
            for (BdbGroupFilterCondEntity bdbEntity : cursor) {
                if (bdbEntity == null) {
                    logger.warn("[BDB Error] Found Null data while loading from groupFilterCondIndex!");
                    continue;
                }
                String topicName = bdbEntity.getTopicName();
                String consumerGroupName = bdbEntity.getConsumerGroupName();
                ConcurrentHashMap<String, BdbGroupFilterCondEntity> filterCondMap =
                        groupFilterCondMap.get(topicName);
                if (filterCondMap == null) {
                    filterCondMap =
                            new ConcurrentHashMap<String, BdbGroupFilterCondEntity>();
                    groupFilterCondMap.put(topicName, filterCondMap);
                }
                filterCondMap.put(consumerGroupName, bdbEntity);
                count++;
                if (logger.isDebugEnabled()) {
                    logger.debug(bdbEntity.toJsonString(sBuilder).toString());
                    sBuilder.delete(0, sBuilder.length());
                }
            }
            logger.debug("[loadGroupFilterCondUnits] Load consumer group finished!");
            logger.info("[loadGroupFilterCondUnits] total load records are {}", count);
        } catch (Exception e) {
            logger.error("[loadGroupFilterCondUnits error] ", e);
            throw e;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        logger.info("loadGroupFilterCondUnits successfully...");
    }

    private void loadGroupFlowCtrlUnits() throws Exception {
        long count = 0L;
        EntityCursor<BdbGroupFlowCtrlEntity> cursor = null;
        logger.info("loadGroupFlowCtrlUnits start...");
        try {
            cursor = groupFlowCtrlIndex.entities();
            groupFlowCtrlMap.clear();
            StringBuilder sBuilder = logger.isDebugEnabled() ? new StringBuilder(512) : null;
            logger.debug("[loadGroupFlowCtrlUnits] Load consumer group start:");
            for (BdbGroupFlowCtrlEntity bdbEntity : cursor) {
                if (bdbEntity == null) {
                    logger.warn("[BDB Error] Found Null data while loading from groupFilterCondIndex!");
                    continue;
                }
                String groupName = bdbEntity.getGroupName();
                groupFlowCtrlMap.put(groupName, bdbEntity);
                count++;
                if (logger.isDebugEnabled()) {
                    logger.debug(bdbEntity.toJsonString(sBuilder).toString());
                    sBuilder.delete(0, sBuilder.length());
                }
            }
            logger.debug("[loadGroupFlowCtrlUnits] Load consumer group finished!");
            logger.info("[loadGroupFlowCtrlUnits] total load records are {}", count);
        } catch (Exception e) {
            logger.error("[loadGroupFlowCtrlUnits error] ", e);
            throw e;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        logger.info("loadGroupFlowCtrlUnits successfully...");
    }

    private void loadBlackGroupUnits() throws Exception {
        long count = 0L;
        EntityCursor<BdbBlackGroupEntity> cursor = null;
        logger.info("loadBlackGroupUnits start...");
        try {
            cursor = blackGroupIndex.entities();
            blackGroupTopicMap.clear();
            StringBuilder sBuilder = logger.isDebugEnabled() ? new StringBuilder(512) : null;
            logger.debug("[loadBlackGroupUnits] Load consumer group start:");
            for (BdbBlackGroupEntity bdbEntity : cursor) {
                if (bdbEntity == null) {
                    logger.warn("[BDB Error] Found Null data while loading from blackGroupIndex!");
                    continue;
                }
                String topicName = bdbEntity.getTopicName();
                String consumerGroupName = bdbEntity.getBlackGroupName();
                ConcurrentHashMap<String/* topicName */, BdbBlackGroupEntity> blackGroupMap =
                        blackGroupTopicMap.get(consumerGroupName);
                if (blackGroupMap == null) {
                    blackGroupMap =
                            new ConcurrentHashMap<String, BdbBlackGroupEntity>();
                    blackGroupTopicMap.put(consumerGroupName, blackGroupMap);
                }
                blackGroupMap.put(topicName, bdbEntity);
                count++;
                if (logger.isDebugEnabled()) {
                    logger.debug(bdbEntity.toJsonString(sBuilder).toString());
                    sBuilder.delete(0, sBuilder.length());
                }
            }
            logger.debug("[loadBlackGroupUnits] Load consumer group finished!");
            logger.info("[loadBlackGroupUnits] total load records are {}", count);
        } catch (Exception e) {
            logger.error("[loadBlackGroupUnits error] ", e);
            throw e;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        logger.info("loadBlackGroupUnits successfully...");
    }

    private void loadTopicAuthControlUnits() throws Exception {
        long count = 0L;
        EntityCursor<BdbTopicAuthControlEntity> cursor = null;
        logger.info("loadTopicAuthControlUnits start...");
        try {
            cursor = topicAuthControlIndex.entities();
            topicAuthControlMap.clear();
            StringBuilder sBuilder = logger.isDebugEnabled() ? new StringBuilder(512) : null;
            logger.debug("[loadTopicAuthControlUnits] Load topic authorized control start:");
            for (BdbTopicAuthControlEntity bdbEntity : cursor) {
                if (bdbEntity == null) {
                    logger.warn("[BDB Error] Found Null data while loading from topicAuthControlIndex!");
                    continue;
                }
                String topicName = bdbEntity.getTopicName();
                BdbTopicAuthControlEntity tmpbdbEntity = topicAuthControlMap.get(topicName);
                if (tmpbdbEntity == null) {
                    topicAuthControlMap.put(topicName, bdbEntity);
                }
                count++;
                if (logger.isDebugEnabled()) {
                    logger.debug(bdbEntity.toJsonString(sBuilder).toString());
                    sBuilder.delete(0, sBuilder.length());
                }
            }
            logger.debug("[loadTopicAuthControlUnits] Load topic authorized control finished!");
            logger.info("[loadTopicAuthControlUnits] total load records are {}", count);
        } catch (Exception e) {
            logger.error("[loadTopicAuthControlUnits error] ", e);
            throw e;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        logger.info("loadTopicAuthControlUnits successfully...");
    }

    private void loadConsumeGroupSettingUnits() throws Exception {
        long count = 0L;
        EntityCursor<BdbConsumeGroupSettingEntity> cursor = null;
        logger.info("loadConsumeGroupSettingUnits start...");
        try {
            cursor = consumeGroupSettingIndex.entities();
            consumeGroupSettingMap.clear();
            StringBuilder sBuilder = logger.isDebugEnabled() ? new StringBuilder(512) : null;
            logger.debug("[loadConsumeGroupSettingUnits] Load consumer group begin:");
            for (BdbConsumeGroupSettingEntity bdbEntity : cursor) {
                if (bdbEntity == null) {
                    logger.warn("[BDB Error] Found Null data while loading from offsetResetGroupIndex!");
                    continue;
                }
                consumeGroupSettingMap.put(bdbEntity.getConsumeGroupName(), bdbEntity);
                count++;
                if (logger.isDebugEnabled()) {
                    logger.debug(bdbEntity.toJsonString(sBuilder).toString());
                    sBuilder.delete(0, sBuilder.length());
                }
            }
            logger.debug("[loadConsumeGroupSettingUnits] Load consumer group finished!");
            logger.info("[loadConsumeGroupSettingUnits] total load records are {}", count);
        } catch (Exception e) {
            logger.error("[loadConsumeGroupSettingUnits error] ", e);
            throw e;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        logger.info("loadConsumeGroupSettingUnits successfully...");
    }

    public class Listener implements StateChangeListener {
        @Override
        public void stateChange(StateChangeEvent stateChangeEvent) throws RuntimeException {
            if (repConfig != null) {
                logger.warn("[" + repConfig.getGroupName()
                        + "Receive a group status changed event]...stateChangeEventTime:"
                        + stateChangeEvent.getEventTime());
            }
            doWork(stateChangeEvent);
        }

        public void doWork(final StateChangeEvent stateChangeEvent) {

            final String currentNode = new StringBuilder(512)
                    .append("GroupName:").append(repConfig.getGroupName())
                    .append(",nodeName:").append(repConfig.getNodeName())
                    .append(",hostName:").append(repConfig.getNodeHostPort()).toString();
            if (executorService == null) {
                logger.error("[BDB Error] Found  executorService is null while doWork!");
                return;
            }
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    StringBuilder sBuilder = new StringBuilder(512);
                    switch (stateChangeEvent.getState()) {
                        case MASTER:
                            if (!isMaster) {
                                try {
                                    clearCachedRunData();
                                    loadBrokerConfUnits();
                                    loadTopicConfUnits();
                                    loadGroupFlowCtrlUnits();
                                    loadGroupFilterCondUnits();
                                    loadConsumerGroupUnits();
                                    loadTopicAuthControlUnits();
                                    loadBlackGroupUnits();
                                    loadConsumeGroupSettingUnits();
                                    isMaster = true;
                                    masterStartTime = System.currentTimeMillis();
                                    masterNodeName = stateChangeEvent.getMasterNodeName();
                                    logger.info(sBuilder.append("[BDB Status] ")
                                            .append(currentNode)
                                            .append(" is a master.").toString());
                                } catch (Throwable e) {
                                    isMaster = false;
                                    logger.error("[BDB Error] Fatal error when Reloading Info ", e);
                                }
                            }
                            break;
                        case REPLICA:
                            isMaster = false;
                            masterNodeName = stateChangeEvent.getMasterNodeName();
                            logger.info(sBuilder.append("[BDB Status] ")
                                    .append(currentNode).append(" is a slave.").toString());
                            break;
                        default:
                            isMaster = false;
                            logger.info(sBuilder.append("[BDB Status] ")
                                    .append(currentNode).append(" is Unknown state ")
                                    .append(stateChangeEvent.getState().name()).toString());
                            break;
                    }
                }
            });
        }
    }

}

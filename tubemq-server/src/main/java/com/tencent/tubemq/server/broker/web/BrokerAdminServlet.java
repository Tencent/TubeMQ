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

package com.tencent.tubemq.server.broker.web;

import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.TokenConstants;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.server.broker.TubeBroker;
import com.tencent.tubemq.server.broker.msgstore.MessageStore;
import com.tencent.tubemq.server.broker.msgstore.MessageStoreManager;
import com.tencent.tubemq.server.broker.nodeinfo.ConsumerNodeInfo;
import com.tencent.tubemq.server.broker.offset.OffsetService;
import com.tencent.tubemq.server.common.utils.WebParameterUtils;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/***
 * Broker's web servlet. Used for admin operation, like query consumer's status etc.
 */
public class BrokerAdminServlet extends HttpServlet {
    private final TubeBroker broker;

    public BrokerAdminServlet(TubeBroker broker) {
        this.broker = broker;
    }

    @Override
    protected void doGet(HttpServletRequest req,
                         HttpServletResponse resp) throws ServletException, IOException {
        doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req,
                          HttpServletResponse resp) throws ServletException, IOException {
        StringBuilder sBuilder = new StringBuilder(1024);
        try {
            String method = req.getParameter("method");
            if ("admin_manual_set_current_offset".equals(method)) {
                // manual set offset
                sBuilder = this.adminManuSetCurrentOffSet(req);
            } else if ("admin_query_group_offset".equals(method)) {
                // query consumer group's offset
                sBuilder = this.adminQueryCurrentGroupOffSet(req);
            } else if ("admin_snapshot_message".equals(method)) {
                // query snapshot message
                sBuilder = this.adminQuerySnapshotMessageSet(req);
            } else if ("admin_query_broker_all_consumer_info".equals(method)) {
                // query broker's all consumer info
                sBuilder = this.adminQueryBrokerAllConsumerInfo(req);
            } else if ("admin_query_broker_memstore_info".equals(method)) {
                // get memory store status info
                sBuilder = this.adminGetMemStoreStatisInfo(req);
            } else if ("admin_query_broker_all_store_info".equals(method)) {
                // query broker's all message store info
                sBuilder = this.adminQueryBrokerAllMessageStoreInfo(req);
            } else if ("admin_query_consumer_regmap".equals(method)) {
                Map<String, ConsumerNodeInfo> map =
                        broker.getBrokerServiceServer().getConsumerRegisterMap();
                int totalCnt = 0;
                sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"Success!\",")
                        .append(",\"dataSet\":[");
                for (Entry<String, ConsumerNodeInfo> entry : map.entrySet()) {
                    if (entry.getKey() == null || entry.getValue() == null) {
                        continue;
                    }
                    if (totalCnt > 0) {
                        sBuilder.append(",");
                    }
                    sBuilder.append("{\"Partition\":\"").append(entry.getKey())
                            .append("\",\"Consumer\":\"")
                            .append(entry.getValue().getConsumerId())
                            .append("\",\"index\":").append(++totalCnt).append("}");
                }
                sBuilder.append("],\"totalCnt\":").append(totalCnt).append("}");
            } else {
                sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                        .append("Invalid request: Unsupported method!")
                        .append("\"}");
            }

        } catch (Exception e) {
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append("Bad request from server: ")
                    .append(e.getMessage())
                    .append("\"}");
        }
        resp.getWriter().write(sBuilder.toString());
        resp.setCharacterEncoding(req.getCharacterEncoding());
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.flushBuffer();
    }

    /***
     * Query broker's all consumer info.
     *
     * @param req
     * @return
     * @throws Exception
     */
    private StringBuilder adminQueryBrokerAllConsumerInfo(HttpServletRequest req) throws Exception {
        int index = 0;
        StringBuilder sBuilder = new StringBuilder(1024);
        String groupNameInput =
                WebParameterUtils.validStringParameter("groupName",
                        req.getParameter("groupName"),
                        TBaseConstants.META_MAX_GROUPNAME_LENGTH, false, null);
        sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"Success!\",\"dataSet\":[");
        Map<String, ConsumerNodeInfo> map =
                broker.getBrokerServiceServer().getConsumerRegisterMap();
        for (Entry<String, ConsumerNodeInfo> entry : map.entrySet()) {
            if (TStringUtils.isBlank(entry.getKey()) || entry.getValue() == null) {
                continue;
            }
            String[] partitionIdArr =
                    entry.getKey().split(TokenConstants.ATTR_SEP);
            String groupName = partitionIdArr[0];
            if (!TStringUtils.isBlank(groupNameInput) && (!groupNameInput.equals(groupName))) {
                continue;
            }
            String topicName = partitionIdArr[1];
            int partitionId = Integer.parseInt(partitionIdArr[2]);
            String consumerId = entry.getValue().getConsumerId();
            boolean ifFilterConsume = entry.getValue().isFilterConsume();
            if (index > 0) {
                sBuilder.append(",");
            }
            sBuilder.append("{\"index\":").append(++index).append(",\"groupName\":\"")
                    .append(groupName).append("\",\"topicName\":\"").append(topicName)
                    .append("\",\"partitionId\":").append(partitionId);
            Long regTime =
                    broker.getBrokerServiceServer().getConsumerRegisterTime(consumerId, entry.getKey());
            if (regTime == null || regTime <= 0) {
                sBuilder.append(",\"consumerId\":\"").append(consumerId)
                        .append("\",\"isRegOk\":false")
                        .append(",\"isFilterConsume\":")
                        .append(ifFilterConsume);
            } else {
                sBuilder.append(",\"consumerId\":\"").append(consumerId)
                        .append("\",\"isRegOk\":true,\"lastRegTime\":")
                        .append(regTime).append(",\"isFilterConsume\":")
                        .append(ifFilterConsume);
            }
            sBuilder.append(",\"needSSDProc\":").append(entry.getValue().getNeedSsdProc())
                    .append(",\"ssdTransId\":").append(entry.getValue().getSsdTransId())
                    .append(",\"qryPriorityId\":").append(entry.getValue().getQryPriorityId())
                    .append(",\"curDataLimitInM\":").append(entry.getValue().getCurFlowCtrlLimitSize())
                    .append(",\"curFreqLimit\":").append(entry.getValue().getCurFlowCtrlFreqLimit())
                    .append(",\"totalSentSec\":").append(entry.getValue().getSentMsgSize())
                    .append(",\"isSupportLimit\":").append(entry.getValue().isSupportLimit())
                    .append(",\"sentUnitSec\":").append(entry.getValue().getTotalUnitSec())
                    .append(",\"totalSentMin\":").append(entry.getValue().getTotalUnitMin())
                    .append(",\"sentUnit\":").append(entry.getValue().getSentUnit())
                    .append(",\"SSDDataDltStartInM\":").append(entry.getValue().getDataStartDltInM())
                    .append(",\"SSDDataDltEndInM\":").append(entry.getValue().getSsdDataEndDltInM());
            MessageStoreManager storeManager = broker.getStoreManager();
            OffsetService offsetService = broker.getOffsetManager();
            MessageStore store = storeManager.getOrCreateMessageStore(topicName, partitionId);
            if (store == null) {
                sBuilder.append(",\"isMessageStoreOk\":false}");
            } else {
                long tmpOffset = offsetService.getTmpOffset(groupName, topicName, partitionId);
                long minDataOffset = store.getDataMinOffset();
                long maxDataOffset = store.getDataMaxOffset();
                long minPartOffset = store.getIndexMinOffset();
                long maxPartOffset = store.getIndexMaxOffset();
                long zkOffset = offsetService.getOffset(groupName, topicName, partitionId);
                sBuilder.append(",\"isMessageStoreOk\":true,\"tmpOffset\":").append(tmpOffset)
                        .append(",\"minOffset\":").append(minPartOffset)
                        .append(",\"maxOffset\":").append(maxPartOffset)
                        .append(",\"zkOffset\":").append(zkOffset)
                        .append(",\"minDataOffset\":").append(minDataOffset)
                        .append(",\"maxDataOffset\":").append(maxDataOffset).append("}");
            }
        }
        sBuilder.append("],\"totalCnt\":").append(index).append("}");
        return sBuilder;
    }

    /***
     * Query broker's all messge store info.
     *
     * @param req
     * @return
     * @throws Exception
     */
    private StringBuilder adminQueryBrokerAllMessageStoreInfo(HttpServletRequest req)
            throws Exception {
        StringBuilder sBuilder = new StringBuilder(1024);
        sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"Success!\",\"dataSet\":[");
        String topicNameInput =
                WebParameterUtils.validStringParameter("topicName",
                        req.getParameter("topicName"),
                        TBaseConstants.META_MAX_TOPICNAME_LENGTH, false, null);
        Map<String, ConcurrentHashMap<Integer, MessageStore>> messageTopicStores =
                broker.getStoreManager().getMessageStores();
        int index = 0;
        int recordId = 0;
        for (Map.Entry<String, ConcurrentHashMap<Integer, MessageStore>> entry : messageTopicStores.entrySet()) {
            if (TStringUtils.isBlank(entry.getKey()) ||
                    (TStringUtils.isNotBlank(topicNameInput)
                            && !topicNameInput.equals(entry.getKey()))) {
                continue;
            }
            if (recordId > 0) {
                sBuilder.append(",");
            }
            index = 0;
            sBuilder.append("{\"index\":").append(++recordId).append(",\"topicName\":\"")
                    .append(entry.getKey()).append("\",\"storeInfo\":[");
            ConcurrentHashMap<Integer, MessageStore> partStoreMap = entry.getValue();
            if (partStoreMap != null) {
                for (Entry<Integer, MessageStore> subEntry : partStoreMap.entrySet()) {
                    MessageStore msgStore = subEntry.getValue();
                    if (msgStore == null) {
                        continue;
                    }
                    if (index++ > 0) {
                        sBuilder.append(",");
                    }
                    int numPartId = msgStore.getPartitionNum();
                    sBuilder.append("{\"storeId\":").append(subEntry.getKey())
                            .append(",\"numPartition\":").append(numPartId)
                            .append(",\"minDataOffset\":").append(msgStore.getDataMinOffset())
                            .append(",\"maxDataOffset\":").append(msgStore.getDataMaxOffset())
                            .append(",\"sizeInBytes\":").append(msgStore.getDataStoreSize())
                            .append(",\"partitionInfo\":[");
                    for (int partitionId = 0; partitionId < numPartId; partitionId++) {
                        if (partitionId > 0) {
                            sBuilder.append(",");
                        }
                        sBuilder.append("{\"partitionId\":").append(partitionId)
                                .append(",\"minOffset\":").append(msgStore.getIndexMinOffset())
                                .append(",\"maxOffset\":").append(msgStore.getIndexMaxOffset())
                                .append(",\"sizeInBytes\":").append(msgStore.getIndexStoreSize())
                                .append("}");
                    }
                    sBuilder.append("]}");
                }
            }
            sBuilder.append("]}");
        }
        sBuilder.append("],\"totalCnt\":").append(recordId).append("}");
        return sBuilder;
    }

    /***
     * Get memory store status info.
     *
     * @param req
     * @return
     * @throws Exception
     */
    private StringBuilder adminGetMemStoreStatisInfo(HttpServletRequest req) throws Exception {
        StringBuilder sBuilder = new StringBuilder(1024);
        Set<String> bathTopicNames = new HashSet<String>();
        String inputTopicName = req.getParameter("topicName");
        if (TStringUtils.isNotBlank(inputTopicName)) {
            inputTopicName = String.valueOf(inputTopicName).trim();
            String[] strTopicNames =
                    inputTopicName.split(TokenConstants.ARRAY_SEP);
            for (int i = 0; i < strTopicNames.length; i++) {
                if (TStringUtils.isBlank(strTopicNames[i])) {
                    continue;
                }
                String topicName = strTopicNames[i].trim();
                if (topicName.length() > TBaseConstants.META_MAX_TOPICNAME_LENGTH) {
                    sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                            .append("Invalid parameter: the max length of ")
                            .append(topicName).append(" in topicName parameter over ")
                            .append(TBaseConstants.META_MAX_TOPICNAME_LENGTH)
                            .append(" characters\"}");
                    return sBuilder;
                }
                if (!topicName.matches(TBaseConstants.META_TMP_STRING_VALUE)) {
                    sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                            .append("Invalid parameter: the value of ").append(topicName)
                            .append(" in topicName parameter must begin with a letter,")
                            .append(" can only contain characters,numbers,and underscores!\"}");
                    return sBuilder;
                }
                bathTopicNames.add(topicName);
            }
        }
        boolean requireRefresh =
                WebParameterUtils.validBooleanDataParameter("needRefresh",
                        req.getParameter("needRefresh"), false, false);
        sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"Success!\",\"detail\":[");
        Map<String, ConcurrentHashMap<Integer, MessageStore>> messageTopicStores =
                broker.getStoreManager().getMessageStores();
        int recordId = 0, index = 0;
        for (Map.Entry<String, ConcurrentHashMap<Integer, MessageStore>> entry : messageTopicStores.entrySet()) {
            if (TStringUtils.isBlank(entry.getKey())
                    || (!bathTopicNames.isEmpty() && !bathTopicNames.contains(entry.getKey()))) {
                continue;
            }
            String topicName = entry.getKey();
            if (recordId++ > 0) {
                sBuilder.append(",");
            }
            index = 0;
            sBuilder.append("{\"topicName\":\"").append(topicName).append("\",\"storeStatisInfo\":[");
            ConcurrentHashMap<Integer, MessageStore> partStoreMap = entry.getValue();
            if (partStoreMap != null) {
                for (Entry<Integer, MessageStore> subEntry : partStoreMap.entrySet()) {
                    MessageStore msgStore = subEntry.getValue();
                    if (msgStore == null) {
                        continue;
                    }
                    if (index++ > 0) {
                        sBuilder.append(",");
                    }
                    sBuilder.append("{\"storeId\":").append(subEntry.getKey())
                            .append(",\"memStatis\":").append(msgStore.getCurMemMsgSizeStatisInfo(requireRefresh))
                            .append(",\"fileStatis\":")
                            .append(msgStore.getCurFileMsgSizeStatisInfo(requireRefresh)).append("}");
                }
            }
            sBuilder.append("]}");
        }
        sBuilder.append("],\"totalCount\":").append(recordId).append("}");
        return sBuilder;
    }

    /***
     * Manual set offset.
     *
     * @param req
     * @return
     * @throws Exception
     */
    private StringBuilder adminManuSetCurrentOffSet(HttpServletRequest req) throws Exception {
        StringBuilder sBuilder = new StringBuilder(512);
        final String topicName =
                WebParameterUtils.validStringParameter("topicName",
                        req.getParameter("topicName"),
                        TBaseConstants.META_MAX_TOPICNAME_LENGTH, true, "");
        final String groupName =
                WebParameterUtils.validStringParameter("groupName",
                        req.getParameter("groupName"),
                        TBaseConstants.META_MAX_GROUPNAME_LENGTH, true, "");
        final String modifyUser =
                WebParameterUtils.validStringParameter("modifyUser",
                        req.getParameter("modifyUser"), 64, true, "");
        int partitionId =
                WebParameterUtils.validIntDataParameter("partitionId",
                        req.getParameter("partitionId"), true, -1, 0);
        long manualOffset =
                WebParameterUtils.validLongDataParameter("manualOffset",
                        req.getParameter("manualOffset"), true, -1);
        List<String> topicList = broker.getMetadataManage().getTopics();
        if (!topicList.contains(topicName)) {
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append("Invalid parameter: not found the topicName configure!")
                    .append("\"}");
            return sBuilder;
        }
        MessageStoreManager storeManager = broker.getStoreManager();
        MessageStore store = storeManager.getOrCreateMessageStore(topicName, partitionId);
        if (store == null) {
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append("Invalid parameter: not found the store by topicName!")
                    .append("\"}");
            return sBuilder;
        }
        if (manualOffset < store.getIndexMinOffset()) {
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append("Invalid parameter: manualOffset lower than Current MinOffset:(")
                    .append(manualOffset).append("<").append(store.getIndexMinOffset())
                    .append(")\"}");
            return sBuilder;
        }
        if (manualOffset > store.getIndexMaxOffset()) {
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append("Invalid parameter: manualOffset bigger than Current MaxOffset:(")
                    .append(manualOffset).append(">").append(store.getIndexMaxOffset())
                    .append(")\"}");
            return sBuilder;
        }
        OffsetService offsetService = broker.getOffsetManager();
        long oldOffset =
                offsetService.resetOffset(store, groupName,
                        topicName, partitionId, manualOffset, modifyUser);
        if (oldOffset < 0) {
            sBuilder.append("{\"result\":false,\"errCode\":401,\"errMsg\":\"")
                    .append("Manual update current Offset failue!")
                    .append("\"}");
        } else {
            sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"")
                    .append("Manual update current Offset success!")
                    .append("\",\"oldOffset\":").append(oldOffset).append("}");
        }
        return sBuilder;
    }

    /***
     * Query snapshot message set.
     *
     * @param req
     * @return
     * @throws Exception
     */
    private StringBuilder adminQuerySnapshotMessageSet(HttpServletRequest req) throws Exception {
        StringBuilder sBuilder = new StringBuilder(1024);
        final String topicName =
                WebParameterUtils.validStringParameter("topicName",
                        req.getParameter("topicName"),
                        TBaseConstants.META_MAX_TOPICNAME_LENGTH, true, "");
        final int partitionId =
                WebParameterUtils.validIntDataParameter("partitionId",
                        req.getParameter("partitionId"), false, -1, 0);
        int msgCount =
                WebParameterUtils.validIntDataParameter("msgCount",
                        req.getParameter("msgCount"), false, 3, 3);
        msgCount = msgCount < 1 ? 1 : msgCount;
        if (msgCount > 50) {
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append("Over max allowed msgCount value, allowed count is 50!")
                    .append("\"}");
            return sBuilder;
        }
        Set<String> filterCondStrSet =
                WebParameterUtils.checkAndGetFilterCondSet(req.getParameter("filterConds"),
                        false, true, sBuilder);
        sBuilder = broker.getBrokerServiceServer()
                .getMessageSnapshot(topicName, partitionId, msgCount, filterCondStrSet, sBuilder);
        return sBuilder;
    }

    /***
     * Query consumer group offset.
     *
     * @param req
     * @return
     * @throws Exception
     */
    private StringBuilder adminQueryCurrentGroupOffSet(HttpServletRequest req)
            throws Exception {
        StringBuilder sBuilder = new StringBuilder(1024);
        String topicName =
                WebParameterUtils.validStringParameter("topicName",
                        req.getParameter("topicName"),
                        TBaseConstants.META_MAX_TOPICNAME_LENGTH, true, "");
        String groupName =
                WebParameterUtils.validStringParameter("groupName",
                        req.getParameter("groupName"),
                        TBaseConstants.META_MAX_GROUPNAME_LENGTH, true, "");
        int partitionId =
                WebParameterUtils.validIntDataParameter("partitionId",
                        req.getParameter("partitionId"), true, -1, 0);
        List<String> topicList = broker.getMetadataManage().getTopics();
        if (!topicList.contains(topicName)) {
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append("Invalid parameter: not found the topicName configure!")
                    .append("\"}");
            return sBuilder;
        }
        MessageStoreManager storeManager = broker.getStoreManager();
        OffsetService offsetService = broker.getOffsetManager();
        MessageStore store = storeManager.getOrCreateMessageStore(topicName, partitionId);
        if (store == null) {
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append("Invalid parameter: not found the store by topicName!")
                    .append("\"}");
            return sBuilder;
        }
        boolean requireRealOffset =
                WebParameterUtils.validBooleanDataParameter("requireRealOffset",
                        req.getParameter("requireRealOffset"), false, false);
        long tmpOffset = offsetService.getTmpOffset(groupName, topicName, partitionId);
        long minDataOffset = store.getDataMinOffset();
        long maxDataOffset = store.getDataMaxOffset();
        long minPartOffset = store.getIndexMinOffset();
        long maxPartOffset = store.getIndexMaxOffset();
        sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"")
                .append("OK!")
                .append("\",\"tmpOffset\":").append(tmpOffset)
                .append(",\"minOffset\":").append(minPartOffset)
                .append(",\"maxOffset\":").append(maxPartOffset)
                .append(",\"minDataOffset\":").append(minDataOffset)
                .append(",\"maxDataOffset\":").append(maxDataOffset);
        if (requireRealOffset) {
            long curReadDataOffset = -2;
            long curRdDltDataOffset = -2;
            long zkOffset = offsetService.getOffset(groupName, topicName, partitionId);
            String queryKey =
                    groupName + TokenConstants.ATTR_SEP + topicName + TokenConstants.ATTR_SEP + partitionId;
            ConsumerNodeInfo consumerNodeInfo = broker.getConsumerNodeInfo(queryKey);
            if (consumerNodeInfo != null) {
                curReadDataOffset = consumerNodeInfo.getLastDataRdOffset();
                curRdDltDataOffset = curReadDataOffset < 0 ? -2 : maxDataOffset - curReadDataOffset;
            }
            if (curReadDataOffset < 0) {
                sBuilder.append(",\"zkOffset\":").append(zkOffset)
                        .append(",\"curReadDataOffset\":-1,\"curRdDltDataOffset\":-1");
            } else {
                sBuilder.append(",\"zkOffset\":").append(zkOffset)
                        .append(",\"curReadDataOffset\":").append(curReadDataOffset)
                        .append(",\"curRdDltDataOffset\":").append(curRdDltDataOffset);
            }
        }
        sBuilder.append("}");
        return sBuilder;
    }


}

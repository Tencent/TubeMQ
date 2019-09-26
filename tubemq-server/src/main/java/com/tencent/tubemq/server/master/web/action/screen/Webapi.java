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

package com.tencent.tubemq.server.master.web.action.screen;

import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.cluster.ConsumerInfo;
import com.tencent.tubemq.corebase.cluster.Partition;
import com.tencent.tubemq.corebase.utils.ConcurrentHashSet;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.corerpc.exception.StandbyException;
import com.tencent.tubemq.server.common.utils.WebParameterUtils;
import com.tencent.tubemq.server.master.TMaster;
import com.tencent.tubemq.server.master.nodemanage.nodebroker.BrokerConfManage;
import com.tencent.tubemq.server.master.nodemanage.nodebroker.TopicPSInfoManager;
import com.tencent.tubemq.server.master.nodemanage.nodeconsumer.ConsumerBandInfo;
import com.tencent.tubemq.server.master.nodemanage.nodeconsumer.ConsumerInfoHolder;
import com.tencent.tubemq.server.master.nodemanage.nodeconsumer.NodeRebInfo;
import com.tencent.tubemq.server.master.web.handler.WebAdminFlowRuleHandler;
import com.tencent.tubemq.server.master.web.handler.WebAdminGroupCtrlHandler;
import com.tencent.tubemq.server.master.web.handler.WebAdminTopicAuthHandler;
import com.tencent.tubemq.server.master.web.handler.WebBrokerDefConfHandler;
import com.tencent.tubemq.server.master.web.handler.WebBrokerTopicConfHandler;
import com.tencent.tubemq.server.master.web.model.ClusterGroupVO;
import com.tencent.tubemq.server.master.web.model.ClusterNodeVO;
import com.tencent.tubemq.server.master.web.simplemvc.Action;
import com.tencent.tubemq.server.master.web.simplemvc.RequestContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import javax.servlet.http.HttpServletRequest;

/**
 * Public APIs for master
 * <p>
 * Encapsulate query and modify logic mainly.
 * Generate output JSON by concatenating strings, to improve the performance.
 */
public class Webapi implements Action {
    private TMaster master;

    public Webapi(TMaster master) {
        this.master = master;
    }

    @Override
    public void execute(RequestContext requestContext) {
        StringBuilder strBuffer = new StringBuilder();
        try {
            HttpServletRequest req = requestContext.getReq();
            if (this.master.isStopped()) {
                throw new Exception("Server is stopping...");
            }
            BrokerConfManage brokerConfManage = this.master.getMasterTopicManage();
            if (!brokerConfManage.isSelfMaster()) {
                throw new StandbyException("Please send your request to the master Node.");
            }
            String type = req.getParameter("type");
            String method = req.getParameter("method");
            String strCallbackFun = req.getParameter("callback");
            if ((TStringUtils.isNotEmpty(strCallbackFun))
                    && (strCallbackFun.length() <= TBaseConstants.META_MAX_CALLBACK_STRING_LENGTH)
                    && (strCallbackFun.matches(TBaseConstants.META_TMP_CALLBACK_STRING_VALUE))) {
                strCallbackFun = String.valueOf(strCallbackFun).trim();
            }
            if (type == null) {
                throw new Exception("Please take with type parameter!");
            }
            if (method == null) {
                throw new Exception("Please take with method parameter!");
            }
            boolean succProcess = false;
            if ("op_query".equals(type)) {  // query
                strBuffer = processQueryOperate(req, method);
                succProcess = true;
            } else if ("op_modify".equals(type)) {  // modify
                if (brokerConfManage.isPrimaryNodeActived()) {
                    throw new Exception(
                            "DesignatedPrimary happened...please check if the other member is down");
                }
                strBuffer = processModifyOperate(req, method);
                succProcess = true;
            } else {  // unsupported operations
                strBuffer.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"Unsupported method type :")
                        .append(type).append("}");
                requestContext.put("sb", strBuffer.toString());
            }
            if (succProcess) {  // supported operations, like query or modify
                if (TStringUtils.isEmpty(strCallbackFun)) {
                    requestContext.put("sb", strBuffer.toString());
                } else {
                    requestContext.put("sb", strCallbackFun + "(" + strBuffer.toString() + ")");
                    requestContext.getResp().addHeader("Content-type", "text/plain");
                    requestContext.getResp().addHeader("charset", TBaseConstants.META_DEFAULT_CHARSET_NAME);
                }
            }
        } catch (Throwable e) {
            strBuffer.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"Bad request from client :")
                    .append(e.getMessage()).append("}");
            requestContext.put("sb", strBuffer.toString());
        }
    }

    /**
     * Private method to handle query operation
     *
     * @param req
     * @param method
     * @return output as JSON
     * @throws Throwable
     */
    private StringBuilder processQueryOperate(HttpServletRequest req, final String method) throws Throwable {
        StringBuilder strBuffer = new StringBuilder();
        WebBrokerDefConfHandler webBrokerDefConfHandler = new WebBrokerDefConfHandler(master);
        WebBrokerTopicConfHandler webTopicConfHandler = new WebBrokerTopicConfHandler(master);
        WebAdminGroupCtrlHandler webGroupCtrlHandler = new WebAdminGroupCtrlHandler(master);
        WebAdminTopicAuthHandler webTopicAuthHandler = new WebAdminTopicAuthHandler(master);
        WebAdminFlowRuleHandler webFlowRuleHandler = new WebAdminFlowRuleHandler(master);
        BrokerConfManage brokerConfManage = this.master.getMasterTopicManage();
        if ("admin_query_sub_info".equals(method)) {
            strBuffer = getSubscribeInfo(req);
        } else if ("admin_query_consume_group_detail".equals(method)) {
            strBuffer = getConsumeGroupDetailInfo(req);
        } else if ("admin_query_broker_run_status".equals(method)) {
            strBuffer = webBrokerDefConfHandler.adminQueryBrokerRunStatusInfo(req);
        } else if ("admin_query_broker_configure".equals(method)) {
            strBuffer = webBrokerDefConfHandler.adminQueryBrokerDefConfEntityInfo(req);
        } else if ("admin_query_topic_info".equals(method)) {
            strBuffer = webTopicConfHandler.adminQueryTopicCfgEntityAndRunInfo(req);
        } else if ("admin_query_broker_topic_config_info".equals(method)) {
            strBuffer = webTopicConfHandler.adminQueryBrokerTopicCfgAndRunInfo(req);
        } else if ("admin_query_topic_authorize_control".equals(method)) {
            strBuffer = webTopicAuthHandler.adminQueryTopicAuthControl(req);
        } else if ("admin_query_def_flow_control_rule".equals(method)) {
            strBuffer = webFlowRuleHandler.adminQueryGroupFlowCtrlRule(req, 1);
        } else if ("admin_query_group_flow_control_rule".equals(method)) {
            strBuffer = webFlowRuleHandler.adminQueryGroupFlowCtrlRule(req, 2);
        } else if ("admin_query_black_consumer_group_info".equals(method)) {
            strBuffer = webGroupCtrlHandler.adminQueryBlackGroupInfo(req);
        } else if ("admin_query_allowed_consumer_group_info".equals(method)) {
            strBuffer = webGroupCtrlHandler.adminQueryConsumerGroupInfo(req);
        } else if ("admin_query_group_filtercond_info".equals(method)) {
            strBuffer = webGroupCtrlHandler.adminQueryGroupFilterCondInfo(req);
        } else if ("admin_query_consume_group_setting".equals(method)) {
            strBuffer = webGroupCtrlHandler.adminQueryConsumeGroupSetting(req);
        } else if ("admin_query_master_group_info".equals(method)) {
            strBuffer = getGroupAddressStrInfo(brokerConfManage);
        } else {
            strBuffer.append("{\"result\":false,\"errCode\":400," +
                    "\"errMsg\":\"Unsupported method for the topic info operation\"}");
        }
        return strBuffer;
    }

    /**
     * Private method to handle modify operation
     *
     * @param req
     * @param method
     * @return output as JSON
     * @throws Throwable
     */
    // #lizard forgives
    private StringBuilder processModifyOperate(HttpServletRequest req, final String method) throws Throwable {
        StringBuilder strBuffer = new StringBuilder();
        WebBrokerDefConfHandler webBrokerDefConfHandler = new WebBrokerDefConfHandler(master);
        WebBrokerTopicConfHandler webTopicConfHandler = new WebBrokerTopicConfHandler(master);
        WebAdminGroupCtrlHandler webGroupCtrlHandler = new WebAdminGroupCtrlHandler(master);
        WebAdminTopicAuthHandler webTopicAuthHandler = new WebAdminTopicAuthHandler(master);
        WebAdminFlowRuleHandler webFlowRuleHandler = new WebAdminFlowRuleHandler(master);
        BrokerConfManage brokerConfManage = this.master.getMasterTopicManage();
        if ("admin_add_broker_configure".equals(method)) {
            strBuffer = webBrokerDefConfHandler.adminAddBrokerDefConfEntityInfo(req);
        } else if ("admin_bath_add_broker_configure".equals(method)) {
            strBuffer = webBrokerDefConfHandler.adminBathAddBrokerDefConfEntityInfo(req);
        } else if ("admin_online_broker_configure".equals(method)) {
            strBuffer = webBrokerDefConfHandler.adminOnlineBrokerConf(req);
        } else if ("admin_update_broker_configure".equals(method)) {
            strBuffer = webBrokerDefConfHandler.adminUpdateBrokerConf(req);
        } else if ("admin_reload_broker_configure".equals(method)) {
            strBuffer = webBrokerDefConfHandler.adminReloadBrokerConf(req);
        } else if ("admin_set_broker_read_or_write".equals(method)) {
            strBuffer = webBrokerDefConfHandler.adminSetReadOrWriteBrokerConf(req);
        } else if ("admin_release_broker_autoforbidden_status".equals(method)) {
            strBuffer = webBrokerDefConfHandler.adminRelBrokerAutoForbiddenStatus(req);
        } else if ("admin_offline_broker_configure".equals(method)) {
            strBuffer = webBrokerDefConfHandler.adminOfflineBrokerConf(req);
        } else if ("admin_delete_broker_configure".equals(method)) {
            strBuffer = webBrokerDefConfHandler.adminDeleteBrokerConfEntityInfo(req);
        } else if ("admin_add_new_topic_record".equals(method)) {
            strBuffer = webTopicConfHandler.adminAddTopicEntityInfo(req);
        } else if ("admin_bath_add_new_topic_record".equals(method)) {
            strBuffer = webTopicConfHandler.adminBathAddTopicEntityInfo(req);
        } else if ("admin_modify_topic_info".equals(method)) {
            strBuffer = webTopicConfHandler.adminModifyTopicEntityInfo(req);
        } else if ("admin_delete_topic_info".equals(method)) {
            strBuffer = webTopicConfHandler.adminDeleteTopicEntityInfo(req);
        } else if ("admin_redo_deleted_topic_info".equals(method)) {
            strBuffer = webTopicConfHandler.adminRedoDeleteTopicEntityInfo(req);
        } else if ("admin_remove_topic_info".equals(method)) {
            strBuffer = webTopicConfHandler.adminRemoveTopicEntityInfo(req);
        } else if ("admin_set_topic_authorize_control".equals(method)) {
            strBuffer = webTopicAuthHandler.adminEnableDisableTopicAuthControl(req);
        } else if ("admin_set_def_flow_control_rule".equals(method)) {
            strBuffer = webFlowRuleHandler.adminSetFlowControlRule(req, 1);
        } else if ("admin_rmv_def_flow_control_rule".equals(method)) {
            strBuffer = webFlowRuleHandler.adminDelGroupFlowCtrlRuleStatus(req, 1);
        } else if ("admin_set_group_flow_control_rule".equals(method)) {
            strBuffer = webFlowRuleHandler.adminSetFlowControlRule(req, 2);
        } else if ("admin_rmv_group_flow_control_rule".equals(method)) {
            strBuffer = webFlowRuleHandler.adminDelGroupFlowCtrlRuleStatus(req, 2);
        } else if ("admin_upd_def_flow_control_rule".equals(method)) {
            strBuffer = webFlowRuleHandler.adminModGroupFlowCtrlRuleStatus(req, 1);
        } else if ("admin_upd_group_flow_control_rule".equals(method)) {
            strBuffer = webFlowRuleHandler.adminModGroupFlowCtrlRuleStatus(req, 2);
        } else if ("admin_delete_topic_authorize_control".equals(method)) {
            strBuffer = webTopicAuthHandler.adminDeleteTopicAuthControl(req);
        } else if ("admin_add_black_consumergroup_info".equals(method)) {
            strBuffer = webGroupCtrlHandler.adminAddBlackGroupInfo(req);
        } else if ("admin_bath_add_black_consumergroup_info".equals(method)) {
            strBuffer = webGroupCtrlHandler.adminBathAddBlackGroupInfo(req);
        } else if ("admin_delete_black_consumergroup_info".equals(method)) {
            strBuffer = webGroupCtrlHandler.adminDeleteBlackGroupInfo(req);
        } else if ("admin_add_authorized_consumergroup_info".equals(method)) {
            strBuffer = webGroupCtrlHandler.adminAddConsumerGroupInfo(req);
        } else if ("admin_delete_allowed_consumer_group_info".equals(method)) {
            strBuffer = webGroupCtrlHandler.adminDeleteConsumerGroupInfo(req);
        } else if ("admin_bath_add_topic_authorize_control".equals(method)) {
            strBuffer = webTopicAuthHandler.adminBathAddTopicAuthControl(req);
        } else if ("admin_bath_add_authorized_consumergroup_info".equals(method)) {
            strBuffer = webGroupCtrlHandler.adminBathAddConsumerGroupInfo(req);
        } else if ("admin_add_group_filtercond_info".equals(method)) {
            strBuffer = webGroupCtrlHandler.adminAddGroupFilterCondInfo(req);
        } else if ("admin_bath_add_group_filtercond_info".equals(method)) {
            strBuffer = webGroupCtrlHandler.adminBathAddGroupFilterCondInfo(req);
        } else if ("admin_mod_group_filtercond_info".equals(method)) {
            strBuffer = webGroupCtrlHandler.adminModGroupFilterCondInfo(req);
        } else if ("admin_bath_mod_group_filtercond_info".equals(method)) {
            strBuffer = webGroupCtrlHandler.adminBathModGroupFilterCondInfo(req);
        } else if ("admin_del_group_filtercond_info".equals(method)) {
            strBuffer = webGroupCtrlHandler.adminDeleteGroupFilterCondInfo(req);
        } else if ("admin_add_consume_group_setting".equals(method)) {
            strBuffer = webGroupCtrlHandler.adminAddConsumeGroupSettingInfo(req);
        } else if ("admin_bath_add_consume_group_setting".equals(method)) {
            strBuffer = webGroupCtrlHandler.adminBathAddConsumeGroupSetting(req);
        } else if ("admin_upd_consume_group_setting".equals(method)) {
            strBuffer = webGroupCtrlHandler.adminUpdConsumeGroupSetting(req);
        } else if ("admin_del_consume_group_setting".equals(method)) {
            strBuffer = webGroupCtrlHandler.adminDeleteConsumeGroupSetting(req);
        } else if ("admin_rebalance_group_allocate".equals(method)) {
            strBuffer = webGroupCtrlHandler.adminRebalanceGroupAllocateInfo(req);
        } else if ("admin_transfer_current_master".equals(method)) {
            try {
                WebParameterUtils.reqAuthorizenCheck(master, brokerConfManage, req.getParameter("confModAuthToken"));
                brokerConfManage.transferMaster();
                strBuffer.append("{\"result\":true,\"errCode\":0," +
                        "\"errMsg\":\"TransferMaster method called, please wait 20 seconds!\"}");
            } catch (Exception e2) {
                strBuffer.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                        .append(e2.getMessage()).append("\"}");
            }
        } else {
            strBuffer.append("{\"result\":false,\"errCode\":400," +
                    "\"errMsg\":\"Unsupported method for the topic info operation\"}");
        }
        return strBuffer;
    }

    /**
     * Get master group info
     *
     * @param brokerConfManage
     * @return
     */
    public StringBuilder getGroupAddressStrInfo(BrokerConfManage brokerConfManage) {
        StringBuilder strBuffer = new StringBuilder(512);
        ClusterGroupVO clusterGroupVO = brokerConfManage.getGroupAddressStrInfo();
        if (clusterGroupVO == null) {
            strBuffer.append("{\"result\":false,\"errCode\":500,\"errMsg\":\"GetBrokerGroup info error\",\"data\":[]}");
        } else {
            strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"Ok\",\"groupName\":\"")
                    .append(clusterGroupVO.getGroupName()).append("\",\"isPrimaryNodeActived\":")
                    .append(clusterGroupVO.isPrimaryNodeActived()).append(",\"data\":[");
            int count = 0;
            List<ClusterNodeVO> nodeList = clusterGroupVO.getNodeData();
            if (nodeList != null) {
                for (ClusterNodeVO node : nodeList) {
                    if (node == null) {
                        continue;
                    }
                    if (count++ > 0) {
                        strBuffer.append(",");
                    }
                    strBuffer.append("{\"index\":").append(count).append(",\"name\":\"").append(node.getNodeName())
                            .append("\",\"hostName\":\"").append(node.getHostName())
                            .append("\",\"port\":\"").append(node.getPort())
                            .append("\",\"statusInfo\":{").append("\"nodeStatus\":\"")
                            .append(node.getNodeStatus()).append("\",\"joinTime\":\"")
                            .append(node.getJoinTime()).append("\"}}");
                }
            }
            strBuffer.append("],\"count\":").append(count).append(",\"groupStatus\":\"")
                    .append(clusterGroupVO.getGroupStatus()).append("\"}");
        }
        return strBuffer;
    }

    /**
     * Get subscription info
     *
     * @param req
     * @return
     */
    private StringBuilder getSubscribeInfo(HttpServletRequest req) {
        StringBuilder strBuffer = new StringBuilder();
        try {
            String strConsumeGroup = WebParameterUtils.validStringParameter("consumeGroup",
                    req.getParameter("consumeGroup"), TBaseConstants.META_MAX_GROUPNAME_LENGTH, false, "");
            String strTopicName = WebParameterUtils.validStringParameter("topicName",
                    req.getParameter("topicName"), TBaseConstants.META_MAX_TOPICNAME_LENGTH, false, "");
            ConsumerInfoHolder consumerHolder = master.getConsumerHolder();
            TopicPSInfoManager topicPSInfoManager = master.getTopicPSInfoManager();
            List<String> queryGroupSet = new ArrayList<String>();
            if (TStringUtils.isEmpty(strTopicName)) {
                List<String> tmpGroupSet = consumerHolder.getAllGroup();
                if (!tmpGroupSet.isEmpty()) {
                    if (TStringUtils.isEmpty(strConsumeGroup)) {
                        for (String tmpGroup : tmpGroupSet) {
                            if (tmpGroup != null) {
                                queryGroupSet.add(tmpGroup);
                            }
                        }
                    } else {
                        if (tmpGroupSet.contains(strConsumeGroup)) {
                            queryGroupSet.add(strConsumeGroup);
                        }
                    }
                }
            } else {
                ConcurrentHashSet<String> groupSet = topicPSInfoManager.getTopicSubInfo(strTopicName);
                if ((groupSet != null) && (!groupSet.isEmpty())) {
                    if (TStringUtils.isEmpty(strConsumeGroup)) {
                        for (String tmpGroup : groupSet) {
                            if (tmpGroup != null) {
                                queryGroupSet.add(tmpGroup);
                            }
                        }
                    } else {
                        if (groupSet.contains(strConsumeGroup)) {
                            queryGroupSet.add(strConsumeGroup);
                        }
                    }
                }
            }
            if (queryGroupSet.isEmpty()) {
                strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\",\"count\":0,\"data\":[]}");
            } else {
                int totalCnt = 0;
                strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\",\"count\":")
                        .append(queryGroupSet.size()).append(",\"data\":[");
                for (String tmpGroup : queryGroupSet) {
                    Set<String> topicSet = consumerHolder.getGroupTopicSet(tmpGroup);
                    final int consuemrCnt = consumerHolder.getConsuemrCnt(tmpGroup);
                    if (totalCnt++ > 0) {
                        strBuffer.append(",");
                    }
                    strBuffer.append("{\"consumeGroup\":\"").append(tmpGroup).append("\",\"topicSet\":[");
                    int topicCnt = 0;
                    for (String tmpTopic : topicSet) {
                        if (topicCnt++ > 0) {
                            strBuffer.append(",");
                        }
                        strBuffer.append("\"").append(tmpTopic).append("\"");
                    }
                    strBuffer.append("],\"consumerNum\":").append(consuemrCnt).append("}");
                }
                strBuffer.append("]}");
            }
        } catch (Exception e) {
            strBuffer.append("{\"result\":false,\"errCode\":500,\"errMsg\":\"Exception on process")
                    .append(e.getMessage()).append("\",\"count\":0,\"data\":[]}");
        }
        return strBuffer;
    }

    /**
     * Get consume group detail info
     *
     * @param req
     * @return output as JSON
     */
    // #lizard forgives
    private StringBuilder getConsumeGroupDetailInfo(HttpServletRequest req) {
        StringBuilder strBuffer = new StringBuilder(1024);
        try {
            String strConsumeGroup = WebParameterUtils.validStringParameter("consumeGroup",
                    req.getParameter("consumeGroup"), TBaseConstants.META_MAX_GROUPNAME_LENGTH, true, "");
            boolean isBandConsume = false;
            boolean isNotAllocate = false;
            boolean isSelectBig = true;
            String sessionKey = "";
            int reqSourceCount = -1;
            int curSourceCount = -1;
            long rebanceCheckTime = -1;
            int defBClientRate = -2;
            int confBClientRate = -2;
            int curBClientRate = -2;
            int minRequireClientCnt = -2;
            int rebalanceStatus = -2;
            Set<String> topicSet = new HashSet<>();
            List<ConsumerInfo> consumerList = new ArrayList<ConsumerInfo>();
            Map<String, NodeRebInfo> nodeRebInfoMap = new ConcurrentHashMap<String, NodeRebInfo>();
            Map<String, TreeSet<String>> existedTopicCondtions = new HashMap<String, TreeSet<String>>();
            ConsumerInfoHolder consumerHolder = master.getConsumerHolder();
            ConsumerBandInfo consumerBandInfo = consumerHolder.getConsumerBandInfo(strConsumeGroup);
            if (consumerBandInfo != null) {
                if (consumerBandInfo.getTopicSet() != null) {
                    topicSet = consumerBandInfo.getTopicSet();
                }
                if (consumerBandInfo.getConsumerInfoList() != null) {
                    consumerList = consumerBandInfo.getConsumerInfoList();
                }
                if (consumerBandInfo.getTopicConditions() != null) {
                    existedTopicCondtions = consumerBandInfo.getTopicConditions();
                }
                nodeRebInfoMap = consumerBandInfo.getRebalanceMap();
                isBandConsume = consumerBandInfo.isBandConsume();
                rebalanceStatus = consumerBandInfo.getRebalanceCheckStatus();
                defBClientRate = consumerBandInfo.getDefBClientRate();
                confBClientRate = consumerBandInfo.getConfBClientRate();
                curBClientRate = consumerBandInfo.getCurBClientRate();
                minRequireClientCnt = consumerBandInfo.getMinRequireClientCnt();
                if (isBandConsume) {
                    isNotAllocate = consumerBandInfo.isNotAllocate();
                    isSelectBig = consumerBandInfo.isSelectedBig();
                    sessionKey = consumerBandInfo.getSessionKey();
                    reqSourceCount = consumerBandInfo.getSourceCount();
                    curSourceCount = consumerBandInfo.getGroupCnt();
                    rebanceCheckTime = consumerBandInfo.getCurCheckCycle();
                }
            }
            strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\"")
                    .append(",\"count\":").append(consumerList.size()).append(",\"topicSet\":[");
            int itemCnt = 0;
            for (String topicItem : topicSet) {
                if (itemCnt++ > 0) {
                    strBuffer.append(",");
                }
                strBuffer.append("\"").append(topicItem).append("\"");
            }
            strBuffer.append("],\"consumeGroup\":\"").append(strConsumeGroup).append("\",\"re-rebalance\":{");
            itemCnt = 0;
            for (Map.Entry<String, NodeRebInfo> entry : nodeRebInfoMap.entrySet()) {
                if (itemCnt++ > 0) {
                    strBuffer.append(",");
                }
                strBuffer.append("\"").append(entry.getKey()).append("\":");
                strBuffer = entry.getValue().toJsonString(strBuffer);
            }
            strBuffer.append("},\"isBandConsume\":").append(isBandConsume);
            // Append band consume info
            if (isBandConsume) {
                strBuffer.append(",\"isNotAllocate\":").append(isNotAllocate)
                        .append(",\"sessionKey\":\"").append(sessionKey)
                        .append("\",\"isSelectBig\":").append(isSelectBig)
                        .append(",\"reqSourceCount\":").append(reqSourceCount)
                        .append(",\"curSourceCount\":").append(curSourceCount)
                        .append(",\"rebanceCheckTime\":").append(rebanceCheckTime);
            }
            strBuffer.append(",\"rebInfo\":{");
            if (rebalanceStatus == -2) {
                strBuffer.append("\"isRebalanced\":false");
            } else if (rebalanceStatus == 0) {
                strBuffer.append("\"isRebalanced\":true,\"checkPasted\":false")
                        .append(",\"defBClientRate\":").append(defBClientRate)
                        .append(",\"confBClientRate\":").append(confBClientRate)
                        .append(",\"curBClientRate\":").append(curBClientRate)
                        .append(",\"minRequireClientCnt\":").append(minRequireClientCnt);
            } else {
                strBuffer.append("\"isRebalanced\":true,\"checkPasted\":true")
                        .append(",\"defBClientRate\":").append(defBClientRate)
                        .append(",\"confBClientRate\":").append(confBClientRate)
                        .append(",\"curBClientRate\":").append(curBClientRate);
            }
            strBuffer.append("},\"filterConds\":{");
            if (existedTopicCondtions != null) {
                int keyCount = 0;
                for (Map.Entry<String, TreeSet<String>> entry : existedTopicCondtions.entrySet()) {
                    if (keyCount++ > 0) {
                        strBuffer.append(",");
                    }
                    strBuffer.append("\"").append(entry.getKey()).append("\":[");
                    if (entry.getValue() != null) {
                        int itemCount = 0;
                        for (String filterCond : entry.getValue()) {
                            if (itemCount++ > 0) {
                                strBuffer.append(",");
                            }
                            strBuffer.append("\"").append(filterCond).append("\"");
                        }
                    }
                    strBuffer.append("]");
                }
            }
            strBuffer.append("}");
            // Append consumer info of the group
            getConsumerInfoList(consumerList, isBandConsume, strBuffer);
            strBuffer.append("}");
        } catch (Exception e) {
            strBuffer.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append(e.getMessage()).append("\",\"count\":0,\"data\":[]}");
        }
        return strBuffer;
    }

    /**
     * Private method to append consumer info of the give list to a string builder
     *
     * @param consumerList
     * @param isBandConsume
     * @param strBuffer
     */
    private void getConsumerInfoList(final List<ConsumerInfo> consumerList,
                                     boolean isBandConsume, final StringBuilder strBuffer) {
        strBuffer.append(",\"data\":[");
        if (!consumerList.isEmpty()) {
            Collections.sort(consumerList);
            Map<String, Map<String, Map<String, Partition>>> currentSubInfoMap =
                    master.getCurrentSubInfoMap();
            for (int i = 0; i < consumerList.size(); i++) {
                ConsumerInfo consumer = consumerList.get(i);
                if (consumer == null) {
                    continue;
                }
                if (i > 0) {
                    strBuffer.append(",");
                }
                strBuffer.append("{\"consumerId\":\"").append(consumer.getConsumerId())
                        .append("\"").append(",\"isOverTLS\":").append(consumer.isOverTLS());
                if (isBandConsume) {
                    Map<String, Long> requiredPartition = consumer.getRequiredPartition();
                    if (requiredPartition == null || requiredPartition.isEmpty()) {
                        strBuffer.append(",\"initReSetPartCount\":0,\"initReSetPartInfo\":[]");
                    } else {
                        strBuffer.append(",\"initReSetPartCount\":").append(requiredPartition.size())
                                .append(",\"initReSetPartInfo\":[");
                        int totalPart = 0;
                        for (Map.Entry<String, Long> entry : requiredPartition.entrySet()) {
                            if (totalPart++ > 0) {
                                strBuffer.append(",");
                            }
                            strBuffer.append("{\"partitionKey\":\"").append(entry.getKey())
                                    .append("\",\"Offset\":").append(entry.getValue()).append("}");
                        }
                        strBuffer.append("]");
                    }
                }
                Map<String, Map<String, Partition>> topicSubMap =
                        currentSubInfoMap.get(consumer.getConsumerId());
                if (topicSubMap == null || topicSubMap.isEmpty()) {
                    strBuffer.append(",\"parCount\":0,\"parInfo\":[]}");
                } else {
                    int totalSize = 0;
                    for (Map.Entry<String, Map<String, Partition>> entry : topicSubMap.entrySet()) {
                        totalSize += entry.getValue().size();
                    }
                    strBuffer.append(",\"parCount\":").append(totalSize).append(",\"parInfo\":[");
                    int totalPart = 0;
                    for (Map.Entry<String, Map<String, Partition>> entry : topicSubMap.entrySet()) {
                        Map<String, Partition> partMap = entry.getValue();
                        if (partMap != null) {
                            for (Partition part : partMap.values()) {
                                if (totalPart++ > 0) {
                                    strBuffer.append(",");
                                }
                                strBuffer.append("{\"partId\":").append(part.getPartitionId())
                                        .append(",\"brokerAddr\":\"").append(part.getBroker().toString())
                                        .append("\",\"topicName\":\"").append(part.getTopic()).append("\"}");
                            }
                        }
                    }
                    strBuffer.append("]}");
                }
            }
        }
        strBuffer.append("]");
    }
}

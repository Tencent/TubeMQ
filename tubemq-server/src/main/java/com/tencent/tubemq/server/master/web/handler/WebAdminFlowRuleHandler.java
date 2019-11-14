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

package com.tencent.tubemq.server.master.web.handler;

import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.policies.FlowCtrlItem;
import com.tencent.tubemq.corebase.policies.FlowCtrlRuleHandler;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.server.common.TServerConstants;
import com.tencent.tubemq.server.common.utils.WebParameterUtils;
import com.tencent.tubemq.server.master.TMaster;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbGroupFlowCtrlEntity;
import com.tencent.tubemq.server.master.nodemanage.nodebroker.BrokerConfManage;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WebAdminFlowRuleHandler {

    private static final Logger logger =
            LoggerFactory.getLogger(WebAdminFlowRuleHandler.class);
    private TMaster master;
    private BrokerConfManage brokerConfManage;
    private static final List<Integer> allowedPriorityVal = Arrays.asList(1, 2, 3);

    public WebAdminFlowRuleHandler(TMaster master) {
        this.master = master;
        this.brokerConfManage = this.master.getMasterTopicManage();
    }

    /**
     * add flow control rule
     *
     * @param req
     * @param opType
     * @return
     * @throws Exception
     */
    public StringBuilder adminSetFlowControlRule(HttpServletRequest req,
                                                 int opType) throws Exception {
        StringBuilder strBuffer = new StringBuilder(512);
        try {
            // check if allow modify
            WebParameterUtils.reqAuthorizenCheck(master,
                    brokerConfManage, req.getParameter("confModAuthToken"));
            // get createUser info
            String createUser =
                    WebParameterUtils.validStringParameter("createUser",
                            req.getParameter("createUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH, true, "");
            // get createDate info
            Date createDate =
                    WebParameterUtils.validDateParameter("createDate",
                            req.getParameter("createDate"),
                            TBaseConstants.META_MAX_DATEVALUE_LENGTH, false, new Date());
            // get rule required status info
            int statusId =
                    WebParameterUtils.validIntDataParameter("statusId",
                            req.getParameter("statusId"), false, 0, 0);
            // get and valid priority info
            int qryPriorityId =
                    WebParameterUtils.validIntDataParameter("qryPriorityId",
                            req.getParameter("qryPriorityId"), false, 301, 101);
            checkQryPriorityId(qryPriorityId);
            // get if enable ssd process function
            boolean curNeedSSDProc =
                    WebParameterUtils.validBooleanDataParameter("needSSDProc",
                            req.getParameter("needSSDProc"),
                            false, false);
            Set<String> bathGroupName = new HashSet<String>();
            if (opType == 1) {
                bathGroupName.add(TServerConstants.TOKEN_DEFAULT_FLOW_CONTROL);
            } else {
                // get groupname info if rule is set to consume group
                boolean checkResToken = opType > 1;
                Set<String> resTokenSet = new HashSet<String>();
                if (checkResToken) {
                    resTokenSet.add(TServerConstants.TOKEN_DEFAULT_FLOW_CONTROL);
                }
                bathGroupName =
                        WebParameterUtils.getBatchGroupNames(req.getParameter("groupName"),
                                true, checkResToken, resTokenSet, strBuffer);
            }
            // get and flow control rule info
            int ruleCnt =
                    checkAndGetFlowRules(req.getParameter("flowCtrlInfo"), opType, strBuffer);
            // add flow control to bdb
            for (String groupName : bathGroupName) {
                if (groupName.equals(TServerConstants.TOKEN_DEFAULT_FLOW_CONTROL)) {
                    brokerConfManage.confAddBdbGroupFlowCtrl(
                            new BdbGroupFlowCtrlEntity(strBuffer.toString(),
                                    statusId, ruleCnt, qryPriorityId, "",
                                    curNeedSSDProc, createUser, createDate));
                } else {
                    brokerConfManage.confAddBdbGroupFlowCtrl(
                            new BdbGroupFlowCtrlEntity(groupName,
                                    strBuffer.toString(), statusId, ruleCnt, qryPriorityId, "",
                                    curNeedSSDProc, createUser, createDate));
                }
            }
            strBuffer.delete(0, strBuffer.length());
            strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\"}");
        } catch (Exception e) {
            strBuffer.delete(0, strBuffer.length());
            strBuffer.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append(e.getMessage()).append("\"}");
        }
        return strBuffer;
    }

    /**
     * delete flow control rule
     *
     * @param req
     * @param opType
     * @return
     * @throws Exception
     */
    public StringBuilder adminDelGroupFlowCtrlRuleStatus(HttpServletRequest req,
                                                         int opType) throws Exception {
        StringBuilder strBuffer = new StringBuilder(512);
        try {
            WebParameterUtils.reqAuthorizenCheck(master,
                    brokerConfManage, req.getParameter("confModAuthToken"));
            String createUser =
                    WebParameterUtils.validStringParameter("createUser",
                            req.getParameter("createUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH, true, "");
            Date modifyDate =
                    WebParameterUtils.validDateParameter("createDate",
                            req.getParameter("createDate"),
                            TBaseConstants.META_MAX_DATEVALUE_LENGTH, false, new Date());
            Set<String> bathGroupName = new HashSet<String>();
            if (opType == 1) {
                bathGroupName.add(TServerConstants.TOKEN_DEFAULT_FLOW_CONTROL);
            } else {
                boolean checkResToken = opType > 1;
                Set<String> resTokenSet = new HashSet<String>();
                if (checkResToken) {
                    resTokenSet.add(TServerConstants.TOKEN_DEFAULT_FLOW_CONTROL);
                }
                bathGroupName =
                        WebParameterUtils.getBatchGroupNames(req.getParameter("groupName"),
                                true, checkResToken, resTokenSet, strBuffer);
            }
            brokerConfManage.confDeleteBdbGroupFlowCtrl(bathGroupName);
            strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\"}");
        } catch (Exception e) {
            strBuffer.delete(0, strBuffer.length());
            strBuffer.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append(e.getMessage()).append("\"}");
        }
        return strBuffer;
    }

    /**
     * modify flow control rule
     *
     * @param req
     * @param opType
     * @return
     * @throws Exception
     */
    public StringBuilder adminModGroupFlowCtrlRuleStatus(HttpServletRequest req,
                                                         int opType) throws Exception {
        // #lizard forgives
        StringBuilder strBuffer = new StringBuilder(512);
        try {
            WebParameterUtils.reqAuthorizenCheck(master,
                    brokerConfManage, req.getParameter("confModAuthToken"));
            String modifyUser =
                    WebParameterUtils.validStringParameter("createUser",
                            req.getParameter("createUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH, true, "");
            Date modifyDate =
                    WebParameterUtils.validDateParameter("createDate",
                            req.getParameter("createDate"),
                            TBaseConstants.META_MAX_DATEVALUE_LENGTH, false, new Date());
            Set<String> bathGroupName = new HashSet<String>();
            // check optype
            if (opType == 1) {
                bathGroupName.add(TServerConstants.TOKEN_DEFAULT_FLOW_CONTROL);
            } else {
                boolean checkResToken = opType > 1;
                Set<String> resTokenSet = new HashSet<String>();
                if (checkResToken) {
                    resTokenSet.add(TServerConstants.TOKEN_DEFAULT_FLOW_CONTROL);
                }
                bathGroupName =
                        WebParameterUtils.getBatchGroupNames(req.getParameter("groupName"),
                                true, checkResToken, resTokenSet, strBuffer);
            }
            int ruleCnt =
                    checkAndGetFlowRules(req.getParameter("flowCtrlInfo"), opType, strBuffer);
            String newFlowCtrlInfo = strBuffer.toString();
            strBuffer.delete(0, strBuffer.length());
            for (String groupName : bathGroupName) {
                // check if record changed
                BdbGroupFlowCtrlEntity oldEntity =
                        brokerConfManage.getBdbGroupFlowCtrl(groupName);
                if (oldEntity != null) {
                    boolean foundChange = false;
                    BdbGroupFlowCtrlEntity newGroupFlowCtrlEntity =
                            new BdbGroupFlowCtrlEntity(oldEntity.getGroupName(),
                                    oldEntity.getFlowCtrlInfo(), oldEntity.getStatusId(),
                                    oldEntity.getRuleCnt(), oldEntity.getAttributes(),
                                    oldEntity.getSsdTranslateId(), oldEntity.isNeedSSDProc(),
                                    oldEntity.getCreateUser(), oldEntity.getCreateDate());
                    int statusId =
                            WebParameterUtils.validIntDataParameter("statusId",
                                    req.getParameter("statusId"),
                                    false, TBaseConstants.META_VALUE_UNDEFINED, 0);
                    if (statusId != TBaseConstants.META_VALUE_UNDEFINED
                            && statusId != oldEntity.getStatusId()) {
                        foundChange = true;
                        newGroupFlowCtrlEntity.setStatusId(statusId);
                    }
                    int qryPriorityId =
                            WebParameterUtils.validIntDataParameter("qryPriorityId",
                                    req.getParameter("qryPriorityId"),
                                    false, TBaseConstants.META_VALUE_UNDEFINED, 101);
                    if (qryPriorityId != TBaseConstants.META_VALUE_UNDEFINED
                            && qryPriorityId != oldEntity.getQryPriorityId()) {
                        checkQryPriorityId(qryPriorityId);
                        foundChange = true;
                        newGroupFlowCtrlEntity.setQryPriorityId(qryPriorityId);
                    }
                    if (TStringUtils.isNotBlank(newFlowCtrlInfo)
                            && !newFlowCtrlInfo.equals(oldEntity.getFlowCtrlInfo())) {
                        foundChange = true;
                        newGroupFlowCtrlEntity.setFlowCtrlInfo(newFlowCtrlInfo);
                        newGroupFlowCtrlEntity.setRuleCnt(ruleCnt);
                    }
                    String inNeedSsdProc = req.getParameter("needSSDProc");
                    if (TStringUtils.isNotBlank(inNeedSsdProc)) {
                        boolean curNeedSsdProc =
                                WebParameterUtils.validBooleanDataParameter("needSSDProc",
                                        req.getParameter("needSSDProc"), false, false);
                        if (curNeedSsdProc != oldEntity.isNeedSSDProc()) {
                            foundChange = true;
                            newGroupFlowCtrlEntity.setNeedSSDProc(curNeedSsdProc);
                        }
                    }
                    // update record if found change
                    if (foundChange) {
                        try {
                            brokerConfManage.confUpdateBdbGroupFlowCtrl(newGroupFlowCtrlEntity);
                        } catch (Throwable ee) {
                            //
                        }
                    }
                }
            }
            strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\"}");
        } catch (Exception e) {
            strBuffer.delete(0, strBuffer.length());
            strBuffer.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append(e.getMessage()).append("\"}");
        }
        return strBuffer;
    }

    /**
     * query flow control rule
     *
     * @param req
     * @param opType
     * @return
     * @throws Exception
     */
    public StringBuilder adminQueryGroupFlowCtrlRule(HttpServletRequest req,
                                                     int opType) throws Exception {
        StringBuilder strBuffer = new StringBuilder(512);
        BdbGroupFlowCtrlEntity bdbGroupFlowCtrlEntity = new BdbGroupFlowCtrlEntity();
        try {
            bdbGroupFlowCtrlEntity
                    .setCreateUser(WebParameterUtils.validStringParameter("createUser",
                            req.getParameter("createUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH, false, null));
            bdbGroupFlowCtrlEntity
                    .setStatusId(WebParameterUtils.validIntDataParameter("statusId",
                            req.getParameter("statusId"),
                            false, TBaseConstants.META_VALUE_UNDEFINED, 0));
            bdbGroupFlowCtrlEntity
                    .setQryPriorityId(WebParameterUtils.validIntDataParameter("qryPriorityId",
                            req.getParameter("qryPriorityId"),
                            false, TBaseConstants.META_VALUE_UNDEFINED, 0));
            Set<String> bathGroupName = new HashSet<String>();
            if (opType == 1) {
                bathGroupName.add(TServerConstants.TOKEN_DEFAULT_FLOW_CONTROL);
            } else {
                boolean checkResToken = opType > 1;
                Set<String> resTokenSet = new HashSet<String>();
                if (checkResToken) {
                    resTokenSet.add(TServerConstants.TOKEN_DEFAULT_FLOW_CONTROL);
                }
                bathGroupName =
                        WebParameterUtils.getBatchGroupNames(req.getParameter("groupName"),
                                false, checkResToken, resTokenSet, strBuffer);
            }
            // return result as json format
            int countI = 0;
            strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\",\"data\":[");
            List<BdbGroupFlowCtrlEntity> webGroupFlowCtrlEntities =
                    brokerConfManage.confGetBdbGroupFlowCtrl(bdbGroupFlowCtrlEntity);
            for (BdbGroupFlowCtrlEntity entity : webGroupFlowCtrlEntities) {
                if (!bathGroupName.isEmpty()) {
                    boolean found = false;
                    for (String tmpGroupName : bathGroupName) {
                        if (entity.getGroupName().equals(tmpGroupName)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        continue;
                    }
                }
                if (opType > 1) {
                    if (entity.getGroupName()
                            .equals(TServerConstants.TOKEN_DEFAULT_FLOW_CONTROL)) {
                        continue;
                    }
                }
                if (countI++ > 0) {
                    strBuffer.append(",");
                }
                strBuffer = entity.toJsonString(strBuffer);
            }
            strBuffer.append("],\"count\":").append(countI).append("}");
        } catch (Exception e) {
            strBuffer.delete(0, strBuffer.length());
            strBuffer.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append(e.getMessage()).append("\",\"count\":0,\"data\":[]}");
        }
        return strBuffer;
    }

    // translate rule info to json format string
    private int checkAndGetFlowRules(String inFlowCtrlInfo,
                                     int opType,
                                     StringBuilder strBuffer) throws Exception {
        int ruleCnt = 0;
        strBuffer.append("[");
        if (TStringUtils.isNotBlank(inFlowCtrlInfo)) {
            List<Integer> ruleTypes = Arrays.asList(0, 1, 2, 3);
            inFlowCtrlInfo = String.valueOf(inFlowCtrlInfo).trim();
            FlowCtrlRuleHandler flowCtrlRuleHandler =
                new FlowCtrlRuleHandler(true);
            Map<Integer, List<FlowCtrlItem>> flowCtrlItemMap =
                    flowCtrlRuleHandler.parseFlowCtrlInfo(inFlowCtrlInfo);
            for (Integer typeId : ruleTypes) {
                if (typeId != null) {
                    int rules = 0;
                    List<FlowCtrlItem> flowCtrlItems = flowCtrlItemMap.get(typeId);
                    if (flowCtrlItems != null) {
                        if (opType != 1 && typeId == 2) {
                            throw new Exception(
                                    "Illegal value: SSD limit rule only set in default flow control set!");
                        }
                        if (ruleCnt++ > 0) {
                            strBuffer.append(",");
                        }
                        strBuffer.append("{\"type\":").append(typeId.intValue()).append(",\"rule\":[");
                        for (FlowCtrlItem flowCtrlItem : flowCtrlItems) {
                            if (flowCtrlItem != null) {
                                if (rules++ > 0) {
                                    strBuffer.append(",");
                                }
                                strBuffer = flowCtrlItem.toJsonString(strBuffer);
                            }
                        }
                        strBuffer.append("]}");
                    }
                }
            }
        }
        strBuffer.append("]");
        return ruleCnt;
    }

    private void checkQryPriorityId(int qryPriorityId) throws Exception {
        if (qryPriorityId > 303 || qryPriorityId < 101) {
            throw new Exception(
                    "Illegal value in qryPriorityId parameter: qryPriorityId value"
                            + " must be greater than or equal to 101 and less than or equal to 303!");
        }
        if (!allowedPriorityVal.contains(qryPriorityId % 100)) {
            throw new Exception("Illegal value in qryPriorityId parameter:"
                    + " the units of qryPriorityId must in [1,2,3]!");
        }
        if (!allowedPriorityVal.contains(qryPriorityId / 100)) {
            throw new Exception("Illegal value in qryPriorityId parameter:"
                    + " the hundreds of qryPriorityId must in [1,2,3]!");
        }
    }

}

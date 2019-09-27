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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.server.common.utils.WebParameterUtils;
import com.tencent.tubemq.server.master.TMaster;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbConsumerGroupEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbGroupFilterCondEntity;
import com.tencent.tubemq.server.master.bdbstore.bdbentitys.BdbTopicAuthControlEntity;
import com.tencent.tubemq.server.master.nodemanage.nodebroker.BrokerConfManage;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WebAdminTopicAuthHandler {

    private static final Logger logger =
            LoggerFactory.getLogger(WebAdminTopicAuthHandler.class);
    private TMaster master;
    private BrokerConfManage brokerConfManage;

    public WebAdminTopicAuthHandler(TMaster master) {
        this.master = master;
        this.brokerConfManage = this.master.getMasterTopicManage();
    }

    /**
     * Enable or disable topic authorization control
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminEnableDisableTopicAuthControl(
            HttpServletRequest req) throws Exception {
        StringBuilder sBuilder = new StringBuilder(512);
        try {
            WebParameterUtils.reqAuthorizenCheck(master, brokerConfManage,
                    req.getParameter("confModAuthToken"));
            String createUser =
                    WebParameterUtils.validStringParameter("createUser",
                            req.getParameter("createUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH,
                            true, "");
            Date createDate =
                    WebParameterUtils.validDateParameter("createDate",
                            req.getParameter("createDate"),
                            TBaseConstants.META_MAX_DATEVALUE_LENGTH,
                            false, new Date());
            boolean isEnable =
                    WebParameterUtils.validBooleanDataParameter("isEnable",
                            req.getParameter("isEnable"),
                            false, false);
            Set<String> configuredTopicSet =
                    brokerConfManage.getTotalConfiguredTopicNames();
            Set<String> bathOpTopicNames =
                    WebParameterUtils.getBatchTopicNames(req.getParameter("topicName"),
                            true, true, configuredTopicSet, sBuilder);
            for (String topicName : bathOpTopicNames) {
                brokerConfManage.confSetBdbTopicAuthControl(
                        new BdbTopicAuthControlEntity(topicName,
                                isEnable, createUser, createDate));
            }
            sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\"}");
        } catch (Exception e) {
            sBuilder.delete(0, sBuilder.length());
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append(e.getMessage()).append("\"}");
        }
        return sBuilder;
    }

    /**
     * Add topic authorization control in batch
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminBathAddTopicAuthControl(HttpServletRequest req) throws Exception {
        StringBuilder sBuilder = new StringBuilder(512);
        try {
            WebParameterUtils.reqAuthorizenCheck(master, brokerConfManage,
                    req.getParameter("confModAuthToken"));
            String operator =
                    WebParameterUtils.validStringParameter("createUser", req.getParameter("createUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH, true, "");
            Date createDate =
                    WebParameterUtils.validDateParameter("createDate", req.getParameter("createDate"),
                            TBaseConstants.META_MAX_DATEVALUE_LENGTH, false, new Date());
            List<Map<String, Object>> topicJsonArray =
                    WebParameterUtils.checkAndGetJsonArray("topicJsonSet",
                            req.getParameter("topicJsonSet"), TBaseConstants.META_VALUE_UNDEFINED, true);
            if ((topicJsonArray == null) || (topicJsonArray.isEmpty())) {
                throw new Exception("Null value of topicJsonSet, please set the value first!");
            }
            Set<String> configuredTopicSet = brokerConfManage.getTotalConfiguredTopicNames();
            HashMap<String, BdbTopicAuthControlEntity> inTopicAuthConfEntityMap =
                    new HashMap<String, BdbTopicAuthControlEntity>();
            HashMap<String, BdbConsumerGroupEntity> inGroupAuthConfEntityMap =
                    new HashMap<String, BdbConsumerGroupEntity>();
            for (int count = 0; count < topicJsonArray.size(); count++) {
                Map<String, Object> jsonObject = topicJsonArray.get(count);
                try {
                    String topicName =
                            WebParameterUtils.validStringParameter("topicName", jsonObject.get("topicName"),
                                    TBaseConstants.META_MAX_TOPICNAME_LENGTH, true, "");
                    boolean enableControl =
                            WebParameterUtils.validBooleanDataParameter("isEnable",
                                    jsonObject.get("isEnable"),
                                    false, false);
                    String itemCreateUser =
                            WebParameterUtils.validStringParameter("createUser",
                                    jsonObject.get("createUser"),
                                    TBaseConstants.META_MAX_USERNAME_LENGTH,
                                    false, null);
                    Date itemCreateDate =
                            WebParameterUtils.validDateParameter("createDate",
                                    jsonObject.get("createDate"),
                                    TBaseConstants.META_MAX_DATEVALUE_LENGTH,
                                    false, null);
                    if ((TStringUtils.isBlank(itemCreateUser)) || (itemCreateDate == null)) {
                        itemCreateUser = operator;
                        itemCreateDate = createDate;
                    }
                    if (!configuredTopicSet.contains(topicName)) {
                        throw new Exception(sBuilder.append("Topic: ").append(topicName)
                                .append(" not configure in master's topic configure, please configure first!")
                                .toString());
                    }
                    inTopicAuthConfEntityMap.put(topicName, new BdbTopicAuthControlEntity(topicName,
                            enableControl, itemCreateUser, itemCreateDate));
                    inGroupAuthConfEntityMap =
                            getAuthConsumeGroupInfo(topicName, operator,
                                    createDate, jsonObject, inGroupAuthConfEntityMap, sBuilder);
                } catch (Exception ee) {
                    sBuilder.delete(0, sBuilder.length());
                    throw new Exception(sBuilder.append("Process data exception, data is :")
                            .append(jsonObject.toString()).append(", exception is : ")
                            .append(ee.getMessage()).toString());
                }
            }
            if (inTopicAuthConfEntityMap.isEmpty()) {
                throw new Exception("Not found record in topicJsonSet parameter");
            }
            for (BdbTopicAuthControlEntity tmpTopicEntity : inTopicAuthConfEntityMap.values()) {
                brokerConfManage.confSetBdbTopicAuthControl(tmpTopicEntity);
            }
            for (BdbConsumerGroupEntity tmpGroupEntity : inGroupAuthConfEntityMap.values()) {
                brokerConfManage.confAddAllowedConsumerGroup(tmpGroupEntity);
            }
            sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\"}");
        } catch (Exception e) {
            sBuilder.delete(0, sBuilder.length());
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append(e.getMessage()).append("\"}");
        }
        return sBuilder;
    }

    /**
     * Delete topic authorization control
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminDeleteTopicAuthControl(HttpServletRequest req) throws Exception {
        StringBuilder sBuilder = new StringBuilder(512);
        try {
            WebParameterUtils.reqAuthorizenCheck(master, brokerConfManage,
                    req.getParameter("confModAuthToken"));
            String createUser =
                    WebParameterUtils.validStringParameter("createUser",
                            req.getParameter("createUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH,
                            true, "");
            Set<String> bathOpTopicNames =
                    WebParameterUtils.getBatchTopicNames(req.getParameter("topicName"),
                            true, false, null, sBuilder);
            for (String tmpTopicName : bathOpTopicNames) {
                BdbGroupFilterCondEntity webFilterCondEntity =
                        new BdbGroupFilterCondEntity();
                webFilterCondEntity.setTopicName(tmpTopicName);
                List<BdbGroupFilterCondEntity> webFilterCondEntities =
                        brokerConfManage.confGetBdbAllowedGroupFilterCondSet(webFilterCondEntity);
                if (!webFilterCondEntities.isEmpty()) {
                    webFilterCondEntity.setCreateUser(createUser);
                    brokerConfManage.confDelBdbAllowedGroupFilterCondSet(webFilterCondEntity);
                }
                BdbConsumerGroupEntity webConsumerGroupEntity =
                        new BdbConsumerGroupEntity();
                webConsumerGroupEntity.setGroupTopicName(tmpTopicName);
                List<BdbConsumerGroupEntity> webConsumerGroupEntities =
                        brokerConfManage.confGetBdbAllowedConsumerGroupSet(webConsumerGroupEntity);
                if (!webConsumerGroupEntities.isEmpty()) {
                    webConsumerGroupEntity.setRecordCreateUser(createUser);
                    brokerConfManage.confDelBdbAllowedConsumerGroupSet(webConsumerGroupEntity);
                }
                BdbTopicAuthControlEntity webTopicAuthControlEntity =
                        new BdbTopicAuthControlEntity();
                webTopicAuthControlEntity.setTopicName(tmpTopicName);
                webTopicAuthControlEntity.setCreateUser(createUser);
                brokerConfManage.confDeleteBdbTopicAuthControl(webTopicAuthControlEntity);
            }
            sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\"}");
        } catch (Exception e) {
            sBuilder.delete(0, sBuilder.length());
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append(e.getMessage()).append("\"}");
        }
        return sBuilder;
    }

    /**
     * Query topic authorization control
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminQueryTopicAuthControl(HttpServletRequest req) throws Exception {
        StringBuilder sBuilder = new StringBuilder(512);
        BdbTopicAuthControlEntity queryEntity =
                new BdbTopicAuthControlEntity();
        try {
            queryEntity
                    .setTopicName(WebParameterUtils.validStringParameter("topicName",
                            req.getParameter("topicName"),
                            TBaseConstants.META_MAX_TOPICNAME_LENGTH,
                            false, null));
            queryEntity
                    .setCreateUser(WebParameterUtils.validStringParameter("createUser",
                            req.getParameter("createUser"),
                            TBaseConstants.META_MAX_USERNAME_LENGTH,
                            false, null));
            List<BdbTopicAuthControlEntity> resultEntities =
                    brokerConfManage.confGetBdbTopicAuthCtrlEntityList(queryEntity);
            SimpleDateFormat formatter =
                    new SimpleDateFormat(TBaseConstants.META_TMP_DATE_VALUE);
            int i = 0;
            sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\",\"count\":")
                    .append(resultEntities.size()).append(",\"data\":[");
            for (BdbTopicAuthControlEntity entity : resultEntities) {
                if (i++ > 0) {
                    sBuilder.append(",");
                }
                sBuilder.append("{\"topicName\":\"").append(entity.getTopicName())
                        .append("\",\"isEnable\":").append(entity.isEnableAuthControl())
                        .append(",\"createUser\":\"").append(entity.getCreateUser())
                        .append("\",\"createDate\":\"").append(formatter.format(entity.getCreateDate()))
                        .append("\",\"authConsumeGroup\":[");
                BdbConsumerGroupEntity webConsumerGroupEntity =
                        new BdbConsumerGroupEntity();
                webConsumerGroupEntity.setGroupTopicName(entity.getTopicName());
                List<BdbConsumerGroupEntity> webConsumerGroupEntities =
                        brokerConfManage.confGetBdbAllowedConsumerGroupSet(webConsumerGroupEntity);
                int j = 0;
                if (!webConsumerGroupEntities.isEmpty()) {
                    for (BdbConsumerGroupEntity itemEntity : webConsumerGroupEntities) {
                        if (j++ > 0) {
                            sBuilder.append(",");
                        }
                        sBuilder.append("{\"topicName\":\"").append(itemEntity.getGroupTopicName())
                                .append("\",\"groupName\":\"")
                                .append(itemEntity.getConsumerGroupName())
                                .append("\",\"createUser\":\"")
                                .append(itemEntity.getRecordCreateUser())
                                .append("\",\"createDate\":\"")
                                .append(formatter.format(itemEntity.getRecordCreateDate()))
                                .append("\"}");
                    }
                }
                sBuilder.append("],\"groupCount\":").append(j)
                        .append(",\"authFilterCondSet\":[");
                BdbGroupFilterCondEntity webFilterCondEntity =
                        new BdbGroupFilterCondEntity();
                webFilterCondEntity.setTopicName(entity.getTopicName());
                List<BdbGroupFilterCondEntity> webFilterCondEntities =
                        brokerConfManage.confGetBdbAllowedGroupFilterCondSet(webFilterCondEntity);
                int y = 0;
                for (BdbGroupFilterCondEntity condEntity : webFilterCondEntities) {
                    if (y++ > 0) {
                        sBuilder.append(",");
                    }
                    sBuilder.append("{\"topicName\":\"").append(condEntity.getTopicName())
                            .append("\",\"groupName\":\"").append(condEntity.getConsumerGroupName())
                            .append("\",\"condStatus\":").append(condEntity.getControlStatus());
                    if (condEntity.getAttributes().length() <= 2) {
                        sBuilder.append(",\"filterConds\":\"\"");
                    } else {
                        sBuilder.append(",\"filterConds\":\"")
                                .append(condEntity.getAttributes())
                                .append("\"");
                    }
                    sBuilder.append(",\"createUser\":\"").append(condEntity.getCreateUser())
                            .append("\",\"createDate\":\"").append(formatter.format(condEntity.getCreateDate()))
                            .append("\"}");
                }
                sBuilder.append("],\"filterCount\":").append(y).append("}");
            }
            sBuilder.append("]}");
        } catch (Exception e) {
            sBuilder.delete(0, sBuilder.length());
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append(e.getMessage()).append("\",\"count\":0,\"data\":[]}");
        }
        return sBuilder;
    }

    /**
     * Private method to get authorized consumer group info
     *
     * @param topicName
     * @param operator
     * @param createDate
     * @param jsonObject
     * @param groupAuthEntityMap
     * @param sBuilder
     * @return
     * @throws Exception
     */
    private HashMap<String, BdbConsumerGroupEntity> getAuthConsumeGroupInfo(
            final String topicName,
            final String operator,
            final Date createDate,
            final Map<String, Object> jsonObject,
            HashMap<String, BdbConsumerGroupEntity> groupAuthEntityMap,
            final StringBuilder sBuilder) throws Exception {
        String strAuthConsumGroup = (String) jsonObject.get("authConsumeGroup");
        if ((strAuthConsumGroup != null) && (!TStringUtils.isBlank(strAuthConsumGroup))) {
            List<Map<String, String>> authConsumeGroupSet =
                    JSON.parseObject(strAuthConsumGroup, new TypeReference<List<Map<String, String>>>() {});
            if ((authConsumeGroupSet != null)
                    && (!authConsumeGroupSet.isEmpty())) {
                for (int j = 0; j < authConsumeGroupSet.size(); j++) {
                    Map<String, String> groupObject = authConsumeGroupSet.get(j);
                    String groupName =
                            WebParameterUtils.validStringParameter("groupName",
                                    groupObject.get("groupName"),
                                    TBaseConstants.META_MAX_GROUPNAME_LENGTH,
                                    true, "");
                    String groupTopicName =
                            WebParameterUtils.validStringParameter("topicName",
                                    groupObject.get("topicName"),
                                    TBaseConstants.META_MAX_TOPICNAME_LENGTH,
                                    false, topicName);
                    if (!groupTopicName.equals(topicName)) {
                        throw new Exception("TopicName not equal in authConsumeGroup!");
                    }
                    String groupCreateUser =
                            WebParameterUtils.validStringParameter("createUser",
                                    groupObject.get("createUser"),
                                    TBaseConstants.META_MAX_USERNAME_LENGTH,
                                    false, null);
                    Date groupCreateDate =
                            WebParameterUtils.validDateParameter("createDate",
                                    groupObject.get("createDate"),
                                    TBaseConstants.META_MAX_DATEVALUE_LENGTH,
                                    false, null);
                    if ((TStringUtils.isBlank(groupCreateUser))
                            || (groupCreateDate == null)) {
                        groupCreateUser = operator;
                        groupCreateDate = createDate;
                    }
                    String recordKey = sBuilder.append(groupName)
                            .append("-").append(groupTopicName).toString();
                    sBuilder.delete(0, sBuilder.length());
                    groupAuthEntityMap.put(recordKey,
                            new BdbConsumerGroupEntity(topicName,
                                    groupName, groupCreateUser, groupCreateDate));
                }
            }
        }
        return groupAuthEntityMap;
    }
}

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

package com.tencent.tubemq.server.master.bdbstore.bdbentitys;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.TokenConstants;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.server.common.TServerConstants;
import com.tencent.tubemq.server.common.utils.WebParameterUtils;
import java.io.Serializable;
import java.util.Date;
import org.apache.commons.lang.builder.ToStringBuilder;


@Entity
public class BdbGroupFlowCtrlEntity implements Serializable {
    private static final long serialVersionUID = 2533735122504168321L;
    @PrimaryKey
    private String groupName;           //group name
    private long serialId = -1L;        //serial id
    private int statusId = -1;         // 0:not active； 1：active
    private String flowCtrlInfo;
    private int ruleCnt = 0;            //flow control rule count
    private long ssdTranslateId = System.currentTimeMillis();
    private boolean needSSDProc = false;    //ssd
    private String attributes;          //extra attributes
    private String createUser;          //create user
    private Date createDate;            //create date

    public BdbGroupFlowCtrlEntity() {

    }

    //Constructor
    public BdbGroupFlowCtrlEntity(final String flowCtrlInfo, final int statusId,
                                  final int ruleCnt, final int qryPriorityId,
                                  final String attributes, final boolean curNeedSSDProc,
                                  final String createUser, final Date createDate) {
        this.statusId = statusId;
        this.groupName = TServerConstants.TOKEN_DEFAULT_FLOW_CONTROL;
        this.serialId = System.currentTimeMillis();
        this.flowCtrlInfo = flowCtrlInfo;
        this.attributes = attributes;
        this.ruleCnt = ruleCnt;
        this.ssdTranslateId = System.currentTimeMillis();
        this.needSSDProc = curNeedSSDProc;
        this.createUser = createUser;
        this.createDate = createDate;
        this.setQryPriorityId(qryPriorityId);
    }

    //Constructor
    public BdbGroupFlowCtrlEntity(final String groupName, final String flowCtrlInfo,
                                  final int statusId, final int ruleCnt,
                                  final int qryPriorityId, final String attributes,
                                  final boolean needSSDProc, final String createUser,
                                  final Date createDate) {
        this.groupName = groupName;
        this.serialId = System.currentTimeMillis();
        this.statusId = statusId;
        this.flowCtrlInfo = flowCtrlInfo;
        this.attributes = attributes;
        this.ruleCnt = ruleCnt;
        this.createUser = createUser;
        this.createDate = createDate;
        this.needSSDProc = needSSDProc;
        this.ssdTranslateId = TBaseConstants.META_VALUE_UNDEFINED;
        this.setQryPriorityId(qryPriorityId);
    }

    //Constructor
    public BdbGroupFlowCtrlEntity(final String groupName, final String flowCtrlInfo,
                                  final int statusId, final int ruleCnt,
                                  final String attributes, final long ssdTranslateId,
                                  final boolean needSSDProc, final String createUser,
                                  final Date createDate) {
        this.groupName = groupName;
        this.serialId = System.currentTimeMillis();
        this.statusId = statusId;
        this.flowCtrlInfo = flowCtrlInfo;
        this.attributes = attributes;
        this.ruleCnt = ruleCnt;
        this.createUser = createUser;
        this.createDate = createDate;
        this.needSSDProc = needSSDProc;
        this.ssdTranslateId = ssdTranslateId;
    }

    public long getSsdTranslateId() {
        return ssdTranslateId;
    }

    public int getRuleCnt() {
        return ruleCnt;
    }

    public void setRuleCnt(int ruleCnt) {
        this.ruleCnt = ruleCnt;
    }

    public long getSerialId() {
        return serialId;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getAttributes() {
        return attributes;
    }

    public void setAttributes(String attributes) {
        this.attributes = attributes;
    }

    public String getCreateUser() {
        return createUser;
    }

    public void setCreateUser(String createUser) {
        this.createUser = createUser;
    }

    public boolean isNeedSSDProc() {
        return needSSDProc;
    }

    public void setNeedSSDProc(boolean needSSDProc) {
        this.needSSDProc = needSSDProc;
    }

    public Date getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    public String getFlowCtrlInfo() {
        return flowCtrlInfo;
    }

    public void setFlowCtrlInfo(String flowCtrlInfo) {
        this.flowCtrlInfo = flowCtrlInfo;
        this.serialId = System.currentTimeMillis();
    }

    public int getStatusId() {
        return statusId;
    }

    public void setStatusId(int statusId) {
        this.statusId = statusId;
        this.serialId = System.currentTimeMillis();
    }

    public boolean isValidStatus() {
        return (statusId != 0);
    }

    public int getQryPriorityId() {
        String atrVal =
                TStringUtils.getAttrValFrmAttributes(this.attributes,
                        TokenConstants.TOKEN_QRY_PRIORITY_ID);
        if (atrVal != null) {
            return Integer.valueOf(atrVal);
        }
        return 0;
    }

    public void setQryPriorityId(int qryPriorityId) {
        this.attributes =
                TStringUtils.setAttrValToAttributes(this.attributes,
                        TokenConstants.TOKEN_QRY_PRIORITY_ID,
                        String.valueOf(qryPriorityId));
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("groupName", groupName)
                .append("serialId", serialId)
                .append("statusId", statusId)
                .append("flowCtrlInfo", ".....")
                .append("ruleCnt", ruleCnt)
                .append("ssdTranslateId", ssdTranslateId)
                .append("needSSDProc", needSSDProc)
                .append("attributes", attributes)
                .append("createUser", createUser)
                .append("createDate", createDate)
                .toString();
    }

    /**
     * Serialize config field to json format
     *
     * @param sBuilder
     * @return
     */
    public StringBuilder toJsonString(final StringBuilder sBuilder) {
        return sBuilder.append("{\"type\":\"BdbGroupFlowCtrlEntity\",")
                .append("\"groupName\":\"").append(groupName)
                .append("\",\"statusId\":").append(statusId)
                .append(",\"ssdTranslateId\":").append(ssdTranslateId)
                .append(",\"ruleCnt\":").append(ruleCnt)
                .append(",\"needSSDProc\":").append(needSSDProc)
                .append(",\"serialId\":").append(serialId)
                .append(",\"qryPriorityId\":").append(getQryPriorityId())
                .append(",\"flowCtrlInfo\":").append(flowCtrlInfo)
                .append(", \"attributes\":\"").append(attributes)
                .append("\", \"createUser\":\"").append(createUser)
                .append("\",\"createDate\":\"")
                .append(WebParameterUtils.date2yyyyMMddHHmmss(createDate))
                .append("\"}");
    }
}

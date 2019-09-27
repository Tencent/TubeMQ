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
import com.tencent.tubemq.server.common.utils.WebParameterUtils;
import java.io.Serializable;
import java.util.Date;
import org.apache.commons.lang.builder.ToStringBuilder;


@Entity
public class BdbConsumeGroupSettingEntity implements Serializable {

    private static final long serialVersionUID = 6801442997689232316L;
    @PrimaryKey
    private String consumeGroupName;
    private int enableBind = -2;   // -2: undefine; 0: not started，1: started
    private Date lastBindUsedDate;
    private int allowedBrokerClientRate = -2;
    private String attributes;
    private String createUser;
    private Date createDate;


    public BdbConsumeGroupSettingEntity() {

    } // Needed for deserialization.

    public BdbConsumeGroupSettingEntity(String consumeGroupName, int enableBind,
                                        int allowedBrokerClientRate, String attributes,
                                        String createUser, Date createDate) {
        this.consumeGroupName = consumeGroupName;
        this.enableBind = enableBind;
        this.allowedBrokerClientRate = allowedBrokerClientRate;
        this.attributes = attributes;
        this.createUser = createUser;
        this.createDate = createDate;
    }

    public BdbConsumeGroupSettingEntity(BdbConsumeGroupSettingEntity otherEntity) {
        this.consumeGroupName = otherEntity.getConsumeGroupName();
        this.enableBind = otherEntity.getEnableBind();
        this.allowedBrokerClientRate = otherEntity.getAllowedBrokerClientRate();
        this.attributes = otherEntity.getAttributes();
        this.createUser = otherEntity.getCreateUser();
        this.createDate = otherEntity.getCreateDate();
        this.lastBindUsedDate = otherEntity.getLastBindUsedDate();
    }

    public String getConsumeGroupName() {
        return consumeGroupName;
    }

    public void setConsumeGroupName(String consumeGroupName) {
        this.consumeGroupName = consumeGroupName;
    }

    public int getEnableBind() {
        return enableBind;
    }

    public void setEnableBind(int enableBind) {
        this.enableBind = enableBind;
    }

    public Date getLastBindUsedDate() {
        return lastBindUsedDate;
    }

    public void setLastBindUsedDate(Date lastBindUsedDate) {
        this.lastBindUsedDate = lastBindUsedDate;
    }

    public int getAllowedBrokerClientRate() {
        return allowedBrokerClientRate;
    }

    public void setAllowedBrokerClientRate(int allowedBrokerClientRate) {
        this.allowedBrokerClientRate = allowedBrokerClientRate;
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

    public Date getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    public void setLastUsedDateNow() {
        this.lastBindUsedDate = new Date();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("consumeGroupName", consumeGroupName)
                .append("enableBind", enableBind)
                .append("lastBindUsedDate", lastBindUsedDate)
                .append("allowedBrokerClientRate", allowedBrokerClientRate)
                .append("attributes", attributes)
                .append("createUser", createUser)
                .append("createDate", createDate)
                .toString();
    }

    public StringBuilder toJsonString(final StringBuilder sBuilder) {
        return sBuilder.append("{\"type\":\"BdbConsumeGroupSettingEntity\",")
                .append("\"consumeGroupName\":\"").append(consumeGroupName)
                .append("\",\"enableBind\":").append(enableBind)
                .append(",\"allowedBrokerClientRate\":").append(allowedBrokerClientRate)
                .append(",\"attributes\":\"").append(attributes)
                .append("\",\"createUser\":\"").append(createUser)
                .append("\",\"createDate\":\"")
                .append(WebParameterUtils.date2yyyyMMddHHmmss(createDate))
                .append("\"}");
    }
}

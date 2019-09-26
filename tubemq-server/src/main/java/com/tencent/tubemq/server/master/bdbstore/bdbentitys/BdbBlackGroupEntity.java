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
import com.tencent.tubemq.corebase.TokenConstants;
import com.tencent.tubemq.server.common.utils.WebParameterUtils;
import java.io.Serializable;
import java.util.Date;
import org.apache.commons.lang.builder.ToStringBuilder;


@Entity
public class BdbBlackGroupEntity implements Serializable {

    private static final long serialVersionUID = 6081445397689294316L;
    @PrimaryKey
    private String recordKey;
    private String topicName;
    private String consumerGroupName;
    private String createUser;
    private Date createDate;
    private String attributes;

    public BdbBlackGroupEntity() {

    }

    public BdbBlackGroupEntity(String topicName, String consumerGroupName,
                               String createUser, Date createDate) {
        this.recordKey = new StringBuilder(512).append(topicName)
                .append(TokenConstants.ATTR_SEP).append(consumerGroupName).toString();
        this.topicName = topicName;
        this.consumerGroupName = consumerGroupName;
        this.createUser = createUser;
        this.createDate = createDate;
    }

    public String getAttributes() {
        return attributes;
    }

    public void setAttributes(String attributes) {
        this.attributes = attributes;
    }

    public String getBlackRecordKey() {
        return recordKey;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getBlackGroupName() {
        return consumerGroupName;
    }

    public void setBlackGroupName(String consumerGroupName) {
        this.consumerGroupName = consumerGroupName;
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

    public StringBuilder toJsonString(final StringBuilder sBuilder) {
        return sBuilder.append("{\"type\":\"BdbBlackGroupEntity\",")
                .append("\"recordKey\":\"").append(recordKey)
                .append("\",\"topicName\":\"").append(topicName)
                .append("\",\"consumerGroupName\":\"").append(consumerGroupName)
                .append("\",\"attributes\":\"").append(attributes)
                .append("\",\"createUser\":\"").append(createUser)
                .append("\",\"createDate\":\"")
                .append(WebParameterUtils.date2yyyyMMddHHmmss(createDate))
                .append("\"}");
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("recordKey", recordKey)
                .append("topicName", topicName)
                .append("consumerGroupName", consumerGroupName)
                .append("createUser", createUser)
                .append("createDate", createDate)
                .append("attributes", attributes)
                .toString();
    }
}

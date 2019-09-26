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
public class BdbTopicAuthControlEntity implements Serializable {

    private static final long serialVersionUID = 7356175918639562340L;
    @PrimaryKey
    private String topicName;
    private int enableAuthControl = -1; // -1 : undefine； 0：disable， 1：enable
    private String createUser;
    private Date createDate;
    private String attributes;

    public BdbTopicAuthControlEntity() {

    }

    public BdbTopicAuthControlEntity(String topicName, boolean enableAuthControl,
                                     String createUser, Date createDate) {
        this.topicName = topicName;
        if (enableAuthControl) {
            this.enableAuthControl = 1;
        } else {
            this.enableAuthControl = 0;
        }
        this.createUser = createUser;
        this.createDate = createDate;
    }

    public String getAttributes() {
        return attributes;
    }

    public void setAttributes(String attributes) {
        this.attributes = attributes;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public boolean isEnableAuthControl() {
        if (enableAuthControl == 1) {
            return true;
        }
        return false;
    }

    public int getEnableAuthControl() {
        return this.enableAuthControl;
    }

    public void setEnableAuthControl(boolean enableAuthControl) {
        if (enableAuthControl) {
            this.enableAuthControl = 1;
        } else {
            this.enableAuthControl = 0;
        }
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
        return sBuilder.append("{\"type\":\"BdbConsumerGroupEntity\",")
                .append("\"topicName\":\"").append(topicName)
                .append("\",\"enableAuthControl\":\"").append(enableAuthControl)
                .append("\",\"createUser\":\"").append(createUser)
                .append("\",\"createDate\":\"")
                .append(WebParameterUtils.date2yyyyMMddHHmmss(createDate))
                .append("\",\"attributes\":\"").append(attributes).append("\"}");
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("topicName", topicName)
                .append("enableAuthControl", enableAuthControl)
                .append("createUser", createUser)
                .append("createDate", createDate)
                .append("attributes", attributes)
                .toString();
    }
}

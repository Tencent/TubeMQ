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
public class BdbBrokerConfEntity implements Serializable {
    private static final long serialVersionUID = 3961934697293763691L;

    @PrimaryKey
    private int brokerId = -2;
    private String brokerIp;
    private int brokerPort = -2;
    private String brokerAddress; // broker ip:port
    private String brokerFullInfo; // broker brokerId:ip:port
    private String brokerSimpleInfo; // broker brokerId:ip:
    private int regionId = -2;
    private int manageStatus = -2; // broker status, -2:undefine, 1:Pending for approval, 5:online, 7:offline
    private int numPartitions = -2; // number of partitions
    private int unflushThreshold = -2;  //flush threshold
    private int unflushInterval = -2;   //flush interval
    private String deleteWhen;  //delete policy execute time
    private String deletePolicy;    //delete policy
    private int dataStoreType = -2; //date store type
    private String dataPath;    //data path
    private String attributes;  //extra attributes
    private boolean acceptPublish = true;   //enable publish
    private boolean acceptSubscribe = true; //enable subscribe
    private boolean isConfDataUpdated = false;  //conf data update flag
    private boolean isBrokerLoaded = false; //broker conf load flag
    private String createUser;  //broker create user
    private Date createDate;    //broker create date
    private String modifyUser;  //broker modify user
    private Date modifyDate;    //broker modify date
    private String brokerTLSSimpleInfo; //tls simple info
    private String brokerTLSFullInfo;   //tls full info

    public BdbBrokerConfEntity() {
    }

    //Constructor
    public BdbBrokerConfEntity(final int brokerId, final String brokerIp,
                               final int brokerPort, final int numPartitions,
                               final int unflushThreshold, final int unflushInterval,
                               final String deleteWhen, final String deletePolicy,
                               final int manageStatus, final boolean acceptPublish,
                               final boolean acceptSubscribe, final String attributes,
                               final boolean isConfDataUpdated, final boolean isBrokerLoaded,
                               final String createUser, final Date createDate,
                               final String modifyUser, final Date modifyDate) {
        this.brokerId = brokerId;
        this.brokerIp = brokerIp;
        this.brokerPort = brokerPort;
        this.manageStatus = manageStatus;
        this.numPartitions = numPartitions;
        this.unflushThreshold = unflushThreshold;
        this.unflushInterval = unflushInterval;
        this.deleteWhen = deleteWhen;
        this.deletePolicy = deletePolicy;
        this.acceptPublish = acceptPublish;
        this.acceptSubscribe = acceptSubscribe;
        this.isConfDataUpdated = isConfDataUpdated;
        this.isBrokerLoaded = isBrokerLoaded;
        this.createUser = createUser;
        this.createDate = createDate;
        this.modifyUser = modifyUser;
        this.modifyDate = modifyDate;
        this.attributes = attributes;
        this.buildStrInfo();
    }

    /**
     * Serialize config field to json format
     */
    public StringBuilder toJsonString(final StringBuilder sb) {
        return sb.append("{\"type\":\"BdbBrokerConfEntity\",")
                .append("\"brokerId\":\"").append(brokerId)
                .append("\",\"brokerAddress\":\"").append(brokerAddress)
                .append("\",\"manageStatus\":\"").append(manageStatus)
                .append("\",\"numPartitions\":").append(numPartitions)
                .append(",\"unflushThreshold\":").append(unflushThreshold)
                .append(",\"unflushInterval\":").append(unflushInterval)
                .append(",\"deleteWhen\":\"").append(deleteWhen)
                .append("\",\"deletePolicy\":\"").append(deletePolicy)
                .append("\",\"manageStatus\":").append(manageStatus)
                .append(",\"acceptPublish\":").append(acceptPublish)
                .append(",\"acceptSubscribe\":").append(acceptSubscribe)
                .append(",\"isConfDataUpdated\":").append(isConfDataUpdated)
                .append(",\"isBrokerLoaded\":").append(isBrokerLoaded)
                .append(",\"numTopicStores\":").append(getNumTopicStores())
                .append(",\"dataPath\":\"").append(dataPath)
                .append("\",\"unFlushDataHold\":").append(getDftUnFlushDataHold())
                .append(",\"memCacheMsgSizeInMB\":").append(getDftMemCacheMsgSizeInMB())
                .append(",\"memCacheMsgCntInK\":").append(getDftMemCacheMsgCntInK())
                .append(",\"memCacheFlushIntvl\":").append(getDftMemCacheFlushIntvl())
                .append(",\"createUser\":\"").append(createUser)
                .append("\",\"createDate\":\"")
                .append(WebParameterUtils.date2yyyyMMddHHmmss(createDate))
                .append("\",\"modifyUser\":\"").append(modifyUser)
                .append("\",\"modifyDate\":\"")
                .append(WebParameterUtils.date2yyyyMMddHHmmss(modifyDate))
                .append("\"}");
    }

    /**
     * Get broker config string
     *
     * @return config string
     */
    public String getBrokerDefaultConfInfo() {
        return new StringBuilder(512).append(this.getDftNumPartitions())
                .append(TokenConstants.ATTR_SEP).append(this.isAcceptPublish())
                .append(TokenConstants.ATTR_SEP).append(this.isAcceptSubscribe())
                .append(TokenConstants.ATTR_SEP).append(this.getDftUnflushThreshold())
                .append(TokenConstants.ATTR_SEP).append(this.getDftUnflushInterval())
                .append(TokenConstants.ATTR_SEP).append(this.getDftDeleteWhen())
                .append(TokenConstants.ATTR_SEP).append(this.getDftDeletePolicy())
                .append(TokenConstants.ATTR_SEP).append(this.getNumTopicStores())
                .append(TokenConstants.ATTR_SEP).append(this.getDftUnFlushDataHold())
                .append(TokenConstants.ATTR_SEP).append(this.getDftMemCacheMsgSizeInMB())
                .append(TokenConstants.ATTR_SEP).append(this.getDftMemCacheMsgCntInK())
                .append(TokenConstants.ATTR_SEP).append(this.getDftMemCacheFlushIntvl()).toString();
    }

    public void setConfDataUpdated() {
        this.isBrokerLoaded = false;
        this.isConfDataUpdated = true;
    }

    public boolean isConfDataUpdated() {
        return this.isConfDataUpdated;
    }

    public boolean isBrokerLoaded() {
        return this.isBrokerLoaded;
    }

    public String getAttributes() {
        return attributes;
    }

    public void setAttributes(String attributes) {
        this.attributes = attributes;
    }

    public String getDataPath() {
        return dataPath;
    }

    public void setDataStore(int dataStoreType, String dataPath) {
        this.dataPath = dataPath;
        this.dataStoreType = dataStoreType;
    }

    public int getDataStoreType() {
        return dataStoreType;
    }


    public void setBrokerLoaded() {
        this.isBrokerLoaded = true;
        this.isConfDataUpdated = false;
    }

    public String getSimpleBrokerInfo() {
        if (this.brokerPort == TBaseConstants.META_DEFAULT_BROKER_PORT) {
            return this.brokerSimpleInfo;
        } else {
            return this.brokerFullInfo;
        }
    }

    public String getSimpleTLSBrokerInfo() {
        if (getBrokerTLSPort() == TBaseConstants.META_DEFAULT_BROKER_PORT) {
            return this.brokerTLSSimpleInfo;
        } else {
            return this.brokerTLSFullInfo;
        }
    }

    public String getBrokerIdAndAddress() {
        return this.brokerFullInfo;
    }

    public int getDftUnflushThreshold() {
        return this.unflushThreshold;
    }

    public void setDftUnflushThreshold(int unflushThreshold) {
        this.unflushThreshold = unflushThreshold;
    }

    public int getDftUnflushInterval() {
        return this.unflushInterval;
    }

    public void setDftUnflushInterval(int unflushInterval) {
        this.unflushInterval = unflushInterval;
    }

    public String getDftDeleteWhen() {
        return this.deleteWhen;
    }

    public void setDftDeleteWhen(String deleteWhen) {
        this.deleteWhen = deleteWhen;
    }

    public String getDftDeletePolicy() {
        return this.deletePolicy;
    }

    public void setDftDeletePolicy(String deletePolicy) {
        this.deletePolicy = deletePolicy;
    }

    public int getManageStatus() {
        return manageStatus;
    }

    public void setManageStatus(int manageStatus) {
        this.manageStatus = manageStatus;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(int brokerId) {
        this.brokerId = brokerId;
    }

    public int getNumTopicStores() {
        String atrVal =
                TStringUtils.getAttrValFrmAttributes(this.attributes,
                        TokenConstants.TOKEN_STORE_NUM);
        if (atrVal != null) {
            return Integer.valueOf(atrVal);
        }
        return 1;
    }

    public int getDftMemCacheMsgCntInK() {
        String atrVal =
                TStringUtils.getAttrValFrmAttributes(this.attributes,
                        TokenConstants.TOKEN_MCACHE_MSG_CNT);
        if (atrVal != null) {
            return Integer.valueOf(atrVal);
        }
        return 10;
    }

    public void setDftMemCacheMsgCntInK(final int memCacheMsgCntInK) {
        this.attributes =
                TStringUtils.setAttrValToAttributes(this.attributes,
                        TokenConstants.TOKEN_MCACHE_MSG_CNT,
                        String.valueOf(memCacheMsgCntInK));
    }

    public int getDftMemCacheMsgSizeInMB() {
        String atrVal =
                TStringUtils.getAttrValFrmAttributes(this.attributes,
                        TokenConstants.TOKEN_MCACHE_MSG_SIZE);
        if (atrVal != null) {
            return Integer.valueOf(atrVal);
        }
        return 2;
    }

    public void setDftMemCacheMsgSizeInMB(final int memCacheMsgSizeInMB) {
        this.attributes =
                TStringUtils.setAttrValToAttributes(this.attributes,
                        TokenConstants.TOKEN_MCACHE_MSG_SIZE,
                        String.valueOf(memCacheMsgSizeInMB));
    }

    public int getDftMemCacheFlushIntvl() {
        String atrVal =
                TStringUtils.getAttrValFrmAttributes(this.attributes,
                        TokenConstants.TOKEN_MCACHE_FLUSH_INTVL);
        if (atrVal != null) {
            return Integer.valueOf(atrVal);
        }
        return 20000;
    }

    public void setDftMemCacheFlushIntvl(final int memCacheFlushIntvl) {
        this.attributes =
                TStringUtils.setAttrValToAttributes(this.attributes,
                        TokenConstants.TOKEN_MCACHE_FLUSH_INTVL,
                        String.valueOf(memCacheFlushIntvl));
    }

    public int getDftUnFlushDataHold() {
        String atrVal =
                TStringUtils.getAttrValFrmAttributes(this.attributes,
                        TokenConstants.TOKEN_DATA_UNFLUSHHOLD);
        if (atrVal != null) {
            return Integer.valueOf(atrVal);
        }
        return TServerConstants.CFG_DEFAULT_DATA_UNFLUSH_HOLD;
    }

    public void setDftUnFlushDataHold(final int unFlushDataHold) {
        this.attributes =
                TStringUtils.setAttrValToAttributes(this.attributes,
                        TokenConstants.TOKEN_DATA_UNFLUSHHOLD,
                        String.valueOf(unFlushDataHold));
    }

    public int getBrokerTLSPort() {
        String atrVal =
                TStringUtils.getAttrValFrmAttributes(this.attributes,
                        TokenConstants.TOKEN_TLS_PORT);
        if (atrVal != null) {
            return Integer.valueOf(atrVal);
        }
        return TBaseConstants.META_DEFAULT_BROKER_TLS_PORT;
    }

    public void setBrokerTLSPort(final int brokerTLSPort) {
        this.attributes =
                TStringUtils.setAttrValToAttributes(this.attributes,
                        TokenConstants.TOKEN_TLS_PORT,
                        String.valueOf(brokerTLSPort));
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    public String getBrokerIp() {
        return brokerIp;
    }

    public void setBrokerIp(String brokerIp) {
        this.brokerIp = brokerIp;
    }

    public int getBrokerPort() {
        return brokerPort;
    }

    public int getDftNumPartitions() {
        return numPartitions;
    }

    public void setDftNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public String getRecordCreateUser() {
        return createUser;
    }

    public void setRecordCreateUser(String createUser) {
        this.createUser = createUser;
    }

    public Date getRecordCreateDate() {
        return createDate;
    }

    public void setRecordCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    public String getRecordModifyUser() {
        return modifyUser;
    }

    public void setRecordModifyUser(String modifyUser) {
        this.modifyUser = modifyUser;
    }

    public Date getRecordModifyDate() {
        return modifyDate;
    }

    public void setRecordModifyDate(Date modifyDate) {
        this.modifyDate = modifyDate;
    }

    public boolean isAcceptPublish() {
        return acceptPublish;
    }

    public void setDftAcceptPublish(boolean acceptPublish) {
        this.acceptPublish = acceptPublish;
    }

    public boolean isAcceptSubscribe() {
        return acceptSubscribe;
    }

    public void setDftAcceptSubscribe(boolean acceptSubscribe) {
        this.acceptSubscribe = acceptSubscribe;
    }

    public void setBrokerIpAndPort(String brokerIp, int brokerPort) {
        this.brokerPort = brokerPort;
        this.brokerIp = brokerIp;
        this.buildStrInfo();
    }

    public void appendAttributes(String attrKey, String attrVal) {
        this.attributes =
                TStringUtils.setAttrValToAttributes(this.attributes, attrKey, attrVal);
    }

    private void buildStrInfo() {
        StringBuilder sBuilder = new StringBuilder(512);
        this.brokerAddress = sBuilder.append(this.brokerIp)
                .append(TokenConstants.ATTR_SEP)
                .append(this.brokerPort).toString();
        sBuilder.delete(0, sBuilder.length());
        this.brokerSimpleInfo = sBuilder.append(this.brokerId)
                .append(TokenConstants.ATTR_SEP).append(this.brokerIp)
                .append(TokenConstants.ATTR_SEP).append(" ").toString();
        sBuilder.delete(0, sBuilder.length());
        this.brokerFullInfo = sBuilder.append(this.brokerId)
                .append(TokenConstants.ATTR_SEP).append(this.brokerIp)
                .append(TokenConstants.ATTR_SEP).append(this.brokerPort).toString();
        sBuilder.delete(0, sBuilder.length());
        this.brokerTLSSimpleInfo = sBuilder.append(this.brokerId)
                .append(TokenConstants.ATTR_SEP).append(this.brokerIp)
                .append(TokenConstants.ATTR_SEP).append(" ").toString();
        sBuilder.delete(0, sBuilder.length());
        this.brokerTLSFullInfo = sBuilder.append(this.brokerId)
                .append(TokenConstants.ATTR_SEP).append(this.brokerIp)
                .append(TokenConstants.ATTR_SEP).append(getBrokerTLSPort()).toString();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("brokerId", brokerId)
                .append("brokerIp", brokerIp)
                .append("brokerPort", brokerPort)
                .append("brokerAddress", brokerAddress)
                .append("brokerFullInfo", brokerFullInfo)
                .append("brokerSimpleInfo", brokerSimpleInfo)
                .append("regionId", regionId)
                .append("manageStatus", manageStatus)
                .append("numPartitions", numPartitions)
                .append("unflushThreshold", unflushThreshold)
                .append("unflushInterval", unflushInterval)
                .append("deleteWhen", deleteWhen)
                .append("deletePolicy", deletePolicy)
                .append("dataStoreType", dataStoreType)
                .append("dataPath", dataPath)
                .append("attributes", attributes)
                .append("acceptPublish", acceptPublish)
                .append("acceptSubscribe", acceptSubscribe)
                .append("isConfDataUpdated", isConfDataUpdated)
                .append("isBrokerLoaded", isBrokerLoaded)
                .append("createUser", createUser)
                .append("createDate", createDate)
                .append("modifyUser", modifyUser)
                .append("modifyDate", modifyDate)
                .append("brokerTLSSimpleInfo", brokerTLSSimpleInfo)
                .append("brokerTLSFullInfo", brokerTLSFullInfo)
                .toString();
    }
}

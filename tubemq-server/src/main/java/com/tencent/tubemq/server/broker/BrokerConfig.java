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

package com.tencent.tubemq.server.broker;

import static java.lang.Math.abs;
import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.config.TLSConfig;
import com.tencent.tubemq.corebase.utils.AddressUtils;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import com.tencent.tubemq.server.broker.utils.DataStoreUtils;
import com.tencent.tubemq.server.common.TServerConstants;
import com.tencent.tubemq.server.common.fileconfig.AbstractFileConfig;
import com.tencent.tubemq.server.common.fileconfig.ZKConfig;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.ini4j.Ini;
import org.ini4j.Profile.Section;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Config of broker. Read from broker.ini config file.
 */
public class BrokerConfig extends AbstractFileConfig {
    static final long serialVersionUID = -1L;
    private static final Logger logger = LoggerFactory.getLogger(BrokerConfig.class);
    // broker id
    private int brokerId = 0;
    // broker hostname
    private String hostName;
    // broker port
    private int port = TBaseConstants.META_DEFAULT_BROKER_PORT;
    // broker web service port
    private int webPort = 8081;
    // master service address
    private String masterAddressList;
    private String primaryPath;
    private String secondDataPath;
    // tcp write service thread count
    private int tcpWriteServiceThread =
            Runtime.getRuntime().availableProcessors() * 2;
    // tcp read service thread count
    private int tcpReadServiceThread =
            Runtime.getRuntime().availableProcessors() * 2;
    // tls write service thread count
    private int tlsWriteServiceThread =
            Runtime.getRuntime().availableProcessors() * 2;
    // tls read service thread count
    private int tlsReadServiceThread =
            Runtime.getRuntime().availableProcessors() * 2;
    private long defaultDeduceReadSize = 7 * 1024 * 1024 * 1024L;
    private long defaultDoubleDeduceReadSize = this.defaultDeduceReadSize * 2;
    // max data segment size
    private int maxSegmentSize = 1024 * 1024 * 1024;
    // max index segment size
    private int maxIndexSegmentSize = 700000 * DataStoreUtils.STORE_INDEX_HEAD_LEN;
    // transfer size
    private int transferSize = 512 * 1024;
    // transfer index count
    private int indexTransCount = 1000;
    // rpc read timeout in milliseconds
    private long rpcReadTimeoutMs = 10 * 1000;
    // max ssd file count
    private int maxSSDTotalFileCnt = 70;
    // max ssd file size
    private long maxSSDTotalFileSizes = 32212254720L;
    // consumer register timeout in milliseconds
    private int consumerRegTimeoutMs = 30000;
    private boolean updateConsumerOffsets = true;
    // heartbeat interval in milliseconds
    private long heartbeatPeriodMs = 8000L;
    // quartz thread count
    private int quartzThreadCount = 5;
    // netty write buffer high water mark
    private long nettyWriteBufferHighWaterMark = 10 * 1024 * 1024;
    // netty write buffer low water mark
    private long nettyWriteBufferLowWaterMark = 5 * 1024 * 1024;
    // log cleanup interval in milliseconds
    private long logClearupDurationMs = 30 * 60 * 1000;
    // log flush to disk interval in milliseconds
    private long logFlushDiskDurMs = 20 * 1000;
    // memory flush to disk interval in milliseconds
    private long logFlushMemDurMs = 10 * 1000;
    // socket send buffer
    private long socketSendBuffer = -1;
    // socket receive buffer
    private long socketRecvBuffer = -1;
    // read io exception max count
    private int allowedReadIOExcptCnt = 10;
    // write io exception max count
    private int allowedWriteIOExcptCnt = 10;
    private long ioExcptStatsDurationMs = 120000;
    // row lock wait duration
    private int rowLockWaitDurMs =
            TServerConstants.CFG_ROWLOCK_DEFAULT_DURATION;
    // zookeeper config
    private ZKConfig zkConfig = new ZKConfig();
    // tls config
    private TLSConfig tlsConfig = new TLSConfig();
    private boolean visitMasterAuth = false;
    private String visitName = "";
    private String visitPassword = "";
    private long authValidTimeStampPeriodMs = TBaseConstants.CFG_DEFAULT_AUTH_TIMESTAMP_VALID_INTERVAL;

    public BrokerConfig() {
        super();
    }

    public int getQuartzThreadCount() {
        return this.quartzThreadCount;
    }


    public boolean isUpdateConsumerOffsets() {
        return this.updateConsumerOffsets;
    }

    public int getConsumerRegTimeoutMs() {
        return consumerRegTimeoutMs;
    }

    public long getHeartbeatPeriodMs() {
        return heartbeatPeriodMs;
    }

    public ZKConfig getZkConfig() {
        return this.zkConfig;
    }

    public TLSConfig getTlsConfig() {
        return this.tlsConfig;
    }

    public int getBrokerId() {
        if (this.brokerId <= 0) {
            try {
                brokerId = abs(AddressUtils.ipToInt(AddressUtils.getLocalAddress()));
            } catch (Exception e) {
                logger.error("Get brokerId error!", e);
            }
        }
        return brokerId;
    }

    public String getHostName() {
        return this.hostName;
    }

    public long getRpcReadTimeoutMs() {
        return this.rpcReadTimeoutMs;
    }

    public int getMaxIndexSegmentSize() {
        return maxIndexSegmentSize;
    }

    public int getMaxSSDTotalFileCnt() {
        return maxSSDTotalFileCnt;
    }

    public long getMaxSSDTotalFileSizes() {
        return maxSSDTotalFileSizes;
    }

    public long getAuthValidTimeStampPeriodMs() {
        return authValidTimeStampPeriodMs;
    }

    public long getSocketSendBuffer() {
        return socketSendBuffer;
    }

    public long getSocketRecvBuffer() {
        return socketRecvBuffer;
    }


    @Override
    protected void loadFileSectAttributes(final Ini iniConf) {
        this.loadBrokerSectConf(iniConf);
        this.tlsConfig = this.loadTlsSectConf(iniConf,
                TBaseConstants.META_DEFAULT_BROKER_TLS_PORT);
        this.zkConfig = loadZKeeperSectConf(iniConf);
    }

    /***
     * Load config from broker.ini by section.
     *
     * @param iniConf
     */
    private void loadBrokerSectConf(final Ini iniConf) {
        // #lizard forgives
        final Section brokerSect = iniConf.get(SECT_TOKEN_BROKER);
        if (brokerSect == null) {
            throw new IllegalArgumentException("Require broker section in configure file not Blank!");
        }
        this.brokerId = this.getInt(brokerSect, "brokerId");
        this.port = this.getInt(brokerSect, "port", 8123);
        if (TStringUtils.isBlank(brokerSect.get("primaryPath"))) {
            throw new IllegalArgumentException("Require primaryPath not Blank!");
        }
        this.primaryPath = brokerSect.get("primaryPath").trim();
        if (TStringUtils.isNotBlank(brokerSect.get("secondDataPath"))) {
            this.secondDataPath = brokerSect.get("secondDataPath");
        }
        if (TStringUtils.isBlank(brokerSect.get("hostName"))) {
            throw new IllegalArgumentException(new StringBuilder(256).append("hostName is null or Blank in ")
                    .append(SECT_TOKEN_BROKER).append(" section!").toString());
        }
        try {
            this.hostName = brokerSect.get("hostName").trim();
            AddressUtils.validLocalIp(this.hostName);
        } catch (Throwable e) {
            throw new IllegalArgumentException(new StringBuilder(256).append("Illegal hostName value in ")
                    .append(SECT_TOKEN_BROKER).append(" section!").toString());
        }
        if (TStringUtils.isBlank(brokerSect.get("masterAddressList"))) {
            throw new IllegalArgumentException(new StringBuilder(256).append("masterAddressList is null or Blank in ")
                    .append(SECT_TOKEN_BROKER).append(" section!").toString());
        }
        this.masterAddressList = brokerSect.get("masterAddressList");
        if (TStringUtils.isNotBlank(brokerSect.get("webPort"))) {
            this.webPort = this.getInt(brokerSect, "webPort");
        }
        this.maxSegmentSize = this.getInt(brokerSect, "maxSegmentSize");
        this.transferSize = this.getInt(brokerSect, "transferSize");
        if (TStringUtils.isNotBlank(brokerSect.get("indexTransCount"))) {
            this.indexTransCount = this.getInt(brokerSect, "indexTransCount");
        }
        if (TStringUtils.isNotBlank(brokerSect.get("logClearupDurationMs"))) {
            this.logClearupDurationMs = getLong(brokerSect, "logClearupDurationMs");
            if (this.logClearupDurationMs < 20 * 60 * 1000) {
                this.logClearupDurationMs = 20 * 60 * 1000;
            }
        }
        if (TStringUtils.isNotBlank(brokerSect.get("logFlushDiskDurMs"))) {
            this.logFlushDiskDurMs = getLong(brokerSect, "logFlushDiskDurMs");
            if (this.logFlushDiskDurMs < 10000) {
                this.logFlushDiskDurMs = 10000;
            }
        }
        if (TStringUtils.isNotBlank(brokerSect.get("logFlushMemDurMs"))) {
            this.logFlushMemDurMs = getLong(brokerSect, "logFlushMemDurMs");
            if (this.logFlushMemDurMs < 10000) {
                this.logFlushMemDurMs = 10000;
            }
        }
        if (TStringUtils.isNotBlank(brokerSect.get("authValidTimeStampPeriodMs"))) {
            long tmpPeriodMs = this.getLong(brokerSect, "authValidTimeStampPeriodMs");
            this.authValidTimeStampPeriodMs =
                    tmpPeriodMs < 5000 ? 5000 : tmpPeriodMs > 120000 ? 120000 : tmpPeriodMs;
        }
        if (TStringUtils.isNotBlank(brokerSect.get("socketSendBuffer"))) {
            this.socketSendBuffer = getLong(brokerSect, "socketSendBuffer");
        }
        if (TStringUtils.isNotBlank(brokerSect.get("socketRecvBuffer"))) {
            this.socketRecvBuffer = getLong(brokerSect, "socketRecvBuffer");
        }
        if (TStringUtils.isNotBlank(brokerSect.get("maxIndexSegmentSize"))) {
            this.maxIndexSegmentSize = getInt(brokerSect, "maxIndexSegmentSize");
        }
        if (TStringUtils.isNotBlank(brokerSect.get("maxSSDTotalFileCnt"))) {
            this.maxSSDTotalFileCnt = getInt(brokerSect, "maxSSDTotalFileCnt");
        }
        if (TStringUtils.isNotBlank(brokerSect.get("maxSSDTotalFileSizes"))) {
            this.maxSSDTotalFileSizes = getLong(brokerSect, "maxSSDTotalFileSizes");
        }
        if (!TStringUtils.isBlank(brokerSect.get("quartzThreadCount"))) {
            this.quartzThreadCount = getInt(brokerSect, "quartzThreadCount");
        }
        if (!TStringUtils.isBlank(brokerSect.get("updateConsumerOffsets"))) {
            this.updateConsumerOffsets = getBoolean(brokerSect, "updateConsumerOffsets");
        }
        if (TStringUtils.isNotBlank(brokerSect.get("rpcReadTimeoutMs"))) {
            this.rpcReadTimeoutMs = getLong(brokerSect, "rpcReadTimeoutMs");
        }
        if (TStringUtils.isNotBlank(brokerSect.get("nettyWriteBufferHighWaterMark"))) {
            this.nettyWriteBufferHighWaterMark = getLong(brokerSect, "nettyWriteBufferHighWaterMark");
        }

        if (TStringUtils.isNotBlank(brokerSect.get("nettyWriteBufferLowWaterMark"))) {
            this.nettyWriteBufferLowWaterMark = getLong(brokerSect, "nettyWriteBufferLowWaterMark");
        }
        if (TStringUtils.isNotBlank(brokerSect.get("heartbeatPeriodMs"))) {
            this.heartbeatPeriodMs = getLong(brokerSect, "heartbeatPeriodMs");
        }
        if (TStringUtils.isNotBlank(brokerSect.get("tcpWriteServiceThread"))) {
            this.tcpWriteServiceThread = getInt(brokerSect, "tcpWriteServiceThread");
        }
        if (TStringUtils.isNotBlank(brokerSect.get("tcpReadServiceThread"))) {
            this.tcpReadServiceThread = getInt(brokerSect, "tcpReadServiceThread");
        }
        if (TStringUtils.isNotBlank(brokerSect.get("tlsWriteServiceThread"))) {
            this.tlsWriteServiceThread = getInt(brokerSect, "tlsWriteServiceThread");
        }
        if (TStringUtils.isNotBlank(brokerSect.get("tlsReadServiceThread"))) {
            this.tlsReadServiceThread = getInt(brokerSect, "tlsReadServiceThread");
        }
        if (TStringUtils.isNotBlank(brokerSect.get("consumerRegTimeoutMs"))) {
            this.consumerRegTimeoutMs = getInt(brokerSect, "consumerRegTimeoutMs");
            if (this.consumerRegTimeoutMs < 20000) {
                this.consumerRegTimeoutMs = 20000;
            }
        }
        if (TStringUtils.isNotBlank(brokerSect.get("defaultDeduceReadSize"))) {
            this.defaultDeduceReadSize = getLong(brokerSect, "defaultDeduceReadSize");
            this.defaultDoubleDeduceReadSize = this.defaultDeduceReadSize * 2;
        }
        if (TStringUtils.isNotBlank(brokerSect.get("rowLockWaitDurMs"))) {
            this.rowLockWaitDurMs = getInt(brokerSect, "rowLockWaitDurMs");
        }
        if (TStringUtils.isNotBlank(brokerSect.get("allowedReadIOExcptCnt"))) {
            this.allowedReadIOExcptCnt = getInt(brokerSect, "allowedReadIOExcptCnt");
        }
        if (TStringUtils.isNotBlank(brokerSect.get("allowedWriteIOExcptCnt"))) {
            this.allowedWriteIOExcptCnt = getInt(brokerSect, "allowedWriteIOExcptCnt");
        }
        if (TStringUtils.isNotBlank(brokerSect.get("ioExcptStatsDurationMs"))) {
            this.ioExcptStatsDurationMs = getLong(brokerSect, "ioExcptStatsDurationMs");
        }
        if (TStringUtils.isNotBlank(brokerSect.get("visitMasterAuth"))) {
            this.visitMasterAuth = this.getBoolean(brokerSect, "visitMasterAuth");
        }
        if (this.visitMasterAuth) {
            if (TStringUtils.isBlank(brokerSect.get("visitName"))) {
                throw new IllegalArgumentException(new StringBuilder(256).append("visitName is null or Blank in ")
                        .append(SECT_TOKEN_BROKER).append(" section!").toString());
            }
            if (TStringUtils.isBlank(brokerSect.get("visitPassword"))) {
                throw new IllegalArgumentException(new StringBuilder(256)
                        .append("visitPassword is null or Blank in ").append(SECT_TOKEN_BROKER)
                        .append(" section!").toString());
            }
            this.visitName = brokerSect.get("visitName").trim();
            this.visitPassword = brokerSect.get("visitPassword").trim();
        }
    }

    public long getLogClearupDurationMs() {
        return logClearupDurationMs;
    }

    public long getLogFlushDiskDurMs() {
        return logFlushDiskDurMs;
    }

    public long getLogFlushMemDurMs() {
        return logFlushMemDurMs;
    }

    public boolean isTlsEnable() {
        return this.tlsConfig.isTlsEnable();
    }

    public int getTlsPort() {
        return this.tlsConfig.getTlsPort();
    }

    public long getIoExcptStatsDurationMs() {
        return ioExcptStatsDurationMs;
    }

    public int getAllowedWriteIOExcptCnt() {
        return allowedWriteIOExcptCnt;
    }

    public int getAllowedReadIOExcptCnt() {
        return allowedReadIOExcptCnt;
    }

    public int getRowLockWaitDurMs() {
        return rowLockWaitDurMs;
    }

    public int getPort() {
        return this.port;
    }

    public int getTcpWriteServiceThread() {
        return this.tcpWriteServiceThread;
    }

    public int getTlsWriteServiceThread() {
        return tlsWriteServiceThread;
    }

    public int getTlsReadServiceThread() {
        return tlsReadServiceThread;
    }

    public long getDefaultDeduceReadSize() {
        return defaultDeduceReadSize;
    }

    public long getDoubleDefaultDeduceReadSize() {
        return defaultDoubleDeduceReadSize;
    }

    public int getTcpReadServiceThread() {
        return tcpReadServiceThread;
    }

    public int getTransferSize() {
        return transferSize;
    }

    public int getIndexTransCount() {
        return this.indexTransCount;
    }

    public int getMaxSegmentSize() {
        return this.maxSegmentSize;
    }

    public long getNettyWriteBufferLowWaterMark() {
        return nettyWriteBufferLowWaterMark;
    }

    public long getNettyWriteBufferHighWaterMark() {
        return nettyWriteBufferHighWaterMark;
    }

    public boolean isVisitMasterAuth() {
        return visitMasterAuth;
    }

    public String getVisitName() {
        return visitName;
    }

    public String getVisitPassword() {
        return visitPassword;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

    public String getPrimaryPath() {
        return this.primaryPath;
    }

    public String getSecondDataPath() {
        return this.secondDataPath;
    }

    public int getWebPort() {
        return webPort;
    }


    public String getMasterAddressList() {
        return masterAddressList;
    }


}

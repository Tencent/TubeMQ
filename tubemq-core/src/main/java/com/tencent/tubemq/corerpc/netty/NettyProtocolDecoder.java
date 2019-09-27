/*
 * Tencent is pleased to support the open source community by making TubeMQ available.
 *
 * Copyright (C) 2012-2019 Tencent. All Rights Reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.tubemq.corerpc.netty;

import static com.tencent.tubemq.corebase.utils.AddressUtils.getRemoteAddressIP;
import com.tencent.tubemq.corerpc.RpcConstants;
import com.tencent.tubemq.corerpc.RpcDataPack;
import com.tencent.tubemq.corerpc.exception.UnknownProtocolException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NettyProtocolDecoder extends FrameDecoder {
    private static final Logger logger =
            LoggerFactory.getLogger(NettyProtocolDecoder.class);
    private static final ConcurrentHashMap<String, AtomicLong> errProtolAddrMap =
            new ConcurrentHashMap<String, AtomicLong>();
    private static final ConcurrentHashMap<String, AtomicLong> errSizeAddrMap =
            new ConcurrentHashMap<String, AtomicLong>();
    private static AtomicLong lastProtolTime = new AtomicLong(0);
    private static AtomicLong lastSizeTime = new AtomicLong(0);
    private boolean packHeaderRead = false;
    private int listSize;
    private RpcDataPack dataPack;

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel,
                            ChannelBuffer buffer) throws Exception {
        if (!packHeaderRead) {
            if (buffer.readableBytes() < 12) {
                return null;
            }
            int frameToken = buffer.readInt();
            filterIllegalPkgToken(frameToken,
                    RpcConstants.RPC_PROTOCOL_BEGIN_TOKEN, channel);
            int serialNo = buffer.readInt();
            int tmpListSize = buffer.readInt();
            filterIllegalPackageSize(true, tmpListSize,
                    RpcConstants.MAX_FRAME_MAX_LIST_SIZE, channel);
            this.listSize = tmpListSize;
            this.dataPack = new RpcDataPack(serialNo, new ArrayList<ByteBuffer>(this.listSize));
            this.packHeaderRead = true;
        }
        // get PackBody
        if (buffer.readableBytes() < 4) {
            return null;
        }
        buffer.markReaderIndex();
        int length = buffer.readInt();
        filterIllegalPackageSize(false, length,
                RpcConstants.RPC_MAX_BUFFER_SIZE, channel);
        if (buffer.readableBytes() < length) {
            buffer.resetReaderIndex();
            return null;
        }
        ByteBuffer bb = ByteBuffer.allocate(length);
        buffer.readBytes(bb);
        bb.flip();
        dataPack.getDataLst().add(bb);
        if (dataPack.getDataLst().size() == listSize) {
            packHeaderRead = false;
            return dataPack;
        } else {
            return null;
        }
    }

    private void filterIllegalPkgToken(int inParamValue,
                                       int allowTokenVal, Channel channel) throws UnknownProtocolException {
        if (inParamValue != allowTokenVal) {
            String rmtaddrIp = getRemoteAddressIP(channel);
            if (rmtaddrIp != null) {
                AtomicLong count = errProtolAddrMap.get(rmtaddrIp);
                if (count == null) {
                    AtomicLong tmpCount = new AtomicLong(0);
                    count = errProtolAddrMap.putIfAbsent(rmtaddrIp, tmpCount);
                    if (count == null) {
                        count = tmpCount;
                    }
                }
                count.incrementAndGet();
                long befTime = lastProtolTime.get();
                long curTime = System.currentTimeMillis();
                if (curTime - befTime > 180000) {
                    if (lastProtolTime.compareAndSet(befTime, System.currentTimeMillis())) {
                        logger.warn("[Abnormal Visit] OSS Tube visit list is :" + errProtolAddrMap.toString());
                        errProtolAddrMap.clear();
                    }
                }
            }
            throw new UnknownProtocolException(new StringBuilder(256)
                    .append("Unknown protocol exception for message frame, channel.address = ")
                    .append(channel.getRemoteAddress().toString()).toString());
        }
    }


    private void filterIllegalPackageSize(boolean isFrameSize, int inParamValue,
                                          int allowSize, Channel channel) throws UnknownProtocolException {
        if (inParamValue > allowSize) {
            String rmtaddrIp = getRemoteAddressIP(channel);
            if (rmtaddrIp != null) {
                AtomicLong count = errSizeAddrMap.get(rmtaddrIp);
                if (count == null) {
                    AtomicLong tmpCount = new AtomicLong(0);
                    count = errSizeAddrMap.putIfAbsent(rmtaddrIp, tmpCount);
                    if (count == null) {
                        count = tmpCount;
                    }
                }
                count.incrementAndGet();
                long befTime = lastSizeTime.get();
                long curTime = System.currentTimeMillis();
                if (curTime - befTime > 180000) {
                    if (lastSizeTime.compareAndSet(befTime, System.currentTimeMillis())) {
                        logger.warn("[Abnormal Visit] Abnormal BodySize visit list is :" + errSizeAddrMap.toString());
                        errSizeAddrMap.clear();
                    }
                }
            }
            StringBuilder sBuilder = new StringBuilder(256)
                    .append("Unknown protocol exception for message listSize! channel.address = ")
                    .append(channel.getRemoteAddress().toString());
            if (isFrameSize) {
                sBuilder.append(", Max list size=").append(allowSize)
                        .append(", request's list size=").append(inParamValue);
            } else {
                sBuilder.append(", Max buffer size=").append(allowSize)
                        .append(", request's buffer size=").append(inParamValue);
            }
            throw new UnknownProtocolException(sBuilder.toString());
        }
    }

}

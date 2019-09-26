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

import com.tencent.tubemq.corerpc.RpcConstants;
import com.tencent.tubemq.corerpc.RpcDataPack;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;


public class NettyProtocolEncoder extends OneToOneEncoder {

    @Override
    protected Object encode(ChannelHandlerContext ctx,
                            Channel channel, Object msg) throws Exception {
        RpcDataPack dataPack = (RpcDataPack) msg;
        List<ByteBuffer> origs = dataPack.getDataLst();
        List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(origs.size() * 2 + 1);
        bbs.add(getPackHeader(dataPack));
        for (ByteBuffer b : origs) {
            bbs.add(getLengthHeader(b));
            bbs.add(b);
        }
        return ChannelBuffers.wrappedBuffer(bbs.toArray(new ByteBuffer[bbs.size()]));
    }

    private ByteBuffer getPackHeader(RpcDataPack dataPack) {
        ByteBuffer header = ByteBuffer.allocate(12);
        header.putInt(RpcConstants.RPC_PROTOCOL_BEGIN_TOKEN);
        header.putInt(dataPack.getSerialNo());
        header.putInt(dataPack.getDataLst().size());
        header.flip();
        return header;
    }

    private ByteBuffer getLengthHeader(ByteBuffer buf) {
        ByteBuffer header = ByteBuffer.allocate(4);
        header.putInt(buf.limit());
        header.flip();
        return header;
    }
}

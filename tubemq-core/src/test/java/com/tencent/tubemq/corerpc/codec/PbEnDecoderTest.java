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
package com.tencent.tubemq.corerpc.codec;

import static org.junit.Assert.assertTrue;
import com.googlecode.protobuf.format.JsonFormat;
import com.tencent.tubemq.corebase.protobuf.generated.ClientMaster;
import com.tencent.tubemq.corerpc.RpcConstants;
import org.junit.Test;

public class PbEnDecoderTest {


    @Test
    public void testPbEncodeAndDecoder() throws Exception {
        // mock a pb object
        JsonFormat jsonFormat = new JsonFormat();
        ClientMaster.RegisterRequestP2M.Builder builder = ClientMaster.RegisterRequestP2M.newBuilder();
        builder.setClientId("10001");
        builder.setBrokerCheckSum(99);
        builder.setHostName("tube-test");
        ClientMaster.RegisterRequestP2M object = builder.build();
        final String jsonOject = jsonFormat.printToString(object);
        // encode pb
        byte[] data = PbEnDecoder.pbEncode(object);

        // decode bytes
        ClientMaster.RegisterRequestP2M decodeObject = (ClientMaster.RegisterRequestP2M)
                PbEnDecoder.pbDecode(true, RpcConstants.RPC_MSG_MASTER_PRODUCER_REGISTER, data);

        assertTrue(decodeObject.getClientId().equals(object.getClientId()));
        assertTrue(decodeObject.getBrokerCheckSum() == object.getBrokerCheckSum());
        assertTrue(decodeObject.getHostName().equals(object.getHostName()));
        assertTrue(jsonOject.equals(jsonFormat.printToString(decodeObject)));
    }

}

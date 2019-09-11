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

package com.tencent.tubemq.corerpc.service;

import com.tencent.tubemq.corebase.protobuf.generated.ClientBroker;


public interface BrokerReadService {

    ClientBroker.RegisterResponseB2C consumerRegisterC2B(ClientBroker.RegisterRequestC2B request,
                                                         final String rmtAddress, boolean overtls) throws Throwable;

    ClientBroker.HeartBeatResponseB2C consumerHeartbeatC2B(ClientBroker.HeartBeatRequestC2B request,
                                                           final String rmtAddress, boolean overtls) throws Throwable;

    ClientBroker.GetMessageResponseB2C getMessagesC2B(ClientBroker.GetMessageRequestC2B request,
                                                      final String rmtAddress, boolean overtls) throws Throwable;

    ClientBroker.CommitOffsetResponseB2C consumerCommitC2B(ClientBroker.CommitOffsetRequestC2B request,
                                                           final String rmtAddress, boolean overtls) throws Throwable;

}

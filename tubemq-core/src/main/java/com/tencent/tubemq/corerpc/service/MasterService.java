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

import com.tencent.tubemq.corebase.protobuf.generated.ClientMaster;


public interface MasterService {

    ClientMaster.RegisterResponseM2P producerRegisterP2M(ClientMaster.RegisterRequestP2M request,
                                                         final String rmtAddress, boolean overtls) throws Throwable;

    ClientMaster.HeartResponseM2P producerHeartbeatP2M(ClientMaster.HeartRequestP2M request,
                                                       final String rmtAddress, boolean overtls) throws Throwable;

    ClientMaster.CloseResponseM2P producerCloseClientP2M(ClientMaster.CloseRequestP2M request,
                                                         final String rmtAddress, boolean overtls) throws Throwable;

    ClientMaster.RegisterResponseM2C consumerRegisterC2M(ClientMaster.RegisterRequestC2M request,
                                                         final String rmtAddress, boolean overtls) throws Throwable;

    ClientMaster.HeartResponseM2C consumerHeartbeatC2M(ClientMaster.HeartRequestC2M request,
                                                       final String rmtAddress, boolean overtls) throws Throwable;

    ClientMaster.CloseResponseM2C consumerCloseClientC2M(ClientMaster.CloseRequestC2M request,
                                                         final String rmtAddress, boolean overtls) throws Throwable;

    ClientMaster.RegisterResponseM2B brokerRegisterB2M(ClientMaster.RegisterRequestB2M request,
                                                       final String rmtAddress, boolean overtls) throws Throwable;

    ClientMaster.HeartResponseM2B brokerHeartbeatB2M(ClientMaster.HeartRequestB2M request,
                                                     final String rmtAddress, boolean overtls) throws Throwable;

    ClientMaster.CloseResponseM2B brokerCloseClientB2M(ClientMaster.CloseRequestB2M request,
                                                       final String rmtAddress, boolean overtls) throws Throwable;

}

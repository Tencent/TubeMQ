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

package com.tencent.tubemq.client.consumer;

import com.tencent.tubemq.client.exception.TubeClientException;
import java.util.TreeSet;


public interface PullMessageConsumer extends MessageConsumer {

    boolean isPartitionsReady(long maxWaitTime);

    PullMessageConsumer subscribe(String topic,
                                  TreeSet<String> filterConds) throws TubeClientException;

    ConsumerResult getMessage() throws TubeClientException;

    ConsumerResult confirmConsume(final String confirmContext,
                                  boolean isConsumed) throws TubeClientException;
}

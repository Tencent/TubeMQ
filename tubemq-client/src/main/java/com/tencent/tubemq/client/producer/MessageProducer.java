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

package com.tencent.tubemq.client.producer;

import com.tencent.tubemq.client.exception.TubeClientException;
import com.tencent.tubemq.corebase.Message;
import com.tencent.tubemq.corebase.Shutdownable;
import java.util.Set;


public interface MessageProducer extends Shutdownable {

    void publish(String topic) throws TubeClientException;

    Set<String> publish(Set<String> topicSet) throws TubeClientException;

    Set<String> getPublishedTopicSet() throws TubeClientException;

    boolean isTopicCurAcceptPublish(String topic) throws TubeClientException;

    MessageSentResult sendMessage(final Message message)
            throws TubeClientException, InterruptedException;

    void sendMessage(final Message message, final MessageSentCallback cb)
            throws TubeClientException, InterruptedException;

    void shutdown() throws Throwable;
}

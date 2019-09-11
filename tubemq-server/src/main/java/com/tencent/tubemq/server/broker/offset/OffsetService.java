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

package com.tencent.tubemq.server.broker.offset;

import com.tencent.tubemq.server.broker.msgstore.MessageStore;
import com.tencent.tubemq.server.common.offsetstorage.OffsetStorageInfo;

/***
 * Offset manager service interface.
 */
public interface OffsetService {

    void close(long waitTimeMs);

    OffsetStorageInfo loadOffset(final MessageStore store, final String group,
                                 final String topic, int partitionId,
                                 int readStatus, long reqOffset,
                                 final StringBuilder sb);

    long getOffset(final MessageStore msgStore, final String group,
                   final String topic, int partitionId,
                   boolean isManCommit, boolean lastConsumed,
                   final StringBuilder sb);

    long getOffset(String group, String topic, int partitionId);

    void bookOffset(final String group, final String topic, int partitionId,
                    int readDalt, boolean isManCommit, boolean isMsgEmpty,
                    final StringBuilder sb);

    long commitOffset(final String group, final String topic,
                      int partitionId, boolean isConsumed);

    long resetOffset(final MessageStore store, final String group, final String topic,
                     int partitionId, long reSetOffset, final String modifyer);

    long getTmpOffset(final String group, final String topic, int partitionId);

}

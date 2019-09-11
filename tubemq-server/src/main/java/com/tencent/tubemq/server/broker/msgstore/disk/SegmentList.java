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

package com.tencent.tubemq.server.broker.msgstore.disk;

import java.io.IOException;
import java.util.List;

/***
 * Segment list.
 */
public interface SegmentList {

    void close();

    Segment last();

    Segment findSegment(final long offset);

    List<Segment> getView();

    long getSizeInBytes();

    long getMaxOffset();

    boolean checkExpiredSegments(final long checkTimestamp, final long fileValidTimeMs);

    void delExpiredSegments(final StringBuilder sb);

    void flushLast(boolean force) throws IOException;

    long getCommitMaxOffset();

    long getMinOffset();

    void append(final Segment segment);

    void delete(final Segment segment);

    RecordView getRecordView(final long offset, final int maxSize) throws IOException;

    void relRecordView(RecordView recordView);


}

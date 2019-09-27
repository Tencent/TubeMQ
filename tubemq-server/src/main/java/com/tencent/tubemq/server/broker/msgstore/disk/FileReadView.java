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
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Readonly view of file, only used for read operation.
 */
public class FileReadView implements RecordView {
    private static final Logger logger = LoggerFactory.getLogger(FileReadView.class);
    private final Segment segment;
    private final long offset;


    public FileReadView(Segment segment, final long start,
                        final long offset, final long limit) {
        this.segment = segment;
        this.offset = offset;
    }

    @Override
    public void read(final ByteBuffer bf, final long offset) throws IOException {
        if (segment.isExpired()) {
            //Todo: conduct file closed and expired cases.
        }
        int size = 0;
        while (bf.hasRemaining()) {
            final int l = this.segment.getFileChannel().read(bf, offset + size);
            if (l < 0) {
                break;
            }
            size += l;
        }
    }

    @Override
    public void read(final ByteBuffer bf) throws IOException {
        this.read(bf, this.offset);
    }

    @Override
    public long getStartOffset() {
        return segment.getStart();
    }

    @Override
    public Segment getSegment() {
        return segment;
    }


    @Override
    public long getCommitSize() {
        return segment.getCommitSize();
    }
}

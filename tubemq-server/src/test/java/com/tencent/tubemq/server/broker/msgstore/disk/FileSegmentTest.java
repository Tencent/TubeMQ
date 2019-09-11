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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/***
 * FileSegment test.
 */
public class FileSegmentTest {

    FileSegment fileSegment;

    @org.junit.Test
    public void append() {
        long start = 0;
        File file = new File("src/test/resource/testdata");
        if (file.exists()) {
            file.delete();
        }
        try {
            file.createNewFile();
            // create FileSegment
            fileSegment = new FileSegment(start, file, true, SegmentType.DATA);
            String data = "abc";
            byte[] bytes = data.getBytes();
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            // append data to FileSegment.
            fileSegment.append(buf);
            fileSegment.append(buf);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fileSegment.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @org.junit.Test
    public void getViewRef() {
        long start = 0;
        File file = new File("src/test/resource/testdata");
        if (file.exists()) {
            file.delete();
        }
        try {
            file.createNewFile();
            // create FileSegment.
            fileSegment = new FileSegment(start, file, true, SegmentType.DATA);
            String data = "abc";
            byte[] bytes = data.getBytes();
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            // append data to fileSegment.
            long offset = fileSegment.append(buf);
            int limit = 1000;
            // get view of fileSegment.
            RecordView recordView = fileSegment.getViewRef(start, offset, limit);
            ByteBuffer readBuffer = ByteBuffer.allocate(limit);
            recordView.read(readBuffer);
            byte[] readBytes = readBuffer.array();
            String readData = new String(readBytes);
            readData.substring(0, data.length());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fileSegment.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
/**
 * Tencent is pleased to support the open source community by making TubeMQ available.
 * <p>
 * Copyright (C) 2012-2019 Tencent. All Rights Reserved.
 * <p>
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.tubemq.server.common.utils;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RowLock {
    private static final Logger logger = LoggerFactory.getLogger(RowLock.class);
    private static Random rand = new Random();
    private final ConcurrentHashMap<HashedBytes, CountDownLatch> lockedRows =
            new ConcurrentHashMap<HashedBytes, CountDownLatch>();
    private final ConcurrentHashMap<Integer, HashedBytes> lockIds =
            new ConcurrentHashMap<Integer, HashedBytes>();
    private final AtomicInteger lockIdGenerator = new AtomicInteger(1);
    private final int rowLockWaitDuration;
    private final String name;

    public RowLock(String lockName, int rowLockWaitDuration) {
        this.rowLockWaitDuration = rowLockWaitDuration;
        this.name = lockName;
    }

    public Integer getLock(Integer lockid, byte[] row, boolean waitForLock) throws IOException {
        return getLock(lockid, new HashedBytes(row), waitForLock);
    }

    protected Integer getLock(Integer lockid,
                              HashedBytes row,
                              boolean waitForLock) throws IOException {
        Integer lid;
        if (lockid == null) {
            lid = internalObtainRowLock(row, waitForLock);
        } else {
            HashedBytes rowFromLock = lockIds.get(lockid);
            if (!row.equals(rowFromLock)) {
                throw new IOException(new StringBuilder(512)
                        .append("Invalid row lock: LockId: ").append(lockid)
                        .append(" holds the lock for row: ").append(rowFromLock)
                        .append(" but wanted lock for row: ").append(row).toString());
            }
            lid = lockid;
        }
        return lid;
    }

    private Integer internalObtainRowLock(final HashedBytes rowKey,
                                          boolean waitForLock) throws IOException {
        CountDownLatch rowLatch = new CountDownLatch(1);
        while (true) {
            CountDownLatch existingLatch =
                    lockedRows.putIfAbsent(rowKey, rowLatch);
            if (existingLatch == null) {
                break;
            } else {
                if (!waitForLock) {
                    return null;
                }
                try {
                    if (!existingLatch.await(
                            this.rowLockWaitDuration, TimeUnit.MILLISECONDS)) {
                        throw new IOException(new StringBuilder(256)
                                .append("Timed out on getting lock for row=")
                                .append(rowKey).toString());
                    }
                } catch (InterruptedException ie) {
                    //
                }
            }
        }
        while (true) {
            Integer lockId = lockIdGenerator.incrementAndGet();
            if (lockIds.putIfAbsent(lockId, rowKey) == null) {
                return lockId;
            } else {
                lockIdGenerator.set(rand.nextInt());
            }
        }
    }

    public void releaseRowLock(final Integer lockId) {
        if (lockId == null) {
            return;
        }
        HashedBytes rowKey = lockIds.remove(lockId);
        if (rowKey == null) {
            logger.warn(new StringBuilder(256).append(this.name)
                    .append(" release unknown lockId: ")
                    .append(lockId).toString());
            return;
        }
        CountDownLatch rowLatch = lockedRows.remove(rowKey);
        if (rowLatch == null) {
            logger.error(new StringBuilder(256).append(this.name)
                    .append(" releases row not locked, lockId: ")
                    .append(lockId).append(" row: ").append(rowKey).toString());
            return;
        }
        rowLatch.countDown();
    }
}

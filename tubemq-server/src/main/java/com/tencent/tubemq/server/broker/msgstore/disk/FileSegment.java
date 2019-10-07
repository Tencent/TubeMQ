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

import com.tencent.tubemq.corebase.utils.CheckSum;
import com.tencent.tubemq.server.broker.utils.DataStoreUtils;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Segment file. Topic contains multi FileSegments. Each FileSegment contains data file and index file.
 * It is mini particle of topic expire policy. It will be marked deleted when expired.
 */
public class FileSegment implements Segment, Comparable<FileSegment> {
    private static final Logger logger =
            LoggerFactory.getLogger(FileSegment.class);
    private final long start;
    private final File file;
    private final FileChannel channel;
    private final AtomicLong cachedSize;
    private final AtomicLong flushedSize;
    private final SegmentType segmentType;
    private boolean mutable;
    private AtomicLong useRef = new AtomicLong(0);
    private AtomicBoolean expired = new AtomicBoolean(false);
    private AtomicBoolean closed = new AtomicBoolean(false);


    public FileSegment(final long start, final File file, SegmentType type) throws IOException {
        this(start, file, true, type, Long.MAX_VALUE);
    }

    public FileSegment(final long start, final File file,
                       final boolean mutable, SegmentType type) throws IOException {
        this(start, file, mutable, type, Long.MAX_VALUE);
    }

    public FileSegment(final long start, final File file,
                       SegmentType type, final long checkOffset) throws IOException {
        this(start, file, true, type, checkOffset);
    }

    private FileSegment(final long start, final File file,
                        final boolean mutable, SegmentType type,
                        final long checkOffset) throws IOException {
        this.segmentType = type;
        this.start = start;
        this.file = file;
        this.mutable = mutable;
        this.cachedSize = new AtomicLong(0);
        this.flushedSize = new AtomicLong(0);
        this.channel = new RandomAccessFile(this.file, "rw").getChannel();
        if (mutable) {
            final long startMs = System.currentTimeMillis();
            long remaining = checkOffset == Long.MAX_VALUE ? -1 : (checkOffset - this.start);
            if (this.segmentType == SegmentType.DATA) {
                RecoverResult recoverResult = this.recoverData(remaining);
                if (recoverResult.isEqutal()) {
                    logger.info(
                            "[File Store] Data Segment recover success, ignore content check!");
                } else {
                    if (recoverResult.getTruncated() > 0) {
                        logger.info(new StringBuilder(512)
                                .append("[File Store] Recover DATA Segment succeeded in ")
                                .append((System.currentTimeMillis() - startMs) / 1000)
                                .append(" seconds. ").append(recoverResult.getTruncated())
                                .append(" bytes truncated.").toString());
                    }
                }
            } else {
                RecoverResult recoverResult = this.recoverIndex(remaining);
                if (recoverResult.isEqutal()) {
                    logger.info(
                            "[File Store] Index Segment recover success, ignore content check!");
                } else {
                    if (recoverResult.getTruncated() > 0) {
                        logger.info(new StringBuilder(512)
                                .append("[File Store] Recover Index Segment succeeded in ")
                                .append((System.currentTimeMillis() - startMs) / 1000)
                                .append(" seconds. ").append(recoverResult.getTruncated())
                                .append(" bytes truncated.").toString());
                    }
                }
            }
        } else {
            try {
                this.cachedSize.set(this.channel.size());
                this.flushedSize.set(this.cachedSize.get());
            } catch (final Exception e) {
                if (this.segmentType == SegmentType.DATA) {
                    logger.error("[File Store] Set DATA Segment cachedSize error", e);
                } else {
                    logger.error("[File Store] Set INDEX Segment cachedSize error", e);
                }
            }
        }
        this.useRef.incrementAndGet();
    }

    @Override
    public int close() throws IOException {
        if (this.closed.get()) {
            return 0;
        }
        if (useRef.decrementAndGet() > 0) {
            return 1;
        }
        if (this.closed.compareAndSet(false, true)) {
            if (this.mutable) {
                this.flush(true);
            }
            try {
                channel.close();
            } catch (IOException e2) {
                //
            }
            return 0;
        }
        return 0;
    }

    /***
     * Messages can only be appended to the last FileSegment. The last FileSegment is writable, the others are mutable.
     *
     * @param buf
     * @return
     * @throws IOException
     */
    @Override
    public long append(final ByteBuffer buf) throws IOException {
        if (!this.mutable) {
            //　只有最后一个segment为可修改状态
            if (this.segmentType == SegmentType.DATA) {
                throw new UnsupportedOperationException("[File Store] Data Segment is immutable!");
            } else {
                throw new UnsupportedOperationException(
                        "[File Store] Index Segment is immutable!");
            }
        }
        if (this.closed.get()) {
            throw new UnsupportedOperationException("[File Store] Segment is closed!");
        }
        final long offset = this.cachedSize.get();
        int sizeInBytes = 0;
        while (buf.hasRemaining()) {
            sizeInBytes += this.channel.write(buf);
        }
        this.cachedSize.addAndGet(sizeInBytes);
        return this.start + offset;
    }

    /***
     * Flush file cache to disk.
     *
     * @param force
     * @return
     * @throws IOException
     */
    @Override
    public long flush(boolean force) throws IOException {
        this.channel.force(force);
        this.flushedSize.set(this.cachedSize.get());
        return this.start + this.flushedSize.get();
    }

    @Override
    public boolean isExpired() {
        return expired.get();
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public boolean contains(final long offset) {
        return (this.getCachedSize() == 0
                && offset == this.start
                || this.getCachedSize() > 0
                && offset >= this.start
                && offset <= this.start + this.getCachedSize() - 1);
    }

    @Override
    public FileChannel getFileChannel() {
        return this.channel;
    }

    /***
     * Get the read view of this FileSegment.
     *
     * @return
     */
    @Override
    public RecordView getViewRef() {
        return getViewRef(this.start, 0, getCachedSize());
    }

    /***
     * Get the read view of this FileSegment by params.
     *
     * @param start
     * @param offset
     * @param limit
     * @return
     */
    @Override
    public RecordView getViewRef(final long start, final long offset, final long limit) {
        if (isClosed() || isExpired()) {
            if (isClosed()) {
                throw new UnsupportedOperationException("Segment is Closed!");
            } else {
                throw new UnsupportedOperationException("Segment is not visited!");
            }
        }
        useRef.incrementAndGet();
        return new FileReadView(this, start, offset, Math.min(getCachedSize(), limit) - offset);
    }

    /***
     * Release reference to this FileSegment. File's channel will be closed when the reference decreased to 0.
     */
    @Override
    public void relViewRef() {
        if (useRef.decrementAndGet() == 0) {
            if (this.closed.compareAndSet(false, true)) {
                try {
                    channel.close();
                } catch (IOException e1) {
                    //
                }
            }
        }
    }

    @Override
    public long getStart() {
        return start;
    }

    @Override
    public long getLast() {
        return start + cachedSize.get();
    }

    /***
     * Return the position that have been flushed to disk.
     *
     * @return
     */
    @Override
    public long getCommitLast() {
        return start + flushedSize.get();
    }

    @Override
    public boolean isMutable() {
        return mutable;
    }

    /***
     * Set FileSegment to readonly.
     *
     * @param mutable
     */
    @Override
    public void setMutable(boolean mutable) {
        this.mutable = mutable;
    }

    @Override
    public long getCachedSize() {
        return this.cachedSize.get();
    }

    @Override
    public long getCommitSize() {
        return this.flushedSize.get();
    }

    @Override
    public final File getFile() {
        return this.file;
    }

    @Override
    public int compareTo(FileSegment o) {
        return this.start > o.start ? 1 : this.start < o.start ? -1 : 0;
    }

    /***
     * Check whether this FileSegment is expired, and set expire status. The last FileSegment cannot be marked expired.
     *
     * @param checkTimestamp check timestamp.
     * @param maxValidTimeMs the max expire interval in milliseconds.
     * @return -1 means already expired, 0 means the last FileSegment, 1 means expired.
     */
    @Override
    public int checkAndSetExpired(final long checkTimestamp, final long maxValidTimeMs) {
        if (expired.get()) {
            return -1;
        }
        if (!mutable) {
            // 最后一个segment不能主动设置过期
            if (checkTimestamp - file.lastModified() > maxValidTimeMs) {
                expired.set(true);
                return 1;
            }
        }
        return 0;
    }

    private RecoverResult recoverData(final long checkOffset) throws IOException {
        if (!this.mutable) {
            throw new UnsupportedOperationException(
                    "[File Store] The Data Segment must be mutable!");
        }
        final long totalBytes = this.channel.size();
        if (totalBytes == checkOffset || checkOffset == -1) {
            this.cachedSize.set(totalBytes);
            this.flushedSize.set(totalBytes);
            this.channel.position(totalBytes);
            return new RecoverResult(0, totalBytes == checkOffset);
        }
        long validBytes = 0L;
        long next = 0L;
        long itemRead = 0L;
        int itemMsglen = 0;
        int itemMsgToken = 0;
        int itemCheckSum = 0;
        long itemNext = 0L;
        final ByteBuffer itemBuf = ByteBuffer.allocate(DataStoreUtils.STORE_DATA_HEADER_LEN);
        do {
            do {
                itemBuf.rewind();
                itemRead = this.channel.read(itemBuf);
                if (itemRead < DataStoreUtils.STORE_DATA_HEADER_LEN) {
                    next = -1;
                    break;
                }
                itemBuf.flip();
                itemMsglen = itemBuf.getInt() - DataStoreUtils.STORE_DATA_PREFX_LEN;
                itemMsgToken = itemBuf.getInt();
                itemCheckSum = itemBuf.getInt();
                itemNext = validBytes + DataStoreUtils.STORE_DATA_HEADER_LEN + itemMsglen;
                if ((itemMsgToken != DataStoreUtils.STORE_DATA_TOKER_BEGIN_VALUE)
                        || (itemMsglen <= 0)
                        || (itemMsglen > DataStoreUtils.MAX_MSG_DATA_STORE_SIZE)
                        || (itemNext > totalBytes)) {
                    next = -1;
                    break;
                }
                final ByteBuffer messageBuffer = ByteBuffer.allocate(itemMsglen);
                while (messageBuffer.hasRemaining()) {
                    itemRead = this.channel.read(messageBuffer);
                    if (itemRead < 0) {
                        throw new IOException(
                                "[File Store] The Data Segment is changing in recover processing!");
                    }
                }
                if (CheckSum.crc32(messageBuffer.array()) != itemCheckSum) {
                    next = -1;
                    break;
                }
                next = itemNext;
            } while (false);
            if (next >= 0) {
                validBytes = next;
            }
        } while (next >= 0);
        if (totalBytes != validBytes) {
            this.channel.truncate(validBytes);
        }
        this.cachedSize.set(validBytes);
        this.flushedSize.set(validBytes);
        this.channel.position(validBytes);
        return new RecoverResult(totalBytes - validBytes, false);
    }

    private RecoverResult recoverIndex(final long checkOffset) throws IOException {
        if (!this.mutable) {
            throw new UnsupportedOperationException(
                    "[File Store] The Index Segment must be mutable!");
        }
        final long totalBytes = this.channel.size();
        if (totalBytes == checkOffset) {
            this.cachedSize.set(totalBytes);
            this.flushedSize.set(totalBytes);
            this.channel.position(totalBytes);
            return new RecoverResult(0, true);
        }
        long validBytes = 0L;
        long next = 0L;
        long itemRead = 0L;
        int itemMsgPartId = 0;
        long itemMsgOffset = 0L;
        int itemMsglen = 0;
        int itemKeyCode = 0;
        long itemTimeRecv = 0;
        long itemNext = 0L;
        final ByteBuffer itemBuf = ByteBuffer.allocate(DataStoreUtils.STORE_INDEX_HEAD_LEN);
        do {
            do {
                itemBuf.rewind();
                itemRead = this.channel.read(itemBuf);
                if (itemRead < DataStoreUtils.STORE_INDEX_HEAD_LEN) {
                    next = -1;
                    break;
                }
                itemBuf.flip();
                itemMsgPartId = itemBuf.getInt();
                itemMsgOffset = itemBuf.getLong();
                itemMsglen = itemBuf.getInt();
                itemKeyCode = itemBuf.getInt();
                itemTimeRecv = itemBuf.getLong();
                itemNext = validBytes + DataStoreUtils.STORE_INDEX_HEAD_LEN;
                if ((itemMsgPartId < 0)
                        || (itemMsgOffset < 0)
                        || (itemMsglen <= 0)
                        || (itemMsglen > DataStoreUtils.STORE_MAX_MESSAGE_STORE_LEN)
                        || (itemNext > totalBytes)) {
                    next = -1;
                    break;
                }
                next = itemNext;
            } while (false);
            if (next >= 0) {
                validBytes = next;
            }
        } while (next >= 0);
        if (totalBytes != validBytes) {
            channel.truncate(validBytes);
        }
        this.cachedSize.set(validBytes);
        this.flushedSize.set(validBytes);
        this.channel.position(validBytes);
        return new RecoverResult(totalBytes - validBytes, false);
    }

    public boolean equals(Segment other) {
        if (this == other) {
            return true;
        }
        if (other instanceof FileSegment) {
            FileSegment otherFileSeg = (FileSegment) other;
            if ((this.file.equals(otherFileSeg.file))
                    && (this.start == otherFileSeg.start)
                    && (this.segmentType == otherFileSeg.segmentType)) {
                return true;
            }
        }
        return false;
    }

    private static class RecoverResult {
        private long truncated;
        private boolean isEqutal;

        public RecoverResult(long truncated, boolean isEqutal) {
            this.truncated = truncated;
            this.isEqutal = isEqutal;
        }

        public long getTruncated() {
            return truncated;
        }

        public boolean isEqutal() {
            return isEqutal;
        }
    }


}

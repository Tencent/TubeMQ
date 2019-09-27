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

import com.tencent.tubemq.corebase.utils.ThreadUtils;
import com.tencent.tubemq.server.broker.utils.DataStoreUtils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * FileSegments management. Contains two types FileSegment: data and index.
 */
public class FileSegmentList implements SegmentList {
    private static final Logger logger =
            LoggerFactory.getLogger(FileSegmentList.class);
    // directory to store file.
    private final File segListDir;
    // filesegment type: DATA, INDEX.
    private final SegmentType segType;
    // file's suffix.
    private final String fileSuffix;
    // filesegment type in String: Data, Index.
    private final String segTypeStr;
    // list of segments.
    private AtomicReference<List<Segment>> segmentList =
            new AtomicReference<List<Segment>>();


    public FileSegmentList(final File segListDir, final SegmentType type,
                           boolean needCreate, final long offsetIfCreate,
                           final long lastCheckOffset, StringBuilder sBuilder) throws IOException {
        this.segListDir = segListDir;
        this.segType = type;
        if (this.segType == SegmentType.DATA) {
            segTypeStr = "Data";
            fileSuffix = DataStoreUtils.DATA_FILE_SUFFIX;
        } else {
            segTypeStr = "Index";
            fileSuffix = DataStoreUtils.INDEX_FILE_SUFFIX;
        }
        logger.info(sBuilder.append("[File Store] begin Load ")
                .append(segTypeStr).append(" segments ")
                .append(this.segListDir.getAbsolutePath()).toString());
        sBuilder.delete(0, sBuilder.length());
        final List<Segment> accum = new ArrayList<Segment>();
        final File[] ls = this.segListDir.listFiles();
        if (ls != null) {
            for (final File file : ls) {
                if (file == null) {
                    continue;
                }
                if (file.isFile() && file.toString().endsWith(fileSuffix)) {
                    if (!file.canRead()) {
                        throw new IOException(new StringBuilder(512)
                                .append("Could not read ").append(segTypeStr)
                                .append(" file ").append(file).toString());
                    }
                    final String filename = file.getName();
                    final long start =
                            Long.parseLong(filename.substring(0, filename.length() - fileSuffix.length()));
                    accum.add(new FileSegment(start, file, false, segType));
                }
            }
        }
        if (accum.size() == 0) {
            if (needCreate) {
                final File newFile =
                        new File(this.segListDir,
                                DataStoreUtils.nameFromOffset(offsetIfCreate, fileSuffix));
                logger.info(sBuilder.append("[File Store] Created ").append(segTypeStr)
                        .append(" segment ").append(newFile.getAbsolutePath()).toString());
                sBuilder.delete(0, sBuilder.length());
                accum.add(new FileSegment(offsetIfCreate, newFile, segType));
            }
        } else {
            // segment的列表要求是从低到高连续的排列
            Collections.sort(accum, new Comparator<Segment>() {
                @Override
                public int compare(final Segment o1, final Segment o2) {
                    if (o1.getStart() == o2.getStart()) {
                        return 0;
                    } else if (o1.getStart() > o2.getStart()) {
                        return 1;
                    } else {
                        return -1;
                    }
                }
            });
            validateSegments(accum);
            Segment last = accum.get(accum.size() - 1);
            if ((last.getCachedSize() > 0)
                    && (System.currentTimeMillis() - last.getFile().lastModified()
                    >= DataStoreUtils.MAX_FILE_NO_WRITE_DURATION)) {
                //最后一个segment如果长期没有写入，则会在启动的时候老化掉
                final long newOffset = last.getCommitLast();
                final File newFile =
                        new File(this.segListDir,
                                DataStoreUtils.nameFromOffset(newOffset, fileSuffix));
                logger.info(sBuilder.append("[File Store] Created time roll").append(segTypeStr)
                        .append(" segment ").append(newFile.getAbsolutePath()).toString());
                sBuilder.delete(0, sBuilder.length());
                accum.add(new FileSegment(newOffset, newFile, segType));
            } else {
                last = accum.remove(accum.size() - 1);
                last.close();
                logger.info(sBuilder
                        .append("[File Store] Loading the last ").append(segTypeStr)
                        .append(" segment in mutable mode and running recover on ")
                        .append(last.getFile().getAbsolutePath()).toString());
                sBuilder.delete(0, sBuilder.length());
                final FileSegment mutable =
                        new FileSegment(last.getStart(), last.getFile(), segType, lastCheckOffset);
                accum.add(mutable);
            }
        }
        this.segmentList.set(accum);
        logger.info(sBuilder.append("[File Store] Loaded ")
                .append(segTypeStr).append(" ").append(accum.size()).append(" segments ")
                .append(this.segListDir.getAbsolutePath()).toString());
        sBuilder.delete(0, sBuilder.length());
    }

    @Override
    public void close() {
        final List<Segment> curViews = segmentList.get();
        if (curViews == null || curViews.isEmpty()) {
            return;
        }
        Segment last = curViews.get(curViews.size() - 1);
        if (last == null) {
            return;
        }
        try {
            last.flush(true);
        } catch (Throwable e) {
            //
        }
        for (final Segment segment : curViews) {
            try {
                if (segment != null) {
                    segment.close();
                }
            } catch (Throwable e2) {
                logger.error(new StringBuilder(512).append("[File Store] Close ")
                        .append(segment.getFile().getAbsoluteFile().toString())
                        .append("'s ").append(segTypeStr).append(" file failure").toString(), e2);
            }
        }
    }

    @Override
    public List<Segment> getView() {
        return segmentList.get();
    }

    /***
     * Return RecordView by the given offset.
     *
     * @param offset
     * @param maxSize
     * @return
     * @throws IOException
     */
    @Override
    public RecordView getRecordView(final long offset, final int maxSize) throws IOException {
        final Segment segment = this.findSegment(offset);
        if (segment == null) {
            return null;
        } else {
            return segment.getViewRef(segment.getStart(),
                    offset - segment.getStart(), offset - segment.getStart() + maxSize);
        }
    }

    /***
     * Release RecordView's reference.
     *
     * @param recordView
     */
    @Override
    public void relRecordView(RecordView recordView) {
        if (recordView == null) {
            return;
        }
        Segment segment = recordView.getSegment();
        if (segment == null) {
            return;
        }
        segment.relViewRef();
    }

    @Override
    public void append(final Segment segment) {
        while (true) {
            List<Segment> currList = segmentList.get();
            List<Segment> updateList = new ArrayList<Segment>(currList);
            updateList.add(segment);
            if (segmentList.compareAndSet(currList, updateList)) {
                return;
            }
        }
    }

    /***
     * Check each FileSegment whether is expired, and set expire status.
     *
     * @param checkTimestamp
     * @param fileValidTimeMs
     * @return
     */
    @Override
    public boolean checkExpiredSegments(final long checkTimestamp, final long fileValidTimeMs) {
        final List<Segment> curViews = segmentList.get();
        if (curViews == null || curViews.isEmpty()) {
            return false;
        }
        boolean hasExpired = false;
        for (final Segment segment : curViews) {
            if (segment == null) {
                continue;
            }
            if (segment.checkAndSetExpired(checkTimestamp, fileValidTimeMs) == 0) {
                break;
            }
            hasExpired = true;
        }
        return hasExpired;
    }

    /***
     * Check FileSegments whether is expired, close all expired FileSegments, and then delete these files.
     *
     * @param sb
     */
    @Override
    public void delExpiredSegments(final StringBuilder sb) {
        //　删除过期segment，采用的是两阶段方式进行，避免异常
        List<Segment> rmvSegList = new ArrayList<>();
        List<Segment> delayRmvSegList = new ArrayList<>();
        while (true) {
            rmvSegList.clear();
            final List<Segment> currList = this.segmentList.get();
            int curListSize = currList.size();
            List<Segment> updateList = new ArrayList<Segment>();
            for (int i = 0; i < curListSize; i++) {
                Segment tmpSeg = currList.get(i);
                if (tmpSeg == null) {
                    continue;
                }
                if (tmpSeg.isExpired() || tmpSeg.isClosed()) {
                    rmvSegList.add(tmpSeg);
                } else {
                    updateList.add(tmpSeg);
                }
            }
            if (rmvSegList.isEmpty()) {
                break;
            }
            if (segmentList.compareAndSet(currList, updateList)) {
                break;
            }
        }
        for (Segment segment : rmvSegList) {
            if (segment == null) {
                continue;
            }
            try {
                if (segment.close() > 0) {
                    delayRmvSegList.add(segment);
                } else {
                    segment.getFile().delete();
                }
            } catch (Throwable e) {
                logger.error(
                        "[File Store] failure to close and delete file ", e);
            }
        }
        if (!delayRmvSegList.isEmpty()) {
            ThreadUtils.sleep(50);
            for (Segment segment : delayRmvSegList) {
                if (segment == null) {
                    continue;
                }
                try {
                    segment.getFile().delete();
                } catch (Throwable e) {
                    logger.error(
                            "[File Store] failure to close and delete file ", e);
                }
            }
        }
    }

    @Override
    public void delete(final Segment segment) {
        while (true) {
            final List<Segment> currList = this.segmentList.get();
            int index = -1;
            int curListSize = currList.size();
            List<Segment> updateList = new ArrayList<Segment>(curListSize - 1);
            for (int i = 0; i < curListSize; i++) {
                Segment tmpSeg = currList.get(i);
                if (tmpSeg == null) {
                    continue;
                }
                if (tmpSeg.equals(segment)) {
                    index = i;
                    continue;
                }
                updateList.add(currList.get(i));
            }
            if (index == -1) {
                return;
            }
            if (segmentList.compareAndSet(currList, updateList)) {
                return;
            }
        }
    }

    @Override
    public void flushLast(boolean force) throws IOException {
        final List<Segment> curViews = segmentList.get();
        if (curViews == null || curViews.isEmpty()) {
            return;
        }
        Segment last = curViews.get(curViews.size() - 1);
        if (last == null) {
            return;
        }
        last.flush(force);
    }

    @Override
    public Segment last() {
        final List<Segment> curViews = segmentList.get();
        if (curViews == null || curViews.isEmpty()) {
            return null;
        }
        return curViews.get(curViews.size() - 1);
    }

    /***
     * Return the start position of these FileSegments.
     *
     * @return
     */
    @Override
    public long getMinOffset() {
        final List<Segment> curViews = segmentList.get();
        if (curViews == null || curViews.isEmpty()) {
            return 0L;
        }
        long last = 0L;
        int curSize = curViews.size();
        Segment tmpSeg;
        for (int i = 0; i < curSize; i++) {
            tmpSeg = curViews.get(0);
            if (tmpSeg == null) {
                continue;
            }
            if (tmpSeg.isExpired()) {
                last = tmpSeg.getCommitLast();
                continue;
            }
            return tmpSeg.getStart();
        }
        return last;
    }

    /***
     * Return the max position of these FileSegments.
     *
     * @return
     */
    @Override
    public long getMaxOffset() {
        final List<Segment> curViews = segmentList.get();
        if (curViews == null || curViews.isEmpty()) {
            return 0L;
        }
        Segment last = curViews.get(curViews.size() - 1);
        if (last == null) {
            return 0L;
        }
        return last.getLast();
    }

    /***
     * Return the max position that have been flushed to disk.
     *
     * @return
     */
    @Override
    public long getCommitMaxOffset() {
        final List<Segment> curViews = segmentList.get();
        if (curViews == null || curViews.isEmpty()) {
            return 0L;
        }
        Segment last = curViews.get(curViews.size() - 1);
        if (last == null) {
            return 0L;
        }
        return last.getCommitLast();
    }

    @Override
    public long getSizeInBytes() {
        long sum = 0;
        final List<Segment> curViews = segmentList.get();
        if (curViews == null || curViews.isEmpty()) {
            return sum;
        }
        for (final Segment seg : curViews) {
            if (seg != null) {
                sum += seg.getCachedSize();
            }
        }
        return sum;
    }

    public Segment findSegment(final long offset) {
        // 二分法查找包含offset的segment
        final List<Segment> curViews = segmentList.get();
        if (curViews == null || curViews.isEmpty()) {
            return null;
        }
        int curSize = curViews.size();
        final Segment start = curViews.get(0);
        if (offset < start.getStart()) {
            throw new ArrayIndexOutOfBoundsException(new StringBuilder(512)
                    .append("Request offsets is ").append(offset)
                    .append(", the start is ").append(start.getStart()).toString());
        }
        final Segment last = curViews.get(curSize - 1);
        if (offset >= last.getStart() + last.getCachedSize()) {
            return null;
        }
        int low = 0;
        int high = curSize - 1;
        while (low <= high) {
            final int mid = high + low >>> 1;
            final Segment found = curViews.get(mid);
            if (found.contains(offset)) {
                return found;
            } else if (offset < found.getStart()) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return null;
    }

    private void validateSegments(final List<Segment> segments) {
        //　校验segment文件列表需要首尾相连，如果不关联则表示文件有损坏，需要人工介入处理
        for (int i = 0; i < segments.size() - 1; i++) {
            final Segment curr = segments.get(i);
            final Segment next = segments.get(i + 1);
            if (curr.getStart() + curr.getCachedSize() != next.getStart()) {
                throw new IllegalStateException(new StringBuilder(512)
                        .append("The following ").append(segTypeStr)
                        .append(" segments don't validate: ")
                        .append(curr.getFile().getAbsolutePath()).append(", ")
                        .append(next.getFile().getAbsolutePath()).toString());
            }
        }
    }

}

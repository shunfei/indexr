package io.indexr.segment.rt;

import com.google.common.base.Preconditions;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import io.indexr.segment.Row;
import io.indexr.segment.Segment;
import io.indexr.segment.SegmentFd;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.storage.DPSegment;
import io.indexr.segment.storage.OpenOption;
import io.indexr.segment.storage.StorageSegment;
import io.indexr.segment.storage.Version;
import io.indexr.util.Try;

/**
 * Merge a list of dumped realtime segments.
 */
public class RTSMerge {
    private static final Logger logger = LoggerFactory.getLogger(RTSMerge.class);
    private static final Comparator<UTF8Row> comparator = UTF8Row.dimBytesComparator();

    public static StorageSegment merge(
            boolean grouping,
            SegmentSchema schema,
            SegmentMode mode,
            List<String> dims,
            List<Metric> metrics,
            List<SegmentFd> fds,
            Path path,
            String name) throws IOException {
        DPSegment mergeSegment = null;
        List<StorageSegment> segments = new ArrayList<>();
        try {
            for (SegmentFd fd : fds) {
                segments.add((StorageSegment) (fd.open()));
            }
            int version = fds.size() == 0 ? Version.LATEST_ID : segments.get(0).version();
            mergeSegment = DPSegment.open(
                    version,
                    mode,
                    path,
                    name,
                    schema,
                    OpenOption.Overwrite);
            mergeSegment.update();

            if (dims == null || dims.size() == 0) {
                // No need to sort, use the fast way - copy bytes directly.
                mergeSegment.merge(segments);
            } else {
                UTF8Row.Creator creator = new UTF8Row.Creator(
                        grouping,
                        schema.columns,
                        dims,
                        metrics,
                        null,
                        null,
                        EventIgnoreStrategy.NO_IGNORE);
                sortedMerge(creator, mergeSegment, segments);
            }

            mergeSegment.seal();
            return mergeSegment;
        } catch (Exception e) {
            IOUtils.closeQuietly(mergeSegment);
            throw e;
        } finally {
            for (Segment segment : segments) {
                Try.on(segment::close, logger);
            }
        }
    }

    public static void sortedMerge(UTF8Row.Creator creator,
                                   DPSegment mergeSegment,
                                   List<? extends StorageSegment> segments) throws IOException {
        if (segments.isEmpty()) {
            return;
        }

        Iterator<Row>[] rowItrs = new Iterator[segments.size()];
        long totalRowCount = 0;
        for (int i = 0; i < segments.size(); i++) {
            Segment segment = segments.get(i);
            rowItrs[i] = segment.rowTraversal().iterator();
            totalRowCount += segment.rowCount();
        }
        SortedMergeUTF8RowItr sortedMergeItr = new SortedMergeUTF8RowItr(creator, rowItrs);
        UTF8Row last = null;
        long rowCount = 0;
        while (sortedMergeItr.hasNext()) {
            UTF8Row curRow = sortedMergeItr.next();
            rowCount++;
            if (last == null) {
                last = curRow;
            } else {
                assert comparator.compare(curRow, last) >= 0;
                if (comparator.compare(curRow, last) == 0) {
                    last.merge(curRow);
                    curRow.free();
                } else {
                    mergeSegment.add(last);
                    last.free();
                    last = curRow;
                }
            }
        }
        if (last != null) {
            mergeSegment.add(last);
            last.free();
            last = null;
        }

        long itrRowCount = sortedMergeItr.checkDone();
        Preconditions.checkState(rowCount == totalRowCount && itrRowCount == totalRowCount,
                "Row valueCount: %s, itr rowCount: %s, expected: %s not match",
                rowCount, itrRowCount, totalRowCount);
    }
}

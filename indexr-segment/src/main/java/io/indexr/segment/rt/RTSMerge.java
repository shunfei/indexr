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

import io.indexr.data.BytePiece;
import io.indexr.segment.ColumnSchema;
import io.indexr.segment.ColumnType;
import io.indexr.segment.Row;
import io.indexr.segment.Segment;
import io.indexr.segment.SegmentFd;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.pack.DPSegment;
import io.indexr.segment.pack.OpenOption;
import io.indexr.segment.pack.StorageSegment;
import io.indexr.segment.pack.Version;
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
                    path,
                    name,
                    schema,
                    OpenOption.Overwrite);
            mergeSegment.update();

            //if (true) {
            if (dims == null || dims.size() == 0) {
                // No need to sort, use the fast way - copy bytes directly.
                mergeSegment.merge(segments);
            } else {
                sortedMerge(grouping, schema, dims, metrics, mergeSegment, segments);
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

    private static void sortedMerge(boolean grouping,
                                    SegmentSchema schema,
                                    List<String> dims,
                                    List<Metric> metrics,
                                    DPSegment mergeSegment,
                                    List<StorageSegment> segments) throws IOException {
        if (segments.isEmpty()) {
            return;
        }
        UTF8Row.Creator creator = new UTF8Row.Creator(
                grouping,
                schema.columns,
                dims,
                metrics,
                null,
                null,
                EventIgnoreStrategy.NO_IGNORE);
        Iterator<Row>[] rowItrs = new Iterator[segments.size()];
        long totalRowCount = 0;
        for (int i = 0; i < segments.size(); i++) {
            Segment segment = segments.get(i);
            rowItrs[i] = segment.rowTraversal().iterator();
            totalRowCount += segment.rowCount();
        }
        MergedSortItr mergedSortItr = new MergedSortItr(creator, schema, rowItrs);
        UTF8Row last = null;
        long rowCount = 0;
        while (mergedSortItr.hasNext()) {
            UTF8Row curRow = mergedSortItr.next();
            rowCount++;
            if (last == null) {
                last = curRow;
            } else {
                assert comparator.compare(curRow, last) >= 0;
                if (grouping && comparator.compare(curRow, last) == 0) {
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

        long itrRowCount = mergedSortItr.checkDone();
        Preconditions.checkState(rowCount == totalRowCount && itrRowCount == totalRowCount,
                "Row count: %s, itr rowCount: %s, expected: %s not match",
                rowCount, itrRowCount, totalRowCount);
    }

    /**
     * Merge a list of sorted iterators.
     */
    private static class MergedSortItr implements Iterator<UTF8Row> {
        private final UTF8Row.Creator creator;
        private final SegmentSchema schema;
        private final int itrSize;
        private final Iterator<Row>[] itrs;

        private final UTF8Row[] utf8RowCache;
        private long rowCount = 0;

        public MergedSortItr(UTF8Row.Creator creator, SegmentSchema schema, Iterator<Row>[] itrs) {
            this.creator = creator;
            this.schema = schema;
            this.itrs = itrs;
            this.itrSize = itrs.length;
            this.utf8RowCache = new UTF8Row[itrSize];
        }

        private boolean fillCache() {
            boolean remain = false;
            for (int i = 0; i < itrSize; i++) {
                if (utf8RowCache[i] != null) {
                    remain = true;
                    continue;
                }
                if (itrs[i].hasNext()) {
                    utf8RowCache[i] = toUTF8Row(itrs[i].next());
                    remain = true;
                    rowCount++;
                }
            }
            return remain;
        }

        private UTF8Row toUTF8Row(Row row) {
            List<ColumnSchema> csList = schema.getColumns();
            BytePiece bp = new BytePiece();
            creator.startRow();
            for (int id = 0; id < csList.size(); id++) {
                creator.onColumnId(id);

                ColumnSchema cs = csList.get(id);
                switch (cs.getDataType()) {
                    case ColumnType.INT:
                        creator.onIntValue(row.getInt(id));
                        break;
                    case ColumnType.LONG:
                        creator.onLongValue(row.getLong(id));
                        break;
                    case ColumnType.FLOAT:
                        creator.onFloatValue(row.getFloat(id));
                        break;
                    case ColumnType.DOUBLE:
                        creator.onDoubleValue(row.getDouble(id));
                        break;
                    case ColumnType.STRING:
                        row.getRaw(id, bp);
                        assert bp.base == null;
                        creator.onStringValue(bp.addr, bp.len);
                        break;
                    default:
                        throw new IllegalStateException("Illegal type: " + cs.getDataType());
                }
            }
            return creator.endRow();
        }

        @Override
        public boolean hasNext() {
            return fillCache();
        }

        @Override
        public UTF8Row next() {
            // Find the least from cache.
            UTF8Row least = null;
            int leastId = -1;
            for (int i = 0; i < itrSize; i++) {
                UTF8Row curRow = utf8RowCache[i];
                if (curRow == null) {
                    continue;
                }
                if (leastId == -1 || comparator.compare(curRow, least) < 0) {
                    leastId = i;
                    least = curRow;
                }
            }
            utf8RowCache[leastId] = null;
            return least;
        }

        public long checkDone() {
            for (int i = 0; i < itrSize; i++) {
                Preconditions.checkState(utf8RowCache[i] == null && !itrs[i].hasNext(), "Not empty");
            }
            return rowCount;
        }
    }
}

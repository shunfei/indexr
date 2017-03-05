package io.indexr.segment.rt;

import com.google.common.base.Preconditions;

import java.util.Comparator;
import java.util.Iterator;

import io.indexr.segment.Row;

/**
 * Merge a list of sorted iterators.
 */
public class SortedMergeUTF8RowItr implements Iterator<UTF8Row> {
    private static final Comparator<UTF8Row> comparator = UTF8Row.dimBytesComparator();

    private final UTF8Row.Creator creator;
    private final int itrSize;
    private final Iterator<Row>[] itrs;

    private final UTF8Row[] utf8RowCache;
    private long rowCount = 0;

    public SortedMergeUTF8RowItr(UTF8Row.Creator creator, Iterator<Row>[] itrs) {
        this.creator = creator;
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
        return UTF8Row.from(creator, row);
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

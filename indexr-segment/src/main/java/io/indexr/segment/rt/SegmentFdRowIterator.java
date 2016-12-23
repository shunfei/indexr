package io.indexr.segment.rt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import io.indexr.segment.Row;
import io.indexr.segment.Segment;
import io.indexr.segment.SegmentFd;
import io.indexr.segment.pack.IndexMemCache;
import io.indexr.segment.pack.PackMemCache;
import io.indexr.util.Try;

/**
 * Only for test.
 */
public class SegmentFdRowIterator implements Iterator<Row>, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(SegmentFdRowIterator.class);

    private IndexMemCache indexMemCache;
    private PackMemCache packMemCache;
    private List<SegmentFd> segments;
    private Segment current;
    private Iterator<Row> currentIterator = null;
    private int nextIndex = 0;

    public SegmentFdRowIterator(List<SegmentFd> segments, IndexMemCache indexMemCache, PackMemCache packMemCache) {
        this.segments = segments;
        this.indexMemCache = indexMemCache;
        this.packMemCache = packMemCache;
    }

    @Override
    public boolean hasNext() {
        if (currentIterator != null && currentIterator.hasNext()) {
            return true;
        } else {
            while (true) {
                if (current != null) {
                    Try.on(current::close, logger);
                    current = null;
                }
                if (nextIndex >= segments.size()) {
                    return false;
                }
                SegmentFd segmentFd = segments.get(nextIndex);
                nextIndex++;
                //System.out.printf("next segment: [%s]\n", rts.name());
                try {
                    current = segmentFd.open(indexMemCache, packMemCache);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                currentIterator = current.rowTraversal().iterator();
                if (currentIterator.hasNext()) {
                    return true;
                }
            }
        }
    }

    @Override
    public Row next() {
        return currentIterator.next();
    }

    @Override
    public void close() throws IOException {
        if (current != null) {
            current.close();
            current = null;
        }
    }
}

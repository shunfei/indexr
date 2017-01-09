package io.indexr.segment;

import java.io.IOException;

import io.indexr.segment.pack.IndexMemCache;
import io.indexr.segment.pack.PackMemCache;

/**
 * A segment pointer.
 * {@link #info()} provides {@link InfoSegment}, and {@link #open(IndexMemCache, PackMemCache)} opens the real {@link Segment}.
 * Because open a segment is a relative heavy operation, it is a good idea to filter out those not
 * related segments by info segments.
 * 
 * A {@link SegmentFd} is a light weight java object, it is fine to create many of them and throw away.
 * JVM will clean them up just like normal java object.
 * But The segment return by {@link #open(IndexMemCache, PackMemCache)} is resource related, user should call {@link Segment#close()}
 * to release it after done with.
 */
public interface SegmentFd {

    String name();

    InfoSegment info();

    Segment open(IndexMemCache indexMemCache, PackMemCache packMemCache) throws IOException;

    /**
     * Same as <code>open(null, null)</code>.
     */
    default Segment open() throws IOException {
        return open(null, null);
    }

    /**
     * Wrap a segment into segmentFd.
     */
    public static SegmentFd wrap(Segment segment) {
        return new SegmentFd() {
            @Override
            public String name() {
                return segment.name();
            }

            @Override
            public InfoSegment info() {
                return segment;
            }

            @Override
            public Segment open(IndexMemCache indexMemCache, PackMemCache packMemCache) throws IOException {
                return segment;
            }
        };
    }
}

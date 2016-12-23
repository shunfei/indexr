package io.indexr.segment.helper;

import java.io.IOException;

import io.indexr.segment.Segment;

public interface SegmentOpener {
    /**
     * Open a segment by its name.
     */
    Segment open(String name) throws IOException;
}

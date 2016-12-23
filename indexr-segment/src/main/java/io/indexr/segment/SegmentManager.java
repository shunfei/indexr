package io.indexr.segment;

import java.util.List;

public interface SegmentManager {
    boolean exists(String name) throws Exception;

    List<String> allSegmentNames() throws Exception;

    /**
     * Add segment into storage system.
     */
    void add(Segment segment) throws Exception;

    void remove(String name) throws Exception;

}
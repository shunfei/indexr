package io.indexr.segment;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;

public interface SegmentPool extends Closeable {

    SegmentFd get(String name);

    List<SegmentFd> all();

    void refresh(boolean force);

    /**
     * Get the host names which contains realtime segments.
     */
    default List<String> realtimeHosts() {return Collections.emptyList();}

    @Override
    void close();
}

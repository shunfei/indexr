package io.indexr.segment;

import java.io.IOException;
import java.util.List;

public interface SegmentLocality {
    List<String> getHosts(String segmentName, boolean isRealtime) throws IOException;
}

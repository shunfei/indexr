package io.indexr.segment;

import java.io.IOException;

import io.indexr.segment.pack.StorageSegment;

@FunctionalInterface
public interface SegmentUploader {
    /**
     * Upload a segment to storage system.
     *
     * @param segment The segment to upload.
     * @param openFd  Open the segment fd or not after uploaded.
     * @return The segment fd.
     */
    SegmentFd upload(StorageSegment segment, boolean openFd) throws IOException;
}

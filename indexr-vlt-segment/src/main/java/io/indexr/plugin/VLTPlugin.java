package io.indexr.plugin;

import io.indexr.vlt.segment.VersionAdapter_VLT;

import io.indexr.segment.SegmentMode;

public class VLTPlugin {
    public static SegmentMode VLT = new SegmentMode(10001, "vlt", new VersionAdapter_VLT());

    @InitPlugin
    public static void init() {
        SegmentMode.addMode(VLT);
        SegmentMode.setDefault(VLT);
    }
}

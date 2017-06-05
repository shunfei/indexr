package io.indexr.vlt.segment.storage;

import io.indexr.plugin.Plugins;
import io.indexr.segment.storage.SortedSegmentGeneratorTest;

public class SortedSegmentGeneratorTest_VLT extends SortedSegmentGeneratorTest {
    static {
        Plugins.loadPluginsNEX();
    }
}

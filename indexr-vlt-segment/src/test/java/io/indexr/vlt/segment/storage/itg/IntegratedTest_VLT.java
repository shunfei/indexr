package io.indexr.vlt.segment.storage.itg;

import org.junit.Test;

import java.io.IOException;

import io.indexr.plugin.Plugins;
import io.indexr.plugin.VLTPlugin;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.storage.TestRows;
import io.indexr.segment.storage.Version;
import io.indexr.segment.storage.itg.IntegratedTest;

public class IntegratedTest_VLT extends IntegratedTest {
    static {
        Plugins.loadPluginsNEX();
    }

    @Test
    public void testUpdateColumn2() throws IOException {
        Version version = Version.VERSION_8;
        SegmentMode mode = VLTPlugin.VLT;
        long time = System.currentTimeMillis();
        testCache(version.id, mode, TestRows.genRandomRows(TestRows.ROW_COUNT));
        testCache(version.id, mode, TestRows.genRows(TestRows.ROW_COUNT));
    }
}

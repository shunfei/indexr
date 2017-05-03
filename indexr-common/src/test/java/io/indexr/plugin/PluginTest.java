package io.indexr.plugin;

import org.junit.Assert;
import org.junit.Test;

public class PluginTest {
    @InitPlugin
    public static void init() {
        System.out.println("Hello!");
    }

    @Test
    public void test() throws Exception {
        Assert.assertTrue(Plugins.loadPlugins());
    }
}

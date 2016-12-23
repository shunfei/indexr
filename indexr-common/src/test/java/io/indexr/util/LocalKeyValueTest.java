package io.indexr.util;

import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public class LocalKeyValueTest {
    @Test
    public void test() throws Exception {
        Path path = Files.createTempFile("locakeyvalue_test_", null);
        LocalKeyValue keyValue = new LocalKeyValue(path);
        keyValue.set("k0".getBytes(), "v0".getBytes());
        keyValue.set("k1".getBytes(), "v1".getBytes());
        keyValue.set("k0".getBytes(), "v2".getBytes());


        for (LocalKeyValue.KV kv : keyValue) {
            System.out.printf("key: %s, value: %s\n", UTF8Util.fromUtf8(kv.key), UTF8Util.fromUtf8(kv.value));
        }

        Assert.assertEquals(true, Arrays.equals("v2".getBytes(), keyValue.get("k0".getBytes())));
        Assert.assertEquals(true, Arrays.equals("v1".getBytes(), keyValue.get("k1".getBytes())));

        for (int i = 0; i < 2000; i++) {
            keyValue.set("k0".getBytes(), "v0".getBytes());
        }

        Files.deleteIfExists(path);
    }
}

package io.indexr.util;

import org.apache.spark.unsafe.Platform;
import org.junit.Assert;
import org.junit.Test;

public class UTF8UtilTest {
    @Test
    public void parseLong_test() {
        long l = UTF8Util.parseLong(UTF8Util.toUtf8("-2143400"), 0, 7);
        Assert.assertEquals(-214340, l);
        l = UTF8Util.parseLong(UTF8Util.toUtf8("+2143400"), 0, 8);
        Assert.assertEquals(2143400, l);
        l = UTF8Util.parseLong(UTF8Util.toUtf8("2143400"), 1, 3);
        Assert.assertEquals(143, l);


        Assert.assertEquals(0, Double.compare(0d, Double.longBitsToDouble(0)));
    }

    @Test
    public void containsCommaSep_test() throws Exception {
        byte[] sBytes = UTF8Util.toUtf8("wwww,we");
        byte[] subBytes = UTF8Util.toUtf8("we");
        Assert.assertTrue(UTF8Util.containsCommaSep(sBytes, 1, sBytes.length - 1, subBytes));


        assertCommaSep("we", "we", true);
        assertCommaSep("wwww,we", "we", true);
        assertCommaSep("we,www", "we", true);
        assertCommaSep("aaa,we,www", "we", true);
        assertCommaSep("aaa, we ,www", "we", true);
        assertCommaSep(" we ,www", "we", true);
        assertCommaSep("www, we ", "we", true);
        assertCommaSep(" we ", "we", true);
        assertCommaSep(" w  e e     , ww", "w  e e", true);

        assertCommaSep(" we aa,ww ", "we", false);
        assertCommaSep(" we1 ", "we", false);
        assertCommaSep(" w1e ", "we", false);
        assertCommaSep("awe, wff", "we", false);
        assertCommaSep(",,,,", "we", false);
        assertCommaSep("   ", "we", false);
        assertCommaSep("", "we", false);
    }

    private static void assertCommaSep(String s, String sub, boolean isTrue) {
        byte[] sBytes = UTF8Util.toUtf8(s);
        byte[] subBytes = UTF8Util.toUtf8(sub);
        if (isTrue) {
            Assert.assertTrue(UTF8Util.containsCommaSep(sBytes, subBytes));
            Assert.assertTrue(UTF8Util.containsCommaSep((Object) sBytes, Platform.BYTE_ARRAY_OFFSET, sBytes.length, subBytes));
        } else {
            Assert.assertFalse(UTF8Util.containsCommaSep(sBytes, subBytes));
            Assert.assertFalse(UTF8Util.containsCommaSep((Object) sBytes, Platform.BYTE_ARRAY_OFFSET, sBytes.length, subBytes));
        }
    }
}

package io.indexr.util;

import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Assert;
import org.junit.Test;

public class SQLLikeTest {
    @Test
    public void test() {
        Assert.assertEquals(true, match("wefewf%2333", "wefewfww$222ww#$532333"));
        Assert.assertEquals(false, match("", "wefewfww$222ww#$532333"));
        Assert.assertEquals(false, match("ww", "wefewfww$222ww#$532333"));
        Assert.assertEquals(true, match("%ww%", "wefewfww$222ww#$532333"));
        Assert.assertEquals(true, match("%aa", "wefewfww$222ww#$532333aa"));
        Assert.assertEquals(true, match("%gggg%", "wefewfww$222ww#$532gggg333"));
        Assert.assertEquals(true, match("%gggg_GG", "wefewfww$222ww#$532gggg3GG"));
        Assert.assertEquals(true, match("%ooo%", "gg你妹gg_G呵呵ooo哒喵喵喵G"));
        Assert.assertEquals(true, match("_ああ___你妹%あなたの妹", "?ああ_gg你妹gg_G呵呵ooo哒\t\b喵喵喵Gあなたの妹"));
        Assert.assertEquals(false, match("%ooo", "gg你妹gg_G呵呵ooo哒喵喵喵G"));
        Assert.assertEquals(true, match("aa", "aa"));
    }

    private boolean match(String patern, String str) {
        return SQLLike.match(UTF8String.fromString(str), UTF8String.fromString(patern));
    }
}

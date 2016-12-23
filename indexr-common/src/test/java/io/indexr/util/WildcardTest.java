package io.indexr.util;

import org.junit.Assert;
import org.junit.Test;

public class WildcardTest {
    @Test
    public void test() {
        Assert.assertEquals(true, match("wefewf*2333", "wefewfww$222ww#$532333"));
        Assert.assertEquals(false, match("", "wefewfww$222ww#$532333"));
        Assert.assertEquals(false, match("ww", "wefewfww$222ww#$532333"));
        Assert.assertEquals(true, match("*ww*", "wefewfww$222ww#$532333"));
        Assert.assertEquals(true, match("*aa", "wefewfww$222ww#$532333aa"));
        Assert.assertEquals(true, match("*gggg*", "wefewfww$222ww#$532gggg333"));
        Assert.assertEquals(true, match("*gggg?GG", "wefewfww$222ww#$532gggg3GG"));
        Assert.assertEquals(true, match("*ooo*", "gg你妹gg?G呵呵ooo哒喵喵喵G"));
        Assert.assertEquals(true, match("?ああ*あなたの妹", "？ああgg你妹gg?G呵呵ooo哒\t\b喵喵喵Gあなたの妹"));
        Assert.assertEquals(false, match("*ooo", "gg你妹gg?G呵呵ooo哒喵喵喵G"));
        Assert.assertEquals(true, match("aa", "aa"));
    }

    private boolean match(CharSequence patern, CharSequence str) {
        return Wildcard.match(str, patern);
    }

    @Test
    public void tt() {
        float f = 3;
        long l = Double.doubleToRawLongBits(f);
        Assert.assertEquals(f, (float) Double.longBitsToDouble(l), 0);

        double d = 3;
        long l2 = Double.doubleToRawLongBits(d);

        Assert.assertEquals(l, l2);

        Assert.assertEquals(d, Double.longBitsToDouble(l2), 0);
        Assert.assertEquals(f, (float) Double.longBitsToDouble(l2), 0);
    }
}

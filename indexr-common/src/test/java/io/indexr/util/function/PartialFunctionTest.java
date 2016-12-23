package io.indexr.util.function;

import org.junit.Assert;
import org.junit.Test;

public class PartialFunctionTest {

    @Test
    public void test() {
        Assert.assertEquals(0L, Double.doubleToRawLongBits(0.0));
        Assert.assertEquals(0, Double.compare(Double.longBitsToDouble(0L), 0.0d));
        PartialFunction<Integer, Long> f1 = new PartialFunction<Integer, Long>() {
            @Override
            public boolean isDefinedAt(Integer x) {
                return x == 1;
            }

            @Override
            public Long apply(Integer integer) {
                return integer.longValue() * 10;
            }
        };
        PartialFunction<Integer, Long> f2 = new PartialFunction<Integer, Long>() {
            @Override
            public boolean isDefinedAt(Integer x) {
                return x == 2;
            }

            @Override
            public Long apply(Integer integer) {
                return integer.longValue() * 20;
            }
        };

        PartialFunction<Integer, String> f = f1.andThen(Object::toString);
        Assert.assertEquals("10", f.applyOrElse(1, i -> i.toString() + "else"));
        Assert.assertEquals("2else", f.applyOrElse(2, i -> i.toString() + "else"));
        Assert.assertEquals("10", f.lift().apply(1));
        Assert.assertEquals(null, f.lift().apply(2));

        PartialFunction<Integer, Long> fs = PartialFunctions.applyOrElse(f1, f2);
        Assert.assertEquals((long) 10, (long) fs.apply(1));
        Assert.assertEquals((long) 40, (long) fs.apply(2));


    }
}

package io.indexr.util;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;

public class ReflectionTest {
    static class A {}

    static class B extends A {}

    static class C {
        int ctrId;

        public C(A a, B b, int i) {
            ctrId = 1;
        }

        public C(B b, int i) {
            ctrId = 2;
        }
    }

    @Test
    public void getConstructorTest() throws Exception {
        Object[] args1 = {new A(), new B(), 10};
        Object[] args2 = {new B(), 10};
        Constructor<C> ctr1 = Reflection.getConstructor(C.class, args1);
        Constructor<C> ctr2 = Reflection.getConstructor(C.class, args2);
        Assert.assertNotNull(ctr1);
        Assert.assertNotNull(ctr2);
        Assert.assertEquals(1, ctr1.newInstance(args1).ctrId);
        Assert.assertEquals(2, ctr2.newInstance(args2).ctrId);
    }
}

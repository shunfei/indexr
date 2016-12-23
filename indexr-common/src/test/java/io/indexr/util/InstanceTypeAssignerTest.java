package io.indexr.util;

import org.junit.Assert;
import org.junit.Test;

public class InstanceTypeAssignerTest {

    public static class A {
        int a;

        public A(int a) {
            this.a = a;
        }
    }

    public static class B {
        String s;

        public B(String s) {
            this.s = s;
        }
    }

    public static class BB extends B {
        long b;

        public BB(String s, long b) {
            super(s);
            this.b = b;
        }
    }

    public static class CC extends B {
        float c;

        public CC(String s, float c) {
            super(s);
            this.c = c;
        }
    }

    public class MyClass {
        public A a;
        public B b;
        public BB bb;
        public CC cc;
    }

    @Test
    public void test() {
        MyClass me = new MyClass();
        InstanceTypeAssigner instanceTypeAssigner = new InstanceTypeAssigner(me, B.class);
        B meB = new B("b");
        BB meBB = new BB("bb", 4);
        CC meCC = new CC("cc", 5);
        Object[] objects = new Object[]{10, "ssss", new A(22), meB, meBB, meCC};
        for (Object o : objects) {
            instanceTypeAssigner.tryAssign(o);
        }
        Assert.assertEquals(null, me.a);
        Assert.assertEquals(meB, me.b);
        Assert.assertEquals(meBB, me.bb);
        Assert.assertEquals(meCC, me.cc);

    }

}

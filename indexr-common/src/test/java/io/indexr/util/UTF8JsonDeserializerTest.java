package io.indexr.util;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class UTF8JsonDeserializerTest {

    private static class MyListener implements UTF8JsonDeserializer.Listener {
        @Override
        public void onStartJson(int offset) {
            System.out.print("{");
        }

        @Override
        public void onFinishJson(int offset, int len) {
            System.out.print("}");
        }

        @Override
        public int onKey(ByteBuffer key, int size) {
            String s = UTF8Util.fromUtf8(key, size);
            if (s.isEmpty()) {
                System.out.print("\"\": ");
                return UTF8JsonDeserializer.VARCHAR;
            }
            if (s.charAt(0) == 'v') {
                return UTF8JsonDeserializer.INVALID;
            }
            System.out.printf("\"%s\": ", s);
            if (s.isEmpty()) {
                return UTF8JsonDeserializer.VARCHAR;
            }
            switch (s.charAt(0)) {
                case 'i':
                    return UTF8JsonDeserializer.INT;
                case 'l':
                    return UTF8JsonDeserializer.BIGINT;
                case 'f':
                    return UTF8JsonDeserializer.FLOAT;
                case 'd':
                    return UTF8JsonDeserializer.DOUBLE;
                case 's':
                    return UTF8JsonDeserializer.VARCHAR;
                default:
                    return UTF8JsonDeserializer.VARCHAR;
            }
        }

        @Override
        public boolean onStringValue(ByteBuffer value, int size) {
            System.out.printf("\"%s\", ", UTF8Util.fromUtf8(value, size));
            return true;
        }

        @Override
        public boolean onIntValue(int value) {
            System.out.print(value + ", ");
            return true;
        }

        @Override
        public boolean onLongValue(long value) {
            System.out.print(value + ", ");
            return true;
        }

        @Override
        public boolean onFloatValue(float value) {
            System.out.print(value + ", ");
            return true;
        }

        @Override
        public boolean onDoubleValue(double value) {
            System.out.print(value + ", ");
            return true;
        }
    }

    @Test
    public void test() throws Exception {
        String s = "  {\"ibbeee\": \"222\", \"sfff\": \"3333\"},  {\"sa\\\"aa\\\\ee}\": \"\\\"c    \\\\\\\"c\", \"veeee\":222, \"f\\\\我是帅逼！！！！…………服务范围分为--2342342=4=242344￥￥1111\\\\^\\\\&_@@@@     \\\\\\\\\\\"43*\": -12.33, \"i33*^3\\\\&\\\\\": 33, \"\" : \"+9.9\", \"i''\": 55, \"v445^^#2\": 22}   ";
        //String s = "{\"saa\": \"11cc\", \"ifff\": 3333,}";
        UTF8JsonDeserializer des = new UTF8JsonDeserializer();
        Assert.assertTrue(des.parse(s.getBytes(UTF8Util.UTF8_NAME), new MyListener()));
    }
}

package io.indexr.util;

import java.nio.ByteBuffer;

import io.indexr.io.ByteSlice;

public class UTF16Util {

    public static int byteLen(CharSequence str) {
        return str.length() << 1;
    }

    public static void writeString(ByteSlice des, int offset, CharSequence str) {
        for (int i = 0; i < str.length(); i++) {
            // 1 char = 2 bytes
            des.putChar(offset + (i << 1), str.charAt(i));
        }
    }

    public static void writeString(ByteBuffer des, CharSequence str) {
        for (int i = 0; i < str.length(); i++) {
            des.putChar(str.charAt(i));
        }
    }

}

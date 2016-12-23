package io.indexr.util;

import java.util.Arrays;
import java.util.Collection;

public class ArrayUtil {
    public static boolean contains(Collection<byte[]> bytes, byte[] check) {
        for (byte[] b : bytes) {
            if (Arrays.equals(b, check)) {
                return true;
            }
        }
        return false;
    }
}

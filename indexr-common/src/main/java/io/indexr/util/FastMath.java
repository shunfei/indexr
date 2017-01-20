package io.indexr.util;

public class FastMath {
    private static final long[] CeilLogMagicWand = new long[]{
            0xFFFFFFFF00000000L,
            0x00000000FFFF0000L,
            0x000000000000FF00L,
            0x00000000000000F0L,
            0x000000000000000CL,
            0x0000000000000002L
    };

    // http://stackoverflow.com/a/15327567
    public static int ceilLog2(long x) {
        int y = (((x & (x - 1)) == 0) ? 0 : 1);
        int j = 32;
        int i;

        for (i = 0; i < 6; i++) {
            int k = (((x & CeilLogMagicWand[i]) == 0) ? 0 : j);
            y += k;
            x >>= k;
            j >>= 1;
        }

        return y;
    }

    public static void main(String[] args) {
        for (int i = 0; i < 65; i++) {
            System.out.printf("%d: %d\n", i, i == 0 ? 0 : 1 << ceilLog2(i));
        }
    }
}

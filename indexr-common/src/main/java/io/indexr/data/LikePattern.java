package io.indexr.data;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

public class LikePattern {
    public final UTF8String original;

    public final int minMatchLen;
    public final byte[] analyzed;
    public final boolean[] any;
    public final boolean[] one;
    public final boolean hasAny;
    public final boolean hasOne;

    public LikePattern(UTF8String pattern) {
        int size = pattern.numBytes();
        Object base = pattern.getBaseObject();
        long offset = pattern.getBaseOffset();

        byte[] analyzed = new byte[size];
        boolean[] any = new boolean[size];
        boolean[] one = new boolean[size];

        boolean hasAny = false;
        boolean hasOne = false;

        int analyzedIndex = 0;
        int minMatchLen = 0;
        boolean escaped = false;
        for (int i = 0; i < size; i++) {
            byte c = Platform.getByte(base, offset + i);
            switch (c) {
                case '\\':
                    if (escaped) {
                        escaped = false;

                        minMatchLen++;
                        analyzed[analyzedIndex] = c;
                        any[analyzedIndex] = false;
                        one[analyzedIndex] = false;

                        analyzedIndex++;
                    } else {
                        escaped = true;
                    }
                    break;
                case '_':
                    if (escaped) {
                        escaped = false;

                        minMatchLen++;
                        analyzed[analyzedIndex] = c;
                        any[analyzedIndex] = false;
                        one[analyzedIndex] = false;

                        analyzedIndex++;
                    } else {
                        minMatchLen++;

                        analyzed[analyzedIndex] = 0;
                        any[analyzedIndex] = false;
                        one[analyzedIndex] = true;
                        hasOne = true;

                        analyzedIndex++;
                    }
                    break;
                case '%':
                    if (escaped) {
                        escaped = false;

                        minMatchLen++;
                        analyzed[analyzedIndex] = c;
                        any[analyzedIndex] = false;
                        one[analyzedIndex] = false;

                        analyzedIndex++;
                    } else {
                        analyzed[analyzedIndex] = 0;
                        any[analyzedIndex] = true;
                        one[analyzedIndex] = false;
                        hasAny = true;

                        analyzedIndex++;
                    }
                    break;
                default:
                    if (escaped) {
                        // It is an illegal like pattern, but we don't complain here.
                        escaped = false;

                        minMatchLen += 2;

                        analyzed[analyzedIndex] = '\\';
                        any[analyzedIndex] = false;
                        one[analyzedIndex] = false;
                        analyzedIndex++;

                        analyzed[analyzedIndex] = c;
                        any[analyzedIndex] = false;
                        one[analyzedIndex] = false;
                        analyzedIndex++;
                    } else {
                        minMatchLen++;

                        analyzed[analyzedIndex] = c;
                        any[analyzedIndex] = false;
                        one[analyzedIndex] = false;

                        analyzedIndex++;
                    }
            }
        }

        this.original = pattern;
        this.minMatchLen = minMatchLen;
        this.analyzed = new byte[analyzedIndex];
        this.any = new boolean[analyzedIndex];
        this.one = new boolean[analyzedIndex];
        this.hasAny = hasAny;
        this.hasOne = hasOne;

        System.arraycopy(analyzed, 0, this.analyzed, 0, analyzedIndex);
        System.arraycopy(any, 0, this.any, 0, analyzedIndex);
        System.arraycopy(one, 0, this.one, 0, analyzedIndex);
    }
}

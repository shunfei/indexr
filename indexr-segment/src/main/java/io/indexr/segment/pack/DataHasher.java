package io.indexr.segment.pack;

import org.apache.spark.unsafe.types.UTF8String;

import io.indexr.util.Hasher32;

public class DataHasher {
    // DO NOT CHANGE SEED VALUE!!!
    private static final Hasher32 HASHER_32 = new Hasher32(0);

    public static int stringHash(UTF8String s) {
        return HASHER_32.hashUnsafeWords(s.getBaseObject(), s.getBaseOffset(), s.numBytes());
    }
}

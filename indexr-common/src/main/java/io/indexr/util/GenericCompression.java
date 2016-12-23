package io.indexr.util;

import org.xerial.snappy.Snappy;

public class GenericCompression {

    public static byte[] compress(byte[] uncompressData) {
        try {
            return Snappy.compress(uncompressData);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static byte[] decomress(byte[] compressedData) {
        try {
            return Snappy.uncompress(compressedData);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}

package io.indexr.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import jodd.util.collection.IntHashMap;

/**
 * A hasher try to map known limited byte arrays into unique ints.
 */
public class UniqueHasher {
    private static final Logger logger = LoggerFactory.getLogger(UniqueHasher.class);

    /**
     * Try create one, return null if failed.
     *
     * @param objects  the limited byte array objects.
     * @param tryTimes the iteration count of try
     */
    public static Hasher32 tryCreate(List<byte[]> objects, int tryTimes) {
        Hasher32 hasher = null;
        for (int seed = 0; seed < tryTimes; seed++) {
            hasher = checkSeed(objects, seed);
            if (hasher != null) {
                break;
            }
        }
        return hasher;
    }

    public static Hasher32 tryCreate(List<byte[]> values) {
        return tryCreate(values, 10000);
    }

    private static Hasher32 checkSeed(List<byte[]> values, int seed) {
        IntHashMap hashCodes = new IntHashMap();
        Hasher32 hasher = new Hasher32(seed);
        for (byte[] object : values) {
            int hashCode = hasher.hashUnsafeWords(object);
            byte[] preObject = (byte[]) hashCodes.put(hashCode, object);
            if (preObject != null) {
                return null;
            }
        }
        return hasher;
    }
}

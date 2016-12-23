package io.indexr.segment.rc;

import java.util.BitSet;

public class RCHelper {
    public static BitSet not(BitSet bitSets) {
        bitSets.flip(0, bitSets.size());
        return bitSets;
    }
}

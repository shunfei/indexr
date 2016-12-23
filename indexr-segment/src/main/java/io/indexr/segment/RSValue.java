package io.indexr.segment;

/**
 * Raw set values.
 */
public class RSValue {
    public static final byte Unknown = 0;   // the pack was not checked yet (i.e. RS_UNKNOWN & RS_ALL = RS_ALL)
    public static final byte Some = 1;      // the pack is suspected (but may be empty or full) (i.e. RS_SOME & RS_ALL = RS_SOME)
    public static final byte None = 2;      // the pack is empty
    public static final byte All = 3;       // the pack is full

    /**
     * Get the opposite rsvalue.
     */
    public static byte not(byte v) {
        if (v == All) {
            return None;
        } else if (v == None) {
            return All;
        }
        return v;
    }

    public static byte or(byte v1, byte v2) {
        if (v1 == All || v2 == All)
            return All;
        if (v1 == Some || v2 == Some)
            return Some;
        return None;
    }

    public static byte and(byte v1, byte v2) {
        if (v1 == All && v2 == All)
            return All;
        if (v1 == None || v2 == None)
            return None;
        return Some;
    }
}

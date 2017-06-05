package io.indexr.vlt.codec.pack;

public class PackingUtil {
    /**
     * How many groups will be split if packed by valCount.
     */
    public static int packingGroup(int valCount) {
        return (valCount + 7) >>> 3;
    }

    /**
     * The number of bytes after packed those item with the bitWidth.
     */
    public static int packingOutputBytes(int valCount, int bitWidth) {
        if (valCount == 0 || bitWidth == 0) {
            return 0;
        } else {
            // We pack 8 values a time.
            return ((valCount + 7) >>> 3) * bitWidth;
        }
    }

    private static final IntPackerFactory INT_PACKER_FACTORY = IntPackingLE.factory;
    private static final LongPackerFactory LONG_PACKER_FACTORY = LongPackingLE.factory;

    public static IntPacker intPacker(int width) {
        return INT_PACKER_FACTORY.newPacker(width);
    }

    public static LongPacker longPacker(int width) {
        return LONG_PACKER_FACTORY.newPacker(width);
    }
}

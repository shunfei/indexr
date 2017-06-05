package io.indexr.vlt.codec.pack;

import io.indexr.vlt.codec.UnsafeUtil;

public abstract class LongPacker {
    private int bitWidth;

    LongPacker(int bitWidth) {
        this.bitWidth = bitWidth;
    }

    /**
     * @return the width in bits used for encoding, also how many bytes are packed/unpacked at a time by pack8Values/unpack8Values
     */
    public int bitWidth() {
        return bitWidth;
    }

    /**
     * pack 8 values from input into bitWidth bytes in output.
     * nextPosition: input = (inAddr + 8 * 4), output = (outAddr + bitWidth)
     */
    public abstract void pack8Values(Object inBase, long inAddr, Object outBase, long outAddr);

    /**
     * unpack bitWidth bytes from input into 8 values in output.
     * nextPosition: input = (inAddr + bitWidth), output = (outAddr + 8 * 4)
     */
    public abstract void unpack8Values(Object inBase, long inAddr, Object outBase, long outAddr);

    public abstract void pack8Values(final long[] in, final int inPos, final byte[] out, final int outPos);

    public abstract void unpack8Values(final byte[] in, final int inPos, final long[] out, final int outPos);

    public int packUnsafe(int valCount, Object inBase, long inAddr, Object outBase, long outAddr) {
        int step = valCount >>> 3;
        for (int s = 0; s < step; s++) {
            //int valIndex = s << 3;
            pack8Values(inBase, inAddr + ((s << 3) << 3), outBase, outAddr + (s * bitWidth));
        }

        int tail = valCount & 0x07;
        if (tail != 0) {
            int tailStart = (valCount >>> 3) << 3;
            long[] tailData = new long[8];
            for (int i = 0; i < valCount - tailStart; i++) {
                tailData[i] = UnsafeUtil.getLong(inBase, inAddr + ((tailStart + i) << 3));
            }
            pack8Values(tailData, UnsafeUtil.LONG_ARRAY_OFFSET, outBase, outAddr + (valCount >>> 3) * bitWidth);
        }
        return PackingUtil.packingOutputBytes(valCount, bitWidth);
    }

    public int unpackUnsafe(int valCount, Object inBase, long inAddr, Object outBase, long outAddr) {
        int step = valCount >>> 3;
        for (int s = 0; s < step; s++) {
            //int valIndex = s << 3;
            unpack8Values(inBase, inAddr + (s * bitWidth), outBase, outAddr + ((s << 3) << 3));
        }

        int tail = valCount & 0x07;
        if (tail != 0) {
            int tailStart = (valCount >>> 3) << 3;
            long[] tailData = new long[8];
            unpack8Values(inBase, inAddr + (valCount >>> 3) * bitWidth, tailData, UnsafeUtil.LONG_ARRAY_OFFSET);
            for (int i = 0; i < valCount - tailStart; i++) {
                UnsafeUtil.setLong(outBase, outAddr + ((tailStart + i) << 3), tailData[i]);
            }
        }
        return PackingUtil.packingOutputBytes(valCount, bitWidth);
    }

    public final int pack(int valCount,
                          long[] in, int inOffset,
                          byte[] out, int outOffset) {
        return packUnsafe(valCount,
                in, UnsafeUtil.LONG_ARRAY_OFFSET + (inOffset << 3),
                out, UnsafeUtil.BYTE_ARRAY_OFFSET + outOffset);
    }

    public int unpack(int valCount,
                      byte[] in, int inOffset,
                      long[] out, int outOffset) {
        return unpackUnsafe(valCount,
                in, UnsafeUtil.BYTE_ARRAY_OFFSET + inOffset,
                out, UnsafeUtil.LONG_ARRAY_OFFSET + (outOffset << 3));
    }

    public final int pack(long[] in, byte[] out) {
        return pack(in.length, in, 0, out, 0);
    }

    public final int unpack(byte[] in, long[] out) {
        return unpack(out.length, in, 0, out, 0);
    }

    public final void _pack(long[] in, byte[] out) {
        int valCount = in.length;

        int step = valCount >>> 3;
        for (int s = 0; s < step; s++) {
            pack8Values(in, s << 3, out, s * bitWidth);
        }

        int tail = valCount & 0x07;
        if (tail != 0) {
            int tailStart = (valCount >>> 3) << 3;
            long[] tailData = new long[8];
            for (int i = 0; i < valCount - tailStart; i++) {
                tailData[i] = in[tailStart + i];
            }
            pack8Values(tailData, 0, out, (valCount >>> 3) * bitWidth);
        }
    }

    public final void _unpack(byte[] in, long[] out) {
        int valCount = out.length;

        int step = valCount >>> 3;
        for (int s = 0; s < step; s++) {
            unpack8Values(in, s * bitWidth, out, s << 3);
        }

        int tail = valCount & 0x07;
        if (tail != 0) {
            int tailStart = (valCount >>> 3) << 3;
            long[] tailData = new long[8];
            unpack8Values(in, (valCount >>> 3) * bitWidth, tailData, 0);
            for (int i = 0; i < valCount - tailStart; i++) {
                out[tailStart + i] = tailData[i];
            }
        }
    }
}

package io.indexr.vlt.codec.pack;

import io.indexr.vlt.codec.UnsafeUtil;

public abstract class IntPacker {
    private int bitWidth;

    IntPacker(int bitWidth) {
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

    public abstract void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos);

    public abstract void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos);

    public int packUnsafe(int valCount, Object inBase, long inAddr, Object outBase, long outAddr) {
        int step = valCount >>> 3;
        for (int s = 0; s < step; s++) {
            //int valIndex = s << 3;
            pack8Values(inBase, inAddr + ((s << 3) << 2), outBase, outAddr + (s * bitWidth));
        }

        int tail = valCount & 0x07;
        if (tail != 0) {
            int tailStart = (valCount >>> 3) << 3;
            int[] tailData = new int[8];
            for (int i = 0; i < valCount - tailStart; i++) {
                tailData[i] = UnsafeUtil.getInt(inBase, inAddr + ((tailStart + i) << 2));
            }
            pack8Values(tailData, UnsafeUtil.INT_ARRAY_OFFSET, outBase, outAddr + (valCount >>> 3) * bitWidth);
        }
        return PackingUtil.packingOutputBytes(valCount, bitWidth);
    }

    public int unpackUnsafe(int valCount, Object inBase, long inAddr, Object outBase, long outAddr) {
        int step = valCount >>> 3;
        for (int s = 0; s < step; s++) {
            unpack8Values(inBase, inAddr + (s * bitWidth), outBase, outAddr + ((s << 3) << 2));
        }

        int tail = valCount & 0x07;
        if (tail != 0) {
            int tailStart = (valCount >>> 3) << 3;
            int[] tailData = new int[8];
            unpack8Values(inBase, inAddr + (valCount >>> 3) * bitWidth, tailData, UnsafeUtil.INT_ARRAY_OFFSET);
            for (int i = 0; i < valCount - tailStart; i++) {
                UnsafeUtil.setInt(outBase, outAddr + ((tailStart + i) << 2), tailData[i]);
            }
        }
        return PackingUtil.packingOutputBytes(valCount, bitWidth);
    }

    public final int pack(int valCount,
                          int[] in, int inOffset,
                          byte[] out, int outOffset) {
        return packUnsafe(valCount,
                in, UnsafeUtil.INT_ARRAY_OFFSET + (inOffset << 2),
                out, UnsafeUtil.BYTE_ARRAY_OFFSET + outOffset);
    }

    public final int unpack(int valCount,
                            byte[] in, int inOffset,
                            int[] out, int outOffset) {
        return unpackUnsafe(valCount,
                in, UnsafeUtil.BYTE_ARRAY_OFFSET + inOffset,
                out, UnsafeUtil.INT_ARRAY_OFFSET + (outOffset << 2));
    }

    public final int pack(int[] in, byte[] out) {
        return pack(in.length, in, 0, out, 0);
    }

    public final int unpack(byte[] in, int[] out) {
        return unpack(out.length, in, 0, out, 0);
    }

    public final void _pack(int[] in, byte[] out) {
        int valCount = in.length;

        int step = valCount >>> 3;
        for (int s = 0; s < step; s++) {
            //int valIndex = s << 3;
            pack8Values(in, s << 3, out, s * bitWidth);
        }

        int tail = valCount & 0x07;
        if (tail != 0) {
            int tailStart = (valCount >>> 3) << 3;
            int[] tailData = new int[8];
            for (int i = 0; i < valCount - tailStart; i++) {
                tailData[i] = in[tailStart + i];
            }
            pack8Values(tailData, 0, out, (valCount >>> 3) * bitWidth);
        }

    }

    public final void _unpack(byte[] in, int[] out) {
        int valCount = out.length;

        int step = valCount >>> 3;
        for (int s = 0; s < step; s++) {
            //int valIndex = s << 3;
            unpack8Values(in, s * bitWidth, out, s << 3);
        }

        int tail = valCount & 0x07;
        if (tail != 0) {
            int tailStart = (valCount >>> 3) << 3;
            int[] tailData = new int[8];
            unpack8Values(in, (valCount >>> 3) * bitWidth, tailData, 0);
            for (int i = 0; i < valCount - tailStart; i++) {
                out[tailStart + i] = tailData[i];
            }
        }
    }
}
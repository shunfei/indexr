package io.indexr.vlt.codec;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class VarInteger {
    public static final int NZero = 0;
    public static final int NOne = 1;
    public static final int NTwo = 2;
    public static final int NThree = 3;
    public static final int NFour = 4;

    public static int bitWidth(long max) {
        return 64 - Long.numberOfLeadingZeros(max);
    }

    public static int bitWidth(int max) {
        return 32 - Integer.numberOfLeadingZeros(max);
    }

    public static int bitWidthToByteWidth(int bitWidth) {
        return (bitWidth + 7) >>> 3;
    }

    public static void writeFixedByteWidthInt(int byteWidth, int value, ByteBuffer dest) {
        assert dest.order() == ByteOrder.LITTLE_ENDIAN;

        switch (byteWidth) {
            case NZero:
                break;
            case NOne:
                dest.put((byte) value);
                break;
            case NTwo:
                dest.putShort((short) value);
                break;
            case NThree:
                dest.put((byte) value);
                dest.putShort((short) (value >>> 8));
                break;
            case NFour:
                dest.putInt(value);
                break;
            default:
                throw new RuntimeException("illegal byte width: " + byteWidth);
        }
    }

    public static int readFixedByteWidthInt(int byteWidth, ByteBuffer src) {
        assert src.order() == ByteOrder.LITTLE_ENDIAN;

        switch (byteWidth) {
            case NZero:
                return 0;
            case NOne:
                return src.get() & 0xFF;
            case NTwo:
                return src.getShort() & 0xFFFF;
            case NThree:
                return src.get() & 0xFF | ((src.getShort() & 0xFFFF) << 8);
            case NFour:
                return src.getInt();
            default:
                throw new RuntimeException("illegal byte width: " + byteWidth);
        }
    }

    public static int readUnsignedVarInt(ByteBuffer in) {
        int value = 0;
        int i = 0;
        int b;
        while (((b = in.get() & 0xFF) & 0x80) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
        }
        return value | (b << i);
    }

    public static void writeUnsignedVarInt(int value, ByteBuffer dest) {
        while ((value & 0xFFFFFF80) != 0L) {
            dest.put((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        dest.put((byte) (value & 0x7F));
    }

    public static long readUnsignedVarLong(ByteBuffer in) {
        long value = 0;
        int i = 0;
        long b;
        while (((b = in.get() & 0xFF) & 0x80) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
        }
        return value | (b << i);
    }

    public static void writeUnsignedVarLong(long value, ByteBuffer out) {
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
            out.put((byte) ((int) ((value & 0x7F) | 0x80)));
            value >>>= 7;
        }
        out.put((byte) (value & 0x7F));
    }

    public static long readZigZagVarLong(ByteBuffer in) {
        long raw = readUnsignedVarLong(in);
        long temp = (((raw << 63) >> 63) ^ raw) >> 1;
        return temp ^ (raw & (1L << 63));
    }

    public static void writeZigZagVarLong(long longValue, ByteBuffer out) {
        writeUnsignedVarLong((longValue << 1) ^ (longValue >> 63), out);
    }

    public static void writeZigZagVarInt(int intValue, ByteBuffer out) {
        writeUnsignedVarInt((intValue << 1) ^ (intValue >> 31), out);
    }

    /**
     * uses a trick mentioned in https://developers.google.com/protocol-buffers/docs/encoding to read zigZag encoded data
     */
    public static int readZigZagVarInt(ByteBuffer in) {
        int raw = readUnsignedVarInt(in);
        int temp = (((raw << 31) >> 31) ^ raw) >> 1;
        return temp ^ (raw & (1 << 31));
    }

    public static void main(String[] args) {
        System.out.println(Integer.numberOfLeadingZeros(-100));
    }
}

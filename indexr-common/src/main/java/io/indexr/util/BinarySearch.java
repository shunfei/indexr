package io.indexr.util;

import org.apache.spark.unsafe.Platform;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.indexr.data.OffheapBytes;
import io.indexr.data.StringsStruct;
import io.indexr.data.StringsStructOnByteBufferReader;
import io.indexr.io.ByteBufferReader;

/**
 * Those methods return index of the search key, if it is contained in the array
 * within the specified range;
 * otherwise, <tt>(-(<i>insertion point</i>) - 1)</tt>.  The
 * <i>insertion point</i> is defined as the point at which the
 * key would be inserted into the array: the index of the first
 * element in the range greater than the key,
 * or <tt>toIndex</tt> if all
 * elements in the range are less than the specified key.  Note
 * that this guarantees that the return value will be &gt;= 0 if
 * and only if the key is found.
 */
public class BinarySearch {

    public static int binarySearchInts(long addr, int count, int key) {
        int from = 0;
        int to = count;

        --to;

        while (from <= to) {
            int mid = from + to >>> 1;
            int midVal = MemoryUtil.getInt(addr + (mid << 2));
            if (midVal < key) {
                from = mid + 1;
            } else if (midVal > key) {
                to = mid - 1;
            } else {
                return mid;
            }
        }

        return -(from + 1);
    }

    /**
     * We start search from pos 0.
     */
    public static int binarySearchInts(ByteBuffer buffer, int count, int key) {
        int from = 0;
        int to = count;

        --to;

        while (from <= to) {
            int mid = from + to >>> 1;
            int midVal = buffer.getInt(mid << 2);
            if (midVal < key) {
                from = mid + 1;
            } else if (midVal > key) {
                to = mid - 1;
            } else {
                return mid;
            }
        }

        return -(from + 1);
    }

    public static int binarySearchInts(ByteBufferReader reader, int count, int key) throws IOException {
        int from = 0;
        int to = count;

        --to;

        byte[] valBuffer = new byte[4];

        while (from <= to) {
            int mid = from + to >>> 1;
            reader.read(mid << 2, valBuffer, 0, 4);
            int midVal = Platform.getInt(valBuffer, Platform.BYTE_ARRAY_OFFSET);
            if (midVal < key) {
                from = mid + 1;
            } else if (midVal > key) {
                to = mid - 1;
            } else {
                return mid;
            }
        }

        return -(from + 1);
    }

    public static int binarySearchLongs(long addr, int count, long key) {
        int from = 0;
        int to = count;

        --to;

        while (from <= to) {
            int mid = from + to >>> 1;
            long midVal = MemoryUtil.getLong(addr + (mid << 3));
            if (midVal < key) {
                from = mid + 1;
            } else if (midVal > key) {
                to = mid - 1;
            } else {
                return mid;
            }
        }

        return -(from + 1);
    }

    public static int binarySearchLongs(ByteBuffer buffer, int count, long key) {
        int from = 0;
        int to = count;

        --to;

        while (from <= to) {
            int mid = from + to >>> 1;
            long midVal = buffer.getLong(mid << 3);
            if (midVal < key) {
                from = mid + 1;
            } else if (midVal > key) {
                to = mid - 1;
            } else {
                return mid;
            }
        }

        return -(from + 1);
    }

    public static int binarySearchLongs(ByteBufferReader reader, int count, long key) throws IOException {
        int from = 0;
        int to = count;

        --to;

        byte[] valBuffer = new byte[8];

        while (from <= to) {
            int mid = from + to >>> 1;
            reader.read(mid << 3, valBuffer, 0, 8);
            long midVal = Platform.getLong(valBuffer, Platform.BYTE_ARRAY_OFFSET);
            if (midVal < key) {
                from = mid + 1;
            } else if (midVal > key) {
                to = mid - 1;
            } else {
                return mid;
            }
        }

        return -(from + 1);
    }

    public static int binarySearchFloats(long addr, int count, float key) {
        int from = 0;
        int to = count;

        --to;

        while (from <= to) {
            int mid = from + to >>> 1;
            float midVal = MemoryUtil.getFloat(addr + (mid << 2));
            if (midVal < key) {
                from = mid + 1;
            } else if (midVal > key) {
                to = mid - 1;
            } else {
                return mid;
            }
        }

        return -(from + 1);
    }


    public static int binarySearchFloats(ByteBuffer buffer, int count, float key) {
        int from = 0;
        int to = count;

        --to;

        while (from <= to) {
            int mid = from + to >>> 1;
            float midVal = buffer.getFloat(mid << 2);
            if (midVal < key) {
                from = mid + 1;
            } else if (midVal > key) {
                to = mid - 1;
            } else {
                return mid;
            }
        }

        return -(from + 1);
    }

    public static int binarySearchFloats(ByteBufferReader reader, int count, float key) throws IOException {
        int from = 0;
        int to = count;

        --to;

        byte[] valBuffer = new byte[4];

        while (from <= to) {
            int mid = from + to >>> 1;
            reader.read(mid << 2, valBuffer, 0, 4);
            float midVal = Platform.getFloat(valBuffer, Platform.BYTE_ARRAY_OFFSET);
            if (midVal < key) {
                from = mid + 1;
            } else if (midVal > key) {
                to = mid - 1;
            } else {
                return mid;
            }
        }

        return -(from + 1);
    }

    public static int binarySearchDoubles(long addr, int count, double key) {
        int from = 0;
        int to = count;

        --to;

        while (from <= to) {
            int mid = from + to >>> 1;
            double midVal = MemoryUtil.getDouble(addr + (mid << 3));
            if (midVal < key) {
                from = mid + 1;
            } else if (midVal > key) {
                to = mid - 1;
            } else {
                return mid;
            }
        }

        return -(from + 1);
    }

    public static int binarySearchDoubles(ByteBuffer buffer, int count, double key) {
        int from = 0;
        int to = count;

        --to;

        while (from <= to) {
            int mid = from + to >>> 1;
            double midVal = buffer.getDouble(mid << 3);
            if (midVal < key) {
                from = mid + 1;
            } else if (midVal > key) {
                to = mid - 1;
            } else {
                return mid;
            }
        }

        return -(from + 1);
    }

    public static int binarySearchDoubles(ByteBufferReader reader, int count, double key) throws IOException {
        int from = 0;
        int to = count;

        --to;

        byte[] valBuffer = new byte[8];

        while (from <= to) {
            int mid = from + to >>> 1;
            reader.read(mid << 3, valBuffer, 0, 8);
            double midVal = Platform.getDouble(valBuffer, Platform.BYTE_ARRAY_OFFSET);
            if (midVal < key) {
                from = mid + 1;
            } else if (midVal > key) {
                to = mid - 1;
            } else {
                return mid;
            }
        }

        return -(from + 1);
    }

    public static int binarySearchStrings(StringsStruct struct, Object keyBase, long keyAddr, int keyLen) {
        int from = 0;
        int to = struct.valCount();

        --to;
        OffheapBytes minVal = new OffheapBytes();
        int cmp;
        while (from <= to) {
            int mid = from + to >>> 1;
            struct.getString(mid, minVal);
            cmp = BytesUtil.compareBytes(null, minVal.addr(), minVal.len(), keyBase, keyAddr, keyLen);
            if (cmp < 0) {
                from = mid + 1;
            } else if (cmp > 0) {
                to = mid - 1;
            } else {
                return mid;
            }
        }

        return -(from + 1);
    }

    public static int binarySearchStrings(StringsStructOnByteBufferReader reader,
                                          Object keyBase, long keyAddr, int keyLen,
                                          byte[] valBuffer) throws IOException {
        int from = 0;
        int to = reader.valCount();

        --to;
        int cmp;
        while (from <= to) {
            int mid = from + to >>> 1;
            int valLen = reader.getString(mid, valBuffer);
            cmp = BytesUtil.compareBytes(valBuffer, Platform.BYTE_ARRAY_OFFSET, valLen, keyBase, keyAddr, keyLen);
            if (cmp < 0) {
                from = mid + 1;
            } else if (cmp > 0) {
                to = mid - 1;
            } else {
                return mid;
            }
        }

        return -(from + 1);
    }
}

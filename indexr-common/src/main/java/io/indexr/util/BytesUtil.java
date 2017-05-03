package io.indexr.util;

import org.apache.spark.unsafe.Platform;

import io.indexr.data.OffheapBytes;
import io.indexr.data.StringsStruct;

public class BytesUtil {

    public static int compareBytes(long addr1, int len1, long addr2, int len2) {
        int len = Math.min(len1, len2);
        int res;
        for (int i = 0; i < len; i++) {
            res = Integer.compare(
                    MemoryUtil.getByte(addr1 + i) & 0xFF,
                    MemoryUtil.getByte(addr2 + i) & 0xFF);
            if (res != 0) {
                return res;
            }
        }
        return len1 - len2;
    }

    public static int compareBytes(Object base1, long addr1, int len1, Object base2, long addr2, int len2) {
        int len = Math.min(len1, len2);
        int res;
        for (int i = 0; i < len; i++) {
            res = Integer.compare(
                    Platform.getByte(base1, addr1 + i) & 0xFF,
                    Platform.getByte(base2, addr2 + i) & 0xFF);
            if (res != 0) {
                return res;
            }
        }
        return len1 - len2;
        // Code below is buggy.
        //int len = Math.min(len1, len2);
        //int res;
        //
        //long word1, word2;
        //int wordTotalLen = len & 0xFFFF_FFF8;
        //for (int i = 0; i < wordTotalLen; i += 8) {
        //    word1 = Platform.getLong(base1, addr1 + i);
        //    word2 = Platform.getLong(base2, addr2 + i);
        //    res = Long.compareUnsigned(word1, word2);
        //    if (res != 0) {
        //        return res;
        //    }
        //}
        //
        //int tailBytes = len & 0x07;
        //if (tailBytes != 0) {
        //    long tail1 = 0;
        //    long tail2 = 0;
        //    assert len - wordTotalLen == tailBytes;
        //    for (int i = wordTotalLen; i < len; i++) {
        //        tail1 <<= 8;
        //        tail1 |= Platform.getByte(base1, addr1 + i) & 0xFFL;
        //        tail2 <<= 8;
        //        tail2 |= Platform.getByte(base2, addr2 + i) & 0xFFL;
        //    }
        //    tail1 <<= ((8 - tailBytes) << 3);
        //    tail2 <<= ((8 - tailBytes) << 3);
        //    assert tail1 >= 0 && tail2 >= 0;
        //    res = Long.compareUnsigned(tail1, tail2);
        //    if (res != 0) {
        //        return res;
        //    }
        //}
        //
        //return len1 - len2;
    }

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

    public static int binarySearchStrings(StringsStruct a, Object keyBase, long keyAddr, int keyLen) {
        int from = 0;
        int to = a.valCount();

        --to;
        OffheapBytes minVal = new OffheapBytes();
        int cmp;
        while (from <= to) {
            int mid = from + to >>> 1;
            a.getString(mid, minVal);
            cmp = compareBytes(null, minVal.addr(), minVal.len(), keyBase, keyAddr, keyLen);
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

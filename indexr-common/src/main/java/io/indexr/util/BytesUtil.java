package io.indexr.util;

import org.apache.spark.unsafe.Platform;

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

}

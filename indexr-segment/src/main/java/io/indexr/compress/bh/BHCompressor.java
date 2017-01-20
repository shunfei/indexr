package io.indexr.compress.bh;

import com.sun.jna.NativeLibrary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.indexr.io.ByteSlice;
import io.indexr.util.MemoryUtil;

/**
 * Wrapper of brighthouse compression.
 * This class is closely linked to cpp code and native data structure, don't touch it unless you know what you are doing.
 *
 * Note that all values here are in unsigned form, including max_val.
 */
public class BHCompressor {
    private static final Logger logger = LoggerFactory.getLogger(BHCompressor.class);

    static {
        NativeLibrary.addSearchPath("bhcompress", "lib");
        NativeLibrary.addSearchPath("bhcompress", "/usr/local/lib");
    }

    private static void logError(Throwable t) {
        logger.error("Call compress method failed. Make sure you have setup native compression lib correctly.", t);
    }

    @FunctionalInterface
    private static interface NumberCompressor {
        long compress(long dataAddr, int itemSize, long maxVal);
    }

    @FunctionalInterface
    private static interface NumberDecompressor {
        long decompress(long cmpDataAddr, int itemSize);
    }

    private static ByteSlice compressNumber(ByteSlice data, int itemSize, int byteSizeShift, long maxVal, NumberCompressor compressor) {
        assert data.size() == (itemSize << byteSizeShift);
        if (itemSize == 0 || maxVal == 0) {
            return ByteSlice.empty();
        }
        long native_p = compressor.compress(data.address(), itemSize, maxVal);
        int err = MemoryUtil.getByte(native_p);
        if (err != BhcompressLibrary.CprsErr_J.CPRS_SUCCESS_J) {
            MemoryUtil.free(native_p);
            throw new RuntimeException(String.format("compress number err[%d]", err));
        }
        int cmp_data_size = MemoryUtil.getInt(native_p + 9);
        return ByteSlice.fromAddress(native_p, 1 + 8 + 4 + cmp_data_size);
    }

    private static ByteSlice decompressNumber(ByteSlice cmpData, int itemSize, int byteSizeShift, long maxVal, NumberDecompressor decompressor) {
        if (itemSize == 0) {
            return ByteSlice.empty();
        }
        if (maxVal == 0) {
            return ByteSlice.allocateDirect(itemSize << byteSizeShift);
        }
        long native_p = decompressor.decompress(cmpData.address(), itemSize);
        if (native_p == 0) {
            throw new RuntimeException("decompress number err, check the std output for error code");
        }
        return ByteSlice.fromAddress(native_p, itemSize << byteSizeShift);
    }

    public static ByteSlice compressByte(ByteSlice data, int itemSize, long maxVal) {
        try {
            return compressNumber(data, itemSize, 0, maxVal, BhcompressLibrary.INSTANCE::JavaCompress_Number_Byte_NP);
        } catch (Throwable t) {
            logError(t);
            throw t;
        }
    }

    public static ByteSlice compressShort(ByteSlice data, int itemSize, long maxVal) {
        try {
            return compressNumber(data, itemSize, 1, maxVal, BhcompressLibrary.INSTANCE::JavaCompress_Number_Short_NP);
        } catch (Throwable t) {
            logError(t);
            throw t;
        }
    }

    public static ByteSlice compressInt(ByteSlice data, int itemSize, long maxVal) {
        try {
            return compressNumber(data, itemSize, 2, maxVal, BhcompressLibrary.INSTANCE::JavaCompress_Number_Int_NP);
        } catch (Throwable t) {
            logError(t);
            throw t;
        }
    }

    public static ByteSlice compressLong(ByteSlice data, int itemSize, long maxVal) {
        try {
            return compressNumber(data, itemSize, 3, maxVal, BhcompressLibrary.INSTANCE::JavaCompress_Number_Long_NP);
        } catch (Throwable t) {
            logError(t);
            throw t;
        }
    }

    public static ByteSlice decompressByte(ByteSlice cmpData, int itemSize, long maxVal) {
        try {
            return decompressNumber(cmpData, itemSize, 0, maxVal, BhcompressLibrary.INSTANCE::JavaDecompress_Number_Byte_NP);
        } catch (Throwable t) {
            logError(t);
            throw t;
        }
    }

    public static ByteSlice decompressShort(ByteSlice cmpData, int itemSize, long maxVal) {
        try {
            return decompressNumber(cmpData, itemSize, 1, maxVal, BhcompressLibrary.INSTANCE::JavaDecompress_Number_Short_NP);
        } catch (Throwable t) {
            logError(t);
            throw t;
        }
    }

    public static ByteSlice decompressInt(ByteSlice cmpData, int itemSize, long maxVal) {
        try {
            return decompressNumber(cmpData, itemSize, 2, maxVal, BhcompressLibrary.INSTANCE::JavaDecompress_Number_Int_NP);
        } catch (Throwable t) {
            logError(t);
            throw t;
        }
    }

    public static ByteSlice decompressLong(ByteSlice cmpData, int itemSize, long maxVal) {
        try {
            return decompressNumber(cmpData, itemSize, 3, maxVal, BhcompressLibrary.INSTANCE::JavaDecompress_Number_Long_NP);
        } catch (Throwable t) {
            logError(t);
            throw t;
        }
    }

    public static ByteSlice compressIndexedStr(ByteSlice data, int itemSize) {
        try {
            return doCompressIndexedStr(data, itemSize);
        } catch (Throwable t) {
            logError(t);
            throw t;
        }
    }

    private static ByteSlice doCompressIndexedStr(ByteSlice data, int itemSize) {
        if (itemSize == 0) {
            return ByteSlice.empty();
        }
        long native_p = BhcompressLibrary.INSTANCE.JavaCompress_IndexedString_NP(data.address(), itemSize);
        int err = MemoryUtil.getByte(native_p);
        if (err != BhcompressLibrary.CprsErr_J.CPRS_SUCCESS_J) {
            MemoryUtil.free(native_p);
            throw new RuntimeException(String.format("compress string err[%d]", err));
        }
        int max_len = MemoryUtil.getShort(native_p + 1) & 0xFFFF;
        if (max_len == 0) {
            return ByteSlice.fromAddress(native_p, 1 + 2);
        }
        int index_cmp_data_size = MemoryUtil.getInt(native_p + 3);
        int str_cmp_data_size = MemoryUtil.getInt(native_p + 7);
        return ByteSlice.fromAddress(native_p, 1 + 2 + 4 + 4 + index_cmp_data_size + str_cmp_data_size);
    }

    public static ByteSlice decompressIndexedStr(ByteSlice cmpData, int itemSize) {
        try {
            return doDecompressIndexedStr(cmpData, itemSize);
        } catch (Throwable t) {
            logError(t);
            throw t;
        }
    }

    private static ByteSlice doDecompressIndexedStr(ByteSlice cmpData, int itemSize) {
        if (itemSize == 0) {
            return ByteSlice.empty();
        }
        long native_p = BhcompressLibrary.INSTANCE.JavaDecompress_IndexedString_NP(cmpData.address(), itemSize);
        if (native_p == 0) {
            throw new RuntimeException("decompress string err, check the std output for error code");
        }
        int str_total_len = MemoryUtil.getInt(native_p);
        return ByteSlice.fromAddress(native_p, 4 + (itemSize << 3) + str_total_len);
    }

    public static ByteSlice compressIndexedStr_v1(ByteSlice data, int itemSize) {
        try {
            return doCompressIndexedStr_v1(data, itemSize);
        } catch (Throwable t) {
            logError(t);
            throw t;
        }
    }

    private static ByteSlice doCompressIndexedStr_v1(ByteSlice data, int itemSize) {
        if (itemSize == 0) {
            return ByteSlice.empty();
        }
        if (data.getInt(itemSize) == 0) {
            return ByteSlice.empty();
        }
        long native_p = BhcompressLibrary.INSTANCE.JavaCompress_IndexedString_NP_v1(data.address(), itemSize);
        int err = MemoryUtil.getByte(native_p);
        if (err != BhcompressLibrary.CprsErr_J.CPRS_SUCCESS_J) {
            MemoryUtil.free(native_p);
            throw new RuntimeException(String.format("compress string err[%d]", err));
        }
        int index_cmp_data_size = MemoryUtil.getInt(native_p + 1);
        int str_cmp_data_size = MemoryUtil.getInt(native_p + 5);
        return ByteSlice.fromAddress(native_p, 1 + 4 + 4 + index_cmp_data_size + str_cmp_data_size);
    }

    public static ByteSlice decompressIndexedStr_v1(ByteSlice cmpData, int itemSize) {
        try {
            return doDecompressIndexedStr_v1(cmpData, itemSize);
        } catch (Throwable t) {
            logError(t);
            throw t;
        }
    }

    private static ByteSlice doDecompressIndexedStr_v1(ByteSlice cmpData, int itemSize) {
        if (itemSize == 0) {
            return ByteSlice.empty();
        }
        if (cmpData.size() == 0) {
            // itemSize > 0 and cmpData == 0 means all strings are empty.
            ByteSlice data = ByteSlice.allocateDirect((itemSize + 1) << 2);
            data.clear();
            return data;
        }
        long native_p = BhcompressLibrary.INSTANCE.JavaDecompress_IndexedString_NP_v1(cmpData.address(), itemSize);
        if (native_p == 0) {
            throw new RuntimeException("decompress string err, check the std output for error code");
        }
        int str_total_len = MemoryUtil.getInt(native_p + (itemSize << 2));
        return ByteSlice.fromAddress(native_p, ((itemSize + 1) << 2) + str_total_len);
    }

    /**
     * Test call of jna function.
     */
    public static void test(long v) {
        BhcompressLibrary.INSTANCE.Test(v);
    }

    public static void main(String[] args) {
        BHCompressor.test(10);
    }
}

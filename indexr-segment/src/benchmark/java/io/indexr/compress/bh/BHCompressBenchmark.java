package io.indexr.compress.bh;

import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;
import com.sun.jna.Pointer;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Warmup;

import io.indexr.io.ByteSlice;


@Fork(2)
@Measurement(iterations = 5)
@Warmup(iterations = 1)
public class BHCompressBenchmark {
    static {
        NativeLibrary.addSearchPath("bhcompress", "lib");
    }

    @Benchmark
    public void test_byte() {
        int item_size = 50;
        byte[] data = new byte[item_size];
        for (int i = 0; i < item_size; i++) {
            data[i] = (byte) (i * 2);
        }

        ByteSlice bb = ByteSlice.allocateDirect(item_size);
        long max_val = 0;
        for (int i = 0; i < item_size; i++) {
            byte v = data[i];
            bb.put(i, v);
            max_val = Math.max(max_val, v);
        }
        Pointer p = BhcompressLibrary.INSTANCE.JavaCompress_Number_Byte(bb.asPointer(), item_size, max_val);
        Pointer dp = BhcompressLibrary.INSTANCE.JavaDecompress_Number_Byte(p, item_size);
        Native.free(Pointer.nativeValue(p));
        Native.free(Pointer.nativeValue(dp));
    }

    @Benchmark
    public void test_byte_np() {
        int item_size = 50;
        byte[] data = new byte[item_size];
        for (int i = 0; i < item_size; i++) {
            data[i] = (byte) (i * 2);
        }

        ByteSlice bb = ByteSlice.allocateDirect(item_size);
        long max_val = 0;
        for (int i = 0; i < item_size; i++) {
            byte v = data[i];
            bb.put(i, v);
            max_val = Math.max(max_val, v);
        }
        long native_p = BhcompressLibrary.INSTANCE.JavaCompress_Number_Byte_NP(bb.address(), item_size, max_val);
        long native_dp = BhcompressLibrary.INSTANCE.JavaDecompress_Number_Byte_NP(native_p, item_size);
        Native.free(native_p);
        Native.free(native_dp);
    }

    @Benchmark
    public void test_short() {
        int item_size = 50;
        short[] data = new short[item_size];
        for (int i = 0; i < item_size; i++) {
            data[i] = (short) (i * 2);
        }

        ByteSlice bb = ByteSlice.allocateDirect(item_size * 2);
        long max_val = 0;
        for (int i = 0; i < item_size; i++) {
            short v = data[i];
            bb.putShort(i * 2, v);
            max_val = Math.max(max_val, v);
        }
        long p = BhcompressLibrary.INSTANCE.JavaCompress_Number_Short_NP(bb.address(), item_size, max_val);
        long dp = BhcompressLibrary.INSTANCE.JavaDecompress_Number_Short_NP(p, item_size);
        Native.free(p);
        Native.free(dp);
    }


    @Benchmark
    public void test_int() {
        int item_size = 50;
        int[] data = new int[item_size];
        for (int i = 0; i < item_size; i++) {
            data[i] = (int) (i * 2);
        }

        ByteSlice bb = ByteSlice.allocateDirect(item_size * 4);
        long max_val = 0;
        for (int i = 0; i < item_size; i++) {
            int v = data[i];
            bb.putInt(i * 4, v);
            max_val = Math.max(max_val, v);
        }
        long p = BhcompressLibrary.INSTANCE.JavaCompress_Number_Int_NP(bb.address(), item_size, max_val);
        long dp = BhcompressLibrary.INSTANCE.JavaDecompress_Number_Int_NP(p, item_size);
        Native.free(p);
        Native.free(dp);
    }

    @Benchmark
    public void test_long() {
        int item_size = 50;
        long[] data = new long[item_size];
        for (int i = 0; i < item_size; i++) {
            data[i] = (long) (i * 2);
        }

        ByteSlice bb = ByteSlice.allocateDirect(item_size * 8);
        long max_val = 0;
        for (int i = 0; i < item_size; i++) {
            long v = data[i];
            bb.putLong(i * 8, v);
            max_val = Math.max(max_val, v);
        }
        long p = BhcompressLibrary.INSTANCE.JavaCompress_Number_Long_NP(bb.address(), item_size, max_val);
        long dp = BhcompressLibrary.INSTANCE.JavaDecompress_Number_Long_NP(p, item_size);
        Native.free(p);
        Native.free(dp);
    }

}

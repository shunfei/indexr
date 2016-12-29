package io.indexr.compress.bh;

import com.google.common.base.Preconditions;

import com.sun.jna.NativeLibrary;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.spark.unsafe.types.UTF8String;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import io.indexr.io.ByteSlice;
import io.indexr.segment.pack.DataPack;

@State(Scope.Thread)
@Fork(2)
@Measurement(iterations = 5)
@Warmup(iterations = 1)
public class BHCompressBenchmark {
    static {
        NativeLibrary.addSearchPath("bhcompress", "lib");
    }

    @Param({"65536"})
    public int objCount;
    @Param({"1024", "15536"})
    public int cardinality;

    private byte maxByte = 0;
    private ByteSlice byteData;
    private ByteSlice byteCmpData;

    private short maxShort = 0;
    private ByteSlice shortData;
    private ByteSlice shortCmpData;

    private int maxInt = 0;
    private ByteSlice intData;
    private ByteSlice intCmpData;

    private long maxLong = 0;
    private ByteSlice longData;
    private ByteSlice longCmpData;

    private ByteSlice strData;
    private ByteSlice strCmpData;

    @Setup(Level.Trial)
    public void setup() {
        byteData = ByteSlice.allocateDirect(objCount);
        shortData = ByteSlice.allocateDirect(objCount << 1);
        intData = ByteSlice.allocateDirect(objCount << 2);
        longData = ByteSlice.allocateDirect(objCount << 3);

        List<UTF8String> strings = new ArrayList<>();

        Random random = new Random();
        for (int i = 0; i < objCount; i++) {
            byte bv = (byte) random.nextInt(Byte.MAX_VALUE);
            byteData.put(i, bv);
            maxByte = (byte) Math.max(bv, maxByte);

            short sv = (short) random.nextInt(Short.MAX_VALUE);
            shortData.putShort(i << 1, sv);
            maxShort = (short) Math.max(sv, maxShort);

            int iv = random.nextInt(cardinality);
            intData.putInt(i << 2, iv);
            maxInt = Math.max(iv, maxInt);

            long lv = ((long) random.nextInt(cardinality)) << (random.nextBoolean() ? 31 : 0);
            longData.putLong(i << 3, lv);
            maxLong = Math.max(lv, maxLong);

            String string = RandomStringUtils.random(random.nextInt(20));
            strings.add(UTF8String.fromString(string));
        }
        strData = _from_v1(strings);

        byteCmpData = BHCompressor.compressByte(byteData, objCount, maxByte);
        shortCmpData = BHCompressor.compressShort(shortData, objCount, maxShort);
        intCmpData = BHCompressor.compressInt(intData, objCount, maxInt);
        longCmpData = BHCompressor.compressLong(longData, objCount, maxLong);
        strCmpData = BHCompressor.compressIndexedStr_v1(strData, objCount);
    }

    private static ByteSlice _from_v1(List<UTF8String> strings) {
        int size = strings.size();
        Preconditions.checkArgument(size > 0 && size <= DataPack.MAX_COUNT);

        int strTotalLen = 0;
        for (UTF8String s : strings) {
            int byteCount = s.numBytes();
            strTotalLen += byteCount;
        }
        int indexLen = (size + 1) << 2;

        ByteSlice data = ByteSlice.allocateDirect(indexLen + strTotalLen);

        int offset = 0;
        for (int i = 0; i < size; i++) {
            UTF8String s = strings.get(i);
            byte[] bytes = s.getBytes();
            int byteLen = bytes.length;
            int start = offset;
            int end = start + byteLen;
            data.putInt(i << 2, start);
            data.putInt((i + 1) << 2, end);
            data.put(indexLen + start, bytes);

            offset = end;
        }

        return data;
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        byteData.free();
        byteCmpData.free();

        shortData.free();
        shortCmpData.free();

        intData.free();
        intCmpData.free();

        longData.free();
        longCmpData.free();
    }


    @Benchmark
    public void compressByte() {
        BHCompressor.compressByte(byteData, objCount, maxByte).free();
    }

    @Benchmark
    public void decompressByte() {
        BHCompressor.decompressByte(byteCmpData, objCount, maxByte).free();
    }

    @Benchmark
    public void compressShort() {
        BHCompressor.compressShort(shortData, objCount, maxShort).free();
    }

    @Benchmark
    public void decompressShort() {
        BHCompressor.decompressShort(shortCmpData, objCount, maxShort).free();
    }

    @Benchmark
    public void compressInt() {
        BHCompressor.compressInt(intData, objCount, maxInt).free();
    }

    @Benchmark
    public void decompressInt() {
        BHCompressor.decompressInt(intCmpData, objCount, maxInt).free();
    }

    @Benchmark
    public void compressLong() {
        BHCompressor.compressLong(longData, objCount, maxLong).free();
    }

    @Benchmark
    public void decompressLong() {
        BHCompressor.decompressLong(longCmpData, objCount, maxLong).free();
    }

    @Benchmark
    public void compressString() {
        BHCompressor.compressIndexedStr_v1(strData, objCount).free();
    }

    @Benchmark
    public void decompressString() {
        BHCompressor.decompressIndexedStr_v1(strCmpData, objCount).free();
    }

    public static void main(String[] args) {
        BHCompressBenchmark b = new BHCompressBenchmark();
        b.objCount = 65536;
        b.cardinality = 15536;
        b.setup();

        b.compressByte();
        b.compressShort();
        b.compressInt();
        b.compressLong();
        b.compressString();

        b.decompressByte();
        b.decompressShort();
        b.decompressInt();
        b.decompressLong();
        b.decompressString();

        b.tearDown();
    }
}

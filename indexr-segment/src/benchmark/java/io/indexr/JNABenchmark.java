package io.indexr;

import com.sun.jna.Native;
import com.sun.jna.Pointer;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Warmup;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import io.indexr.io.ByteSlice;
import io.indexr.util.ByteBufferUtil;
import io.indexr.util.MemoryUtil;

@Fork(2)
@Measurement(iterations = 5)
@Warmup(iterations = 1)
public class JNABenchmark {

    @Benchmark
    public void memory_malloc_jna() {
        long native_p = Native.malloc(100);
        ByteSlice buffer = ByteSlice.wrap(MemoryUtil.getByteBuffer(native_p, 100, true));

        for (int i = 0; i < 100; i++) {
            buffer.put(i, (byte) i);
        }

        //Native.free(native_p);
    }

    @Benchmark
    public void memory_malloc_jna2() {
        long native_p = Native.malloc(100);
        ByteSlice buffer = ByteSlice.wrap(MemoryUtil.getByteBuffer(native_p, 100, true));
        for (int i = 0; i < 100; i++) {
            buffer.put(i, (byte) i);
        }

        Pointer.createConstant(native_p);

        //Native.free(native_p);
    }

    @Benchmark
    public void memory_malloc_jna3() {
        long native_p = Native.malloc(100);
        ByteSlice buffer = ByteSlice.wrap(MemoryUtil.getByteBuffer(native_p, 100, true));
        for (int i = 0; i < 100; i++) {
            buffer.put(i, (byte) i);
        }

        buffer.asPointer();

        //Native.free(native_p);
    }

    @Benchmark
    public void memory_malloc_java() {
        ByteSlice buffer = ByteSlice.allocateDirect(100);
        for (int i = 0; i < 100; i++) {
            buffer.put(i, (byte) i);
        }
    }

    @Benchmark
    public void memory_malloc_java2() {
        ByteSlice buffer = ByteSlice.allocateDirect(100);
        for (int i = 0; i < 100; i++) {
            buffer.put(i, (byte) i);
        }
        buffer.asPointer();
    }

    static Pointer pointer = ByteSlice.allocateDirect(100).asPointer();


    private Pointer test_malloc(Pointer p, int a) {
        int b = a++;
        if (b == 0) {
            b = a++;
        }
        return p;
    }

    @Benchmark
    public void memory_invoke_java() {
        Pointer p = test_malloc(pointer, 100);
    }


    private static final Unsafe unsafe;

    static {
        try {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (sun.misc.Unsafe) field.get(null);
        } catch (Exception e) {
            throw new AssertionError(e);
        }

    }

    @Benchmark
    public void memory_malloc_free_unsafe() {
        long p = unsafe.allocateMemory(1000);
        unsafe.freeMemory(p);
    }

    @Benchmark
    public void memory_malloc_free_jna() {
        long p = Native.malloc(1000);
        Native.free(p);
    }

    @Benchmark
    public void memory_get_set_pointer() {
        long p = Native.malloc(1000);
        Pointer pointer = new Pointer(p);
        pointer.setByte(10, (byte) 10);
        byte b = pointer.getByte(10);
        Native.free(p);
    }

    @Benchmark
    public void memory_get_set_directbytebuffer() {
        long p = Native.malloc(1000);
        ByteSlice slice = ByteSlice.fromAddress(p, 1000, false);
        slice.put(10, (byte) 10);
        byte b = slice.get(10);
        Native.free(p);
    }

    static int size = 80;
    static ByteBuffer bb = ByteBufferUtil.allocateDirect(size);
    static long bb_addr = MemoryUtil.getAddress(bb);

    @Benchmark
    public void memory_double_bytebuffer() {
        for (int i = 0; i < size; i += 8) {
            bb.putDouble(i, i);
        }

        for (int i = 0; i < size; i += 8) {
            bb.getDouble(i);
        }
    }

    @Benchmark
    public void memory_double_unsafe() {
        for (int i = 0; i < size; i += 8) {
            if ((i < 0) || (8 > size - i))
                throw new IndexOutOfBoundsException();
            MemoryUtil.unsafe.putDouble(bb_addr + i, i);
        }

        for (int i = 0; i < size; i += 8) {
            if ((i < 0) || (8 > size - i))
                throw new IndexOutOfBoundsException();
            MemoryUtil.unsafe.getDouble(bb_addr + i);
        }
    }
}

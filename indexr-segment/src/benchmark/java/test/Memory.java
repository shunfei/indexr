package test;

import org.apache.spark.unsafe.Platform;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import io.indexr.util.MemoryUtil;

@Warmup(iterations = 1)
@State(Scope.Thread)
@Fork(1)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class Memory {
    @Param({"8000000"})
    public int count;

    int[] heap = new int[count];
    long offheap = MemoryUtil.allocate(count * 4);

    int[] heap2 = new int[count];
    long offheap2 = MemoryUtil.allocate(count * 4);

    @Setup(Level.Invocation)
    public void setup() throws IOException {
        this.heap = new int[count];
        this.offheap = MemoryUtil.allocate(count * 4);

        this.heap2 = new int[count];
        this.offheap2 = MemoryUtil.allocate(count * 4);
    }

    @TearDown(Level.Invocation)
    public void cleanup() throws IOException {
        MemoryUtil.free(offheap);
        MemoryUtil.free(offheap2);
    }

    @Benchmark
    public void heap() {
        for (int i = 0; i < count; i++) {
            heap[i] = heap2[i] + 1;
        }
    }

    @Benchmark
    public void heap2() {
        for (int i = 0; i < count; i += 8) {
            heap[i] = heap2[i] + 1;
            heap[i + 1] = heap2[i + 1] + 1;
            heap[i + 2] = heap2[i + 2] + 1;
            heap[i + 3] = heap2[i + 3] + 1;
            heap[i + 4] = heap2[i + 4] + 1;
            heap[i + 5] = heap2[i + 5] + 1;
            heap[i + 6] = heap2[i + 6] + 1;
            heap[i + 7] = heap2[i + 7] + 1;
        }
    }

    @Benchmark
    public void offheap0() {
        for (int i = 0; i < count; i++) {
            MemoryUtil.setInt(offheap + (i << 2), MemoryUtil.getInt(offheap2 + (i << 2)) + 1);
        }
    }

    @Benchmark
    public void offheap0_1() {
        for (int i = 0; i < count; i += 8) {
            MemoryUtil.setInt(offheap + (i << 2), MemoryUtil.getInt(offheap2 + (i << 2)) + 1);
            MemoryUtil.setInt(offheap + (i + 1 << 2), MemoryUtil.getInt(offheap2 + (i + 1 << 2)) + 1);
            MemoryUtil.setInt(offheap + (i + 2 << 2), MemoryUtil.getInt(offheap2 + (i + 2 << 2)) + 1);
            MemoryUtil.setInt(offheap + (i + 3 << 2), MemoryUtil.getInt(offheap2 + (i + 3 << 2)) + 1);
            MemoryUtil.setInt(offheap + (i + 4 << 2), MemoryUtil.getInt(offheap2 + (i + 4 << 2)) + 1);
            MemoryUtil.setInt(offheap + (i + 5 << 2), MemoryUtil.getInt(offheap2 + (i + 5 << 2)) + 1);
            MemoryUtil.setInt(offheap + (i + 6 << 2), MemoryUtil.getInt(offheap2 + (i + 6 << 2)) + 1);
            MemoryUtil.setInt(offheap + (i + 7 << 2), MemoryUtil.getInt(offheap2 + (i + 7 << 2)) + 1);
        }
    }

    @Benchmark
    public void offheap1() {
        for (int i = 0; i < count; i++) {
            Platform.putInt(null, offheap + (i << 2), Platform.getInt(null, offheap + (i << 2)) + 1);
        }
    }
}

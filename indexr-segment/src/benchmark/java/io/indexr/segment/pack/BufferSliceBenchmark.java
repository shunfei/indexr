package io.indexr.segment.pack;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

import io.indexr.io.ByteSlice;

@State(Scope.Thread)
@Fork(2)
@Measurement(iterations = 5)
@Warmup(iterations = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class BufferSliceBenchmark {
    final static int size = 6553500;

    int[] array;
    ByteSlice slice;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        if (array == null) {
            array = new int[size];
            slice = ByteSlice.allocateDirect(size << 2);
        } else {
            throw new RuntimeException("hhhh");
        }

        for (int i = 0; i < size; i++) {
            array[i] = i;
            slice.putInt(i << 2, i);
        }
    }

    @TearDown(Level.Trial)
    public void clean() throws Exception {
        array = null;
        slice = null;
    }

    @Benchmark
    public void array(Blackhole blackhole) {
        long sum = 0;
        for (int i = 0; i < size; i++) {
            sum += array[i];
        }
        blackhole.consume(sum);
    }

    @Benchmark
    public void slice(Blackhole blackhole) {
        long sum = 0;
        for (int i = 0; i < size; i++) {
            sum += slice.getInt(i << 2);
        }
        blackhole.consume(sum);
    }

}

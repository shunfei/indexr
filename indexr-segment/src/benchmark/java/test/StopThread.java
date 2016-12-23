package test;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(2)
@Measurement(iterations = 5)
@Warmup(iterations = 1)
public class StopThread {

    public volatile boolean stop = false;
    @Param({"32768000"})
    private long count;

    @Benchmark
    public long byVolatile() {
        long i = System.currentTimeMillis();
        long c = 0;
        while (c < count && !stop) {
            i++;
            c++;
        }
        return i;
    }

    @Benchmark
    public long byThread() {
        long i = System.currentTimeMillis();
        long c = 0;
        while (c < count && !Thread.interrupted()) {
            i++;
            c++;
        }
        return i;
    }
}

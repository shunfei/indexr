package test;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(2)
@Measurement(iterations = 5)
@Warmup(iterations = 1)
public class LoopyBenchmarks {
    @Param({"32", "1024", "32768"})
    int size;

    byte[] bunn;

    @Setup
    public void prepare() {
        bunn = new byte[size];
        System.out.println("==================prepare=================");
    }

    @Benchmark
    public void goodOldLoop(Blackhole fox) {
        for (int y = 0; y < bunn.length; y++) { // good old C style for (the win?)
            fox.consume(bunn[y]);
        }
    }

    @Benchmark
    public void sweetLoop(Blackhole fox) {
        for (byte bunny : bunn) { // syntactic sugar loop goodness
            fox.consume(bunny);
        }
    }

    @Benchmark
    public void goodOldLoopReturns(Blackhole fox) {
        byte[] sunn = bunn; // make a local copy of the field
        for (int y = 0; y < sunn.length; y++) {
            fox.consume(sunn[y]);
        }
    }

    @Benchmark
    public int sumOldLoop() {
        int sum = 0;
        for (int y = 0; y < bunn.length; y++) {
            sum += bunn[y];
        }
        return sum;
    }

    @Benchmark
    public int sumSweetLoop() {
        int sum = 0;
        for (byte bunny : bunn) {
            sum += bunny;
        }
        return sum;
    }
}

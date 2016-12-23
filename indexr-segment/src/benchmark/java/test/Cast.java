package test;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@Warmup(iterations = 1)
@State(Scope.Thread)
@Fork(1)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class Cast {

    static class A {
        int f() {
            return 10;
        }
    }

    static class B extends A {
        @Override
        int f() {
            return 20;
        }
    }

    private static final int count = 10000000;
    private static Object[] arr1 = new Object[count];
    private static A[] arr2 = new A[count];
    private static B[] arr3 = new B[count];

    static {
        for (int i = 0; i < count; i++) {
            arr1[i] = new B();
            arr2[i] = new B();
            arr3[i] = new B();
        }
    }

    @Benchmark
    public void cast_obj() {
        for (int i = 0; i < count; i++) {
            ((B) arr1[i]).f();
        }
    }

    @Benchmark
    public void cast_interface() {
        for (int i = 0; i < count; i++) {
            arr2[i].f();
        }
    }

    @Benchmark
    public void cast_origin() {
        for (int i = 0; i < count; i++) {
            arr3[i].f();
        }
    }
}

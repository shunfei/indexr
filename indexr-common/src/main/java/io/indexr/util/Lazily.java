package io.indexr.util;

import java.util.function.Supplier;

// http://stackoverflow.com/a/29141814
public class Lazily {

    @FunctionalInterface
    public interface Lazy<T> extends Supplier<T> {
        Supplier<T> init();

        public default T get() {
            return init().get();
        }
    }

    public static <U> Supplier<U> lazily(Lazy<U> lazy) {
        return lazy;
    }

    public static <T> Supplier<T> value(T value) {
        return () -> value;
    }

    /** Usage example.

     private static class Baz {
     }

     private static Baz createBaz() {
     return new Baz();
     }

     private Supplier<Baz> fieldBaz = Lazily.lazily(() -> fieldBaz = Lazily.value(createBaz()));

     */
}

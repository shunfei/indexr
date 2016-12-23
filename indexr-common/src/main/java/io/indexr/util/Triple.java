package io.indexr.util;

public class Triple<F, S, T> {
    public F first;
    public S second;
    public T third;

    public Triple() {

    }

    public Triple(F first, S second, T third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    public static <F, S, T> Triple<F, S, T> of(F f, S s, T t) {
        return new Triple<>(f, s, t);
    }
}

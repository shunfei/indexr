package io.indexr.util;

public class Pair<F, S> {
    public F first;
    public S second;

    public Pair() {

    }

    public Pair(F first, S second) {
        this.first = first;
        this.second = second;
    }

    public static <F, S> Pair<F, S> of(F f, S s) {
        return new Pair<>(f, s);
    }
}

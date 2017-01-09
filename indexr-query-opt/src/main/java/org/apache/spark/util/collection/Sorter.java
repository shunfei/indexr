package org.apache.spark.util.collection;

import java.util.Comparator;

/**
 * A simple wrapper over the Java implementation [[TimSort]].
 * 
 * The Java implementation is package private, and hence it cannot be called outside package
 * org.apache.spark.util.collection. This is a simple wrapper of it that is available to spark.
 */
public class Sorter<K, Buffer> {
    private SortDataFormat<K, Buffer> s;

    public Sorter(SortDataFormat<K, Buffer> s) {
        this.s = s;
    }

    private TimSort<K, Buffer> timSort = new TimSort<K, Buffer>(s);

    /**
     * Sorts the input buffer within range [lo, hi).
     */
    public void sort(Buffer a, int lo, int hi, Comparator<? super K> c) {
        timSort.sort(a, lo, hi, c);
    }
}

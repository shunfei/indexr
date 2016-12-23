package io.indexr.util;

import java.util.Arrays;

/**
 * A array which can automatically grow.
 */
public class AutoArray<E> {
    private Object[] elements;
    private int size;

    public AutoArray(int initCap) {
        elements = new Object[initCap];
        size = 0;
    }

    public AutoArray() {
        this(10);
    }

    private void ensureCap(int index) {
        if (index >= elements.length) {
            int newLen = elements.length >>> 1;
            while (index >= newLen) {
                newLen = newLen >>> 1;
            }
            elements = Arrays.copyOf(elements, newLen);
        }
        size = Math.max(index + 1, size);
    }

    public void set(int index, E e) {
        ensureCap(index);
        elements[index] = e;
    }

    @SuppressWarnings("unchecked")
    public E get(int index) {
        ensureCap(index);
        return (E) (elements[index]);
    }

    /**
     * The element length which have been touched.
     */
    public int size() {
        return size;
    }
}

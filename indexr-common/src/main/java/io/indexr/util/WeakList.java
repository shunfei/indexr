package io.indexr.util;

import java.lang.ref.WeakReference;
import java.util.Arrays;

public class WeakList<E> {
    private WeakReference<E>[] elements;
    private Creator<E> creator;
    private int size;

    @SuppressWarnings("unchecked")
    public WeakList(Creator<E> creator, int initCap) {
        elements = new WeakReference[initCap];
        this.creator = creator;
        size = 0;
    }

    public WeakList() {
        this(null, 10);
    }

    private void ensureCap(int index) {
        if (index >= elements.length) {
            int newLen = elements.length << 1;
            while (index >= newLen) {
                newLen = newLen << 1;
            }
            elements = Arrays.copyOf(elements, newLen);
        }
        size = Math.max(index + 1, size);
    }

    public void set(int index, E e) {
        ensureCap(index);
        elements[index] = new WeakReference<E>(e);
    }

    public void remove(int index) {
        ensureCap(index);
        elements[index] = null;
    }

    @SuppressWarnings("unchecked")
    public E get(int index) {
        ensureCap(index);
        WeakReference<E> ref = elements[index];
        E e = null;
        if (ref == null || (e = ref.get()) == null) {
            if (creator == null) {
                return null;
            } else {
                e = creator.create(index);
                elements[index] = new WeakReference<E>(e);
                return e;
            }
        }
        return e;
    }

    /**
     * The element length which have been touched.
     */
    public int size() {
        return size;
    }

    @FunctionalInterface
    public static interface Creator<E> {
        E create(int index);
    }
}

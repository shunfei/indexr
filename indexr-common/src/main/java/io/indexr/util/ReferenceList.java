package io.indexr.util;

import java.lang.ref.Reference;
import java.util.Arrays;

public class ReferenceList<E> {
    private Reference<E>[] elements;
    private Creator<E> creator;
    private RefCreator<E> refCreator;
    private int size;

    @FunctionalInterface
    public static interface RefCreator<E> {
        Reference<E> create(E e);
    }

    @SuppressWarnings("unchecked")
    public ReferenceList(Creator<E> creator, RefCreator<E> refCreator, int initCap) {
        this.elements = new Reference[initCap];
        this.creator = creator;
        this.refCreator = refCreator;
        size = 0;
    }

    public ReferenceList(RefCreator<E> refCreator) {
        this(null, refCreator, 10);
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
        elements[index] = refCreator.create(e);
    }

    public void remove(int index) {
        ensureCap(index);
        elements[index] = null;
    }

    @SuppressWarnings("unchecked")
    public E get(int index) {
        ensureCap(index);
        Reference<E> ref = elements[index];
        E e = null;
        if (ref == null || (e = ref.get()) == null) {
            if (creator == null) {
                return null;
            } else {
                e = creator.create(index);
                elements[index] = refCreator.create(e);
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

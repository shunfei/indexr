package io.indexr.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

public class FilterIterator<T> implements Iterator<T> {
    Iterator<T> unfiltered;
    Predicate<T> predicate;
    T next = null;
    boolean finished = false;

    public FilterIterator(Iterator<T> unfiltered, Predicate<T> predicate) {
        this.unfiltered = unfiltered;
        this.predicate = predicate;
    }

    T computeNext() {
        while (unfiltered.hasNext()) {
            T e = unfiltered.next();
            if (predicate.test(e)) {
                return e;
            }
        }
        return null;
    }

    @Override
    public boolean hasNext() {
        if (finished) {
            return false;
        }
        if (next != null) {
            return true;
        }
        next = computeNext();
        if (next == null) {
            finished = true;
            return false;
        } else {
            return true;
        }
    }

    @Override
    public T next() {
        if (next == null) {
            throw new NoSuchElementException();
        }
        T ret = next;
        next = null;
        return ret;
    }
}

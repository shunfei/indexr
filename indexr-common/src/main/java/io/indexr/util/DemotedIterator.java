package io.indexr.util;

import java.util.Iterator;

public class DemotedIterator<Super, Child extends Super> implements Iterator<Super> {
    private final Iterator<Child> it;

    public DemotedIterator(Iterator<Child> it) {
        this.it = it;
    }

    @Override
    public boolean hasNext() {
        return it.hasNext();
    }

    @Override
    public Super next() {
        return it.next();
    }
}

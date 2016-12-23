package io.indexr.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class DemotedList<Super, Child extends Super> implements List<Super> {
    private final List<Child> list;

    public DemotedList(List<Child> list) {
        this.list = list;
    }

    // @formatter:off
    public int size() {return list.size();}
    public boolean isEmpty() {return list.isEmpty();}
    public boolean contains(Object o) {return list.contains(o);}
    public Iterator<Super> iterator() {return new DemotedIterator<>(list.iterator());}
    public Object[] toArray() {return list.toArray();}
    public <T> T[] toArray(T[] a) {return list.toArray(a);}
    public boolean add(Super a) {throw new UnsupportedOperationException();}
    public boolean remove(Object o) {return list.remove(o);}
    public boolean containsAll(Collection<?> c) {return list.containsAll(c);}
    public boolean addAll(Collection<? extends Super> c) {throw new UnsupportedOperationException();}
    public boolean addAll(int index, Collection<? extends Super> c) {throw new UnsupportedOperationException();}
    public boolean removeAll(Collection<?> c) {throw new UnsupportedOperationException();}
    public boolean retainAll(Collection<?> c) {throw new UnsupportedOperationException();}
    public void clear() {throw new UnsupportedOperationException();}
    public Super get(int index) {return list.get(index);}
    public Super set(int index, Super element) {throw new UnsupportedOperationException();}
    public void add(int index, Super element) {throw new UnsupportedOperationException();}
    public Super remove(int index) {throw new UnsupportedOperationException();}
    public int indexOf(Object o) {return list.indexOf(o);}
    public int lastIndexOf(Object o) {return list.lastIndexOf(o);}
    public ListIterator<Super> listIterator() {throw new UnsupportedOperationException();}
    public ListIterator<Super> listIterator(int index) {throw new UnsupportedOperationException();}
    public List<Super> subList(int fromIndex, int toIndex) {throw  new UnsupportedOperationException();}
    // @formatter:on
}

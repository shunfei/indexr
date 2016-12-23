package io.indexr.segment;

import com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.stream.Stream;

public interface RowTraversal {

    default Stream<Row> stream() {throw new UnsupportedOperationException();}

    Iterator<Row> iterator();

    static RowTraversal empty() {
        return new RowTraversal() {
            @Override
            public Stream<Row> stream() {
                return Stream.empty();
            }

            @Override
            public Iterator<Row> iterator() {
                return Iterators.emptyIterator();
            }
        };
    }
}

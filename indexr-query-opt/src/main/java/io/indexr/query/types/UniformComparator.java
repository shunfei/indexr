package io.indexr.query.types;

@FunctionalInterface
public interface UniformComparator {
    int compare(long v1, long v2);
}

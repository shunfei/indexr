package io.indexr.data;

@FunctionalInterface
public interface ByteArraySetter {
    void set(int id, byte[] array, int offset, int len);
}

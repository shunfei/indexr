package io.indexr.data;

@FunctionalInterface
public interface IntSetter {
    /**
     * Specify <code>id</code> with <code>value</code>.
     *
     * @param id    The object index.
     * @param value The object value.
     */
    void set(int id, int value);
}

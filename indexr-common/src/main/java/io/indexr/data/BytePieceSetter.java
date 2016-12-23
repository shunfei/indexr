package io.indexr.data;

@FunctionalInterface
public interface BytePieceSetter {
    /**
     * Specify <code>id</code> with <code>value</code>.
     *
     * @param id    The object index.
     * @param bytes The object value.
     */
    void set(int id, BytePiece bytes);
}

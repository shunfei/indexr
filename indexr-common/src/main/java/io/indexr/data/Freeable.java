package io.indexr.data;

public interface Freeable {
    /**
     * Release the occupied resources.
     */
    void free();
}

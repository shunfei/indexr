package io.indexr.segment.cache;

import io.indexr.data.Freeable;
import io.indexr.data.Sizable;
import io.indexr.io.ByteSlice;

public class CachedByteSlice implements Sizable, Freeable {
    private ByteSlice data;

    public CachedByteSlice(ByteSlice data) {
        this.data = data;
    }

    public ByteSlice data() {
        return data;
    }

    @Override
    public long size() {
        return data.size();
    }

    @Override
    public void free() {
        data.free();
        data = null;
    }
}

package io.indexr.vlt.codec.pack;

public interface LongPackerFactory {
    LongPacker newPacker(int width);
}

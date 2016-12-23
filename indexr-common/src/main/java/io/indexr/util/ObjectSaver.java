package io.indexr.util;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.indexr.io.ByteBufferReader;
import io.indexr.io.ByteBufferWriter;

public class ObjectSaver {

    @FunctionalInterface
    public static interface ObjReader<T> {
        T read(ByteBuffer src);
    }

    @FunctionalInterface
    public static interface ObjWriter<T> {
        void write(T obj, ByteBuffer dst);
    }

    public static <T> T load(ByteBufferReader source, long offset, ObjReader<T> objReader) throws IOException {
        ByteBuffer sizeBuffer = ByteBufferUtil.allocateHeap(4);
        source.read(offset, sizeBuffer, 4);
        sizeBuffer.flip();
        int size = sizeBuffer.getInt();

        ByteBuffer buffer = ByteBufferUtil.allocateDirect(size);

        source.read(offset + 4, buffer, size);
        buffer.flip();
        T obj = objReader.read(buffer);

        assert buffer.remaining() == 0;
        ByteBufferUtil.free(buffer);
        return obj;
    }

    public static <T> void save(ByteBufferWriter dst, T obj, int objSize, ObjWriter<T> objWriter) throws IOException {
        ByteBuffer buffer = ByteBufferUtil.allocateDirect(objSize + 4);
        buffer.putInt(objSize);
        objWriter.write(obj, buffer);

        assert buffer.remaining() == 0;
        buffer.flip();
        dst.write(buffer, objSize + 4);

        ByteBufferUtil.free(buffer);
    }
}

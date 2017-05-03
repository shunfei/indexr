package io.indexr.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import io.indexr.io.ByteBufferWriter;

public interface Serializable {
    /**
     * Write its size(int) and itself into the buffer, only return fase while this buffer cannot contain.
     */
    boolean serialize(ByteBuffer buffer);

    /**
     * Save objects. Objects will first seralized into buffer, then flushed into writer.
     *
     * @param byteBufferWriter the distination to save.
     * @param buffer           the buffer used to cache object data, which could be updated(expand size) by this method.
     *                         Normally it should be reused.
     * @param saveList         the objects to save.
     */
    public static void save(ByteBufferWriter byteBufferWriter,
                            Holder<ByteBuffer> buffer,
                            List<? extends Serializable> saveList) throws IOException {
        ByteBuffer bb = buffer.get();
        while (true) {
            boolean serializeOk = true;
            bb.clear();
            for (Serializable s : saveList) {
                if (!s.serialize(bb)) {
                    serializeOk = false;
                    break;
                }
            }
            if (serializeOk) {
                bb.flip();
                byteBufferWriter.write(bb);
                return;
            } else {
                int nextCap = bb.capacity() << 1;
                ByteBufferUtil.free(bb);
                bb = ByteBufferUtil.allocateDirect(nextCap);
                buffer.set(bb);
            }
        }
    }
}

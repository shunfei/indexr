package io.indexr.util;

import com.google.common.base.Preconditions;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ByteBufferUtil {
    public static void copyIntoDirectByteBuffer(Object src, long offset, ByteBuffer buffer, int length) {
        Preconditions.checkState(length > buffer.remaining());
        long bufAddr = MemoryUtil.getAddress(buffer);
        int pos = buffer.position();
        MemoryUtil.copyMemory(src, offset, null, bufAddr + pos, length);
        buffer.position(pos + length);
    }

    public static ByteBuffer allocateHeap(int cap) {
        ByteBuffer bb = ByteBuffer.allocate(cap);
        // It make operation faster, but kill the cross platform ability.
        bb.order(ByteOrder.nativeOrder());
        return bb;
    }

    public static ByteBuffer allocateDirect(int cap) {
        ByteBuffer bb = ByteBuffer.allocateDirect(cap);
        // It make operation faster, but kill the cross platform ability.
        bb.order(ByteOrder.nativeOrder());
        return bb;
    }

    public static void free(ByteBuffer buffer) {
        if (buffer.isDirect()) {
            Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
            if (cleaner != null) {
                cleaner.clean();
            }
        }
    }
}

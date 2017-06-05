package io.indexr.vlt.segment.index;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.indexr.io.ByteBufferReader;
import io.indexr.io.ByteBufferWriter;
import io.indexr.util.ByteBufferUtil;
import io.indexr.util.DirectBitMap;
import io.indexr.util.MemoryUtil;
import io.indexr.util.Pair;

public class MergeBitMapUtil {

    public static int bitmapSize(int bits) {
        return DirectBitMap.bits2words(bits) << 3;
    }

    public static Pair<DirectBitMap[], ByteBuffer> readBitmaps(ByteBufferReader reader,
                                                               int bitsPerBitmap,
                                                               int startBitmapId,
                                                               int endBitmapId) throws IOException {
        int bitmapSize = bitmapSize(bitsPerBitmap);
        int bitmapCount = endBitmapId - startBitmapId;

        ByteBuffer buffer = ByteBufferUtil.allocateDirect(bitmapCount * bitmapSize);
        reader.read(startBitmapId * bitmapSize, buffer);
        buffer.clear();
        long bufferAddr = MemoryUtil.getAddress(buffer);

        DirectBitMap[] bitMaps = new DirectBitMap[bitmapCount];
        for (int i = 0; i < bitmapCount; i++) {
            bitMaps[i] = new DirectBitMap(bufferAddr + i * bitmapSize, DirectBitMap.bits2words(bitsPerBitmap), null);
        }
        return new Pair<>(bitMaps, buffer);
    }


    public static void readAndMergeBitmaps(ByteBufferReader reader,
                                           DirectBitMap mergeBitmap,
                                           int startBitmapId,
                                           int endBitmapId) throws IOException {
        if (startBitmapId >= endBitmapId) {
            return;
        }
        int bitmapSize = mergeBitmap.wlen << 3;
        int bitmapCount = endBitmapId - startBitmapId;

        ByteBuffer buffer = ByteBufferUtil.allocateDirect(bitmapCount * bitmapSize);
        long bufferAddr = MemoryUtil.getAddress(buffer);

        reader.read(startBitmapId * bitmapSize, buffer);
        buffer.clear();

        for (int id = 0; id < bitmapCount; id++) {
            DirectBitMap bitmap = new DirectBitMap(bufferAddr + id * bitmapSize, mergeBitmap.wlen, null);
            mergeBitmap.or(bitmap);
        }

        ByteBufferUtil.free(buffer);
    }

    public static void readAndMergeBitmaps(ByteBufferReader reader,
                                           DirectBitMap mergeBitmap,
                                           int[] bitmapIds) throws IOException {
        int bitmapSize = mergeBitmap.wlen << 3;

        ByteBuffer buffer = ByteBufferUtil.allocateDirect(bitmapSize);
        long bufferAddr = MemoryUtil.getAddress(buffer);

        DirectBitMap tmpBitmap = new DirectBitMap(bufferAddr, mergeBitmap.wlen, null);
        for (int id : bitmapIds) {
            reader.read(id * bitmapSize, buffer);
            buffer.clear();
            mergeBitmap.or(tmpBitmap);
        }

        ByteBufferUtil.free(buffer);
    }

    private static int calStep(int slot, int bitmapCount) {
        return bitmapCount % slot == 0 ? bitmapCount / slot : bitmapCount / slot + 1;
    }

    public static int idToMergeSlotId(int slot, int bitmapCount, int id) {
        assert slot > 0 && bitmapCount > 0 && id >= 0 && id <= bitmapCount;
        return id / calStep(slot, bitmapCount);
    }

    public static int mergeSlotIdToId(int slot, int bitmapCount, int slotId) {
        assert slot > 0 && bitmapCount > 0 && slotId >= 0 && slotId <= slot;
        return slotId * calStep(slot, bitmapCount);
    }

    /**
     * Merge bitmaps together, splited by mergeSlot.
     */
    public static void mergeBitMaps(ByteBufferReader reader,
                                    ByteBufferWriter mergeBitmapWriter,
                                    int bitCount,
                                    int bitmapCount,
                                    int mergeSlot) throws IOException {
        Preconditions.checkState(mergeSlot <= bitmapCount);

        if (mergeSlot == 0) {
            return;
        }

        DirectBitMap bitmap = new DirectBitMap(bitCount);
        ByteBuffer buffer = (ByteBuffer) bitmap.attach;

        int curSlotIndex = 0;
        int from = 0;
        int bitmapId = 0;
        for (; bitmapId < bitmapCount; bitmapId++) {
            int mergeSlotIndex = idToMergeSlotId(mergeSlot, bitmapCount, bitmapId);
            if (mergeSlotIndex != curSlotIndex) {
                assert curSlotIndex == mergeSlotIndex - 1;

                readAndMergeBitmaps(reader, bitmap, from, bitmapId);

                mergeBitmapWriter.write(buffer);
                buffer.clear();
                bitmap.clear();

                from = bitmapId;
                curSlotIndex = mergeSlotIndex;
            }
        }

        readAndMergeBitmaps(reader, bitmap, from, bitmapId);

        mergeBitmapWriter.write(buffer);
        bitmap.free();
    }
}

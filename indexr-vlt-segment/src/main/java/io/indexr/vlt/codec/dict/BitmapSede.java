//package io.indexr.vlt.codec.dict;
//
//import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
//import org.roaringbitmap.buffer.MutableRoaringBitmap;
//
//import java.io.DataOutputStream;
//import java.io.IOException;
//import java.io.OutputStream;
//import java.nio.ByteBuffer;
//
//import io.indexr.vlt.codec.ErrorCode;
//import io.indexr.util.MemoryUtil;
//
//import static io.indexr.vlt.codec.UnsafeUtil.getInt;
//import static io.indexr.vlt.codec.UnsafeUtil.setByte;
//import static io.indexr.vlt.codec.UnsafeUtil.setBytes;
//import static io.indexr.vlt.codec.UnsafeUtil.setInt;
//
//public class BitmapSede {
//
//    public static int calSlotIndex(int slot, int totalCount, int id) {
//        assert slot > 0 && totalCount > 0 && id >= 0 && id < totalCount;
//        return (int) ((double) id * slot / totalCount);
//    }
//
//    public static int encode(int slot, MutableRoaringBitmap[] bitmaps, long addr, int outputLimit) {
//        MutableRoaringBitmap[] actualBitmaps;
//        if (slot == 0) {
//            actualBitmaps = new MutableRoaringBitmap[0];
//        } else if (slot >= bitmaps.length) {
//            actualBitmaps = bitmaps;
//        } else {
//            actualBitmaps = new MutableRoaringBitmap[slot];
//            int curSlotIndex = 0;
//            int from = 0;
//            int bitmapId = 0;
//            for (; bitmapId < bitmaps.length; bitmapId++) {
//                int slotIndex = calSlotIndex(slot, bitmaps.length, bitmapId);
//                if (slotIndex != curSlotIndex) {
//                    MutableRoaringBitmap[] toMerge = new MutableRoaringBitmap[bitmapId - from];
//                    System.arraycopy(bitmaps, from, toMerge, 0, bitmapId - from);
//                    actualBitmaps[curSlotIndex] = MutableRoaringBitmap.or(toMerge);
//
//                    from = bitmapId;
//                    assert curSlotIndex == slotIndex - 1;
//                    curSlotIndex = slotIndex;
//                }
//            }
//            MutableRoaringBitmap[] toMerge = new MutableRoaringBitmap[bitmapId - from];
//            System.arraycopy(bitmaps, from, toMerge, 0, bitmapId - from);
//            actualBitmaps[curSlotIndex] = MutableRoaringBitmap.or(toMerge);
//        }
//        setInt(addr, actualBitmaps.length);
//        int size = BitmapSede.writeBitmaps(actualBitmaps, addr + 4, outputLimit - 4);
//        if (size < 0) {
//            return size;
//        }
//        return 4 + size;
//    }
//
//    public static ImmutableRoaringBitmap[] decode(long addr, int consumeLimit) {
//        int count = getInt(addr);
//        return BitmapSede.readBitmaps(count, addr + 4, consumeLimit - 4);
//    }
//
//    private static final class DirectDataOuput extends OutputStream {
//        private final long addr;
//        private int offset;
//
//        DirectDataOuput(long addr) {
//            this.addr = addr;
//        }
//
//        @Override
//        public void write(int b) throws IOException {
//            setByte(addr + offset, (byte) b);
//            offset++;
//        }
//
//        @Override
//        public void write(byte[] b) throws IOException {
//            setBytes(addr + offset, b, 0, b.length);
//            offset += b.length;
//        }
//
//        @Override
//        public void write(byte[] b, int off, int len) throws IOException {
//            setBytes(addr + offset, b, off, len);
//            offset += len;
//        }
//    }
//
//    private static int writeBitmaps(MutableRoaringBitmap[] bitmaps, long addr, int ouputLimit) {
//        try {
//            DataOutputStream ouput = new DataOutputStream(new DirectDataOuput(addr));
//            int totalSize = 0;
//            for (int i = 0; i < bitmaps.length; i++) {
//                MutableRoaringBitmap bitmap = bitmaps[i];
//                bitmap.runOptimize();
//                totalSize += bitmap.serializedSizeInBytes();
//                if (totalSize > ouputLimit) {
//                    return ErrorCode.BUFFER_OVERFLOW;
//                }
//                bitmap.serialize(ouput);
//            }
//            return totalSize;
//        } catch (Exception e) {
//            // Never happen.
//            return ErrorCode.BUG;
//        }
//    }
//
//    private static ImmutableRoaringBitmap[] readBitmaps(int count, long addr, int consumeLimit) {
//        ByteBuffer buffer = MemoryUtil.getByteBuffer(addr, consumeLimit, false);
//        ImmutableRoaringBitmap[] bitmaps = new ImmutableRoaringBitmap[count];
//        for (int i = 0; i < count; i++) {
//            ImmutableRoaringBitmap bitmap = new ImmutableRoaringBitmap(buffer);
//            bitmaps[i] = bitmap;
//            buffer.position(buffer.position() + bitmap.serializedSizeInBytes());
//        }
//        return bitmaps;
//    }
//}

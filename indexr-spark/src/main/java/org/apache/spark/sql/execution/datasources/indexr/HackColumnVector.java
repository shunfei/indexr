package org.apache.spark.sql.execution.datasources.indexr;

import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;

import io.indexr.util.MemoryUtil;

public interface HackColumnVector {

    public static HackColumnVector create(ColumnVector columnVector) {
        if (columnVector instanceof OnHeapColumnVector
                && HackOnHeapColumnVector.HACK_ENABLE) {
            return new HackOnHeapColumnVector((OnHeapColumnVector) columnVector);
        } else if (columnVector instanceof OffHeapColumnVector
                && HackOffHeapColumnVector.HACK_ENABLE) {
            return new HackOffHeapColumnVector((OffHeapColumnVector) columnVector);
        }
        return null;
    }

    public void copyInts(long srcAddr, int desOffsetValCount, int valCount);

    public void copyLongs(long srcAddr, int desOffsetValCount, int valCount);

    public void copyFloats(long srcAddr, int desOffsetValCount, int valCount);

    public void copyDoubles(long srcAddr, int desOffsetValCount, int valCount);

    public static class HackOnHeapColumnVector implements HackColumnVector {
        private static long OFFSET_intData;
        private static long OFFSET_longData;
        private static long OFFSET_floatData;
        private static long OFFSET_doubleData;
        private static boolean HACK_ENABLE = true;

        static {
            try {
                OFFSET_intData = MemoryUtil.unsafe.objectFieldOffset(HackOnHeapColumnVector.class.getDeclaredField("intData"));
                OFFSET_longData = MemoryUtil.unsafe.objectFieldOffset(HackOnHeapColumnVector.class.getDeclaredField("longData"));
                OFFSET_floatData = MemoryUtil.unsafe.objectFieldOffset(HackOnHeapColumnVector.class.getDeclaredField("floatData"));
                OFFSET_doubleData = MemoryUtil.unsafe.objectFieldOffset(HackOnHeapColumnVector.class.getDeclaredField("doubleData"));
            } catch (NoSuchFieldException e) {
                HACK_ENABLE = false;
            }
        }

        private final OnHeapColumnVector columnVector;

        public HackOnHeapColumnVector(OnHeapColumnVector columnVector) {
            this.columnVector = columnVector;
        }

        public void copyInts(long srcAddr, int desOffsetValCount, int valCount) {
            MemoryUtil.copyMemory(null, srcAddr, columnVector, OFFSET_intData + (desOffsetValCount << 2), valCount << 2);
        }

        public void copyLongs(long srcAddr, int desOffsetValCount, int valCount) {
            MemoryUtil.copyMemory(null, srcAddr, columnVector, OFFSET_longData + (desOffsetValCount << 3), valCount << 3);
        }

        public void copyFloats(long srcAddr, int desOffsetValCount, int valCount) {
            MemoryUtil.copyMemory(null, srcAddr, columnVector, OFFSET_floatData + (desOffsetValCount << 2), valCount << 2);
        }

        public void copyDoubles(long srcAddr, int desOffsetValCount, int valCount) {
            MemoryUtil.copyMemory(null, srcAddr, columnVector, OFFSET_doubleData + (desOffsetValCount << 3), valCount << 3);
        }
    }

    public static class HackOffHeapColumnVector implements HackColumnVector {
        private static long OFFSET_data;
        private static boolean HACK_ENABLE = true;

        static {
            try {
                OFFSET_data = MemoryUtil.unsafe.objectFieldOffset(OffHeapColumnVector.class.getDeclaredField("data"));
            } catch (NoSuchFieldException e) {
                HACK_ENABLE = false;
            }
        }

        private final long dataAddr;

        public HackOffHeapColumnVector(OffHeapColumnVector columnVector) {
            this.dataAddr = MemoryUtil.unsafe.getLong(columnVector, OFFSET_data);
        }

        public void copyInts(long srcAddr, int desOffsetValCount, int valCount) {
            MemoryUtil.copyMemory(null, srcAddr, null, dataAddr + (desOffsetValCount << 2), valCount << 2);
        }

        public void copyLongs(long srcAddr, int desOffsetValCount, int valCount) {
            MemoryUtil.copyMemory(null, srcAddr, null, dataAddr + (desOffsetValCount << 3), valCount << 3);
        }

        public void copyFloats(long srcAddr, int desOffsetValCount, int valCount) {
            MemoryUtil.copyMemory(null, srcAddr, null, dataAddr + (desOffsetValCount << 2), valCount << 2);
        }

        public void copyDoubles(long srcAddr, int desOffsetValCount, int valCount) {
            MemoryUtil.copyMemory(null, srcAddr, null, dataAddr + (desOffsetValCount << 3), valCount << 3);
        }
    }
}

package io.indexr.segment.storage;

import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

import io.indexr.data.BytePiece;
import io.indexr.segment.DPValues;
import io.indexr.segment.Row;
import io.indexr.segment.pack.DataPack;

final class DPRowSpliterator implements Spliterator<Row>, Iterator<Row>, Row {
    private static final Logger logger = LoggerFactory.getLogger(DPRowSpliterator.class);

    private final long limit;

    private final DPLoader[] loaders;
    private final DPValues[] curPacks;
    private final int[] curPackIds;
    private long nextRowId;

    private int packId;
    private int packRowId;

    DPRowSpliterator(long offset, long limit, DPLoader[] loaders) {
        this.limit = limit;
        this.loaders = loaders;
        this.curPacks = new DPValues[loaders.length];
        this.curPackIds = new int[loaders.length];
        this.nextRowId = offset;
    }

    private void ensurcePack(int colId, int packId) {
        try {
            if (curPacks[colId] == null
                    || curPackIds[colId] != packId) {
                curPacks[colId] = loaders[colId].load(packId);
                curPackIds[colId] = packId;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Row nextRow() {
        long myRowId = nextRowId;
        nextRowId++;
        packId = (int) (myRowId >>> DataPack.SHIFT);
        packRowId = (int) (myRowId & DataPack.MASK);
        return this;
    }

    @Override
    public boolean tryAdvance(Consumer<? super Row> action) {
        if (nextRowId >= limit) {
            return false;
        } else {
            action.accept(nextRow());
            return true;
        }
    }

    @Override
    public boolean hasNext() {
        return nextRowId < limit;
    }

    @Override
    public Row next() {
        return nextRow();
    }

    @Override
    public void forEachRemaining(Consumer<? super Row> action) {
        while (nextRowId < limit) {
            action.accept(nextRow());
        }
    }

    @Override
    public Spliterator<Row> trySplit() {
        long lo = nextRowId;
        long mid = ((nextRowId + limit) >>> 1) & ~1;
        if (nextRowId < mid) {
            nextRowId = mid;
            return new DPRowSpliterator(lo, mid, loaders);
        } else {
            return null;
        }
    }

    @Override
    public long getExactSizeIfKnown() {
        return limit - nextRowId;
    }

    @Override
    public long estimateSize() {
        return limit - nextRowId;
    }

    @Override
    public int characteristics() {
        return ORDERED | SIZED | IMMUTABLE | SUBSIZED;
    }

    @Override
    public long getUniformValue(int colId, byte type) {
        ensurcePack(colId, packId);
        return curPacks[colId].uniformValAt(packRowId, type);
    }

    @Override
    public int getInt(int colId) {
        ensurcePack(colId, packId);
        return curPacks[colId].intValueAt(packRowId);
    }

    @Override
    public long getLong(int colId) {
        ensurcePack(colId, packId);
        return curPacks[colId].longValueAt(packRowId);
    }

    @Override
    public float getFloat(int colId) {
        ensurcePack(colId, packId);
        return curPacks[colId].floatValueAt(packRowId);
    }

    @Override
    public double getDouble(int colId) {
        ensurcePack(colId, packId);
        return curPacks[colId].doubleValueAt(packRowId);
    }

    @Override
    public UTF8String getString(int colId) {
        ensurcePack(colId, packId);
        return curPacks[colId].stringValueAt(packRowId);
    }

    @Override
    public void getRaw(int colId, BytePiece bytes) {
        ensurcePack(colId, packId);
        curPacks[colId].rawValueAt(packRowId, bytes);
    }

    @Override
    public byte[] getRaw(int colId) {
        ensurcePack(colId, packId);
        return curPacks[colId].rawValueAt(packRowId);
    }

    @FunctionalInterface
    public static interface DPLoader {
        DPValues load(int packId) throws IOException;
    }
}

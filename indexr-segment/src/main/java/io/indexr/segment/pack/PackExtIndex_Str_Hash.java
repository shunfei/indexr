package io.indexr.segment.pack;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;

import io.indexr.data.LikePattern;
import io.indexr.io.ByteBufferWriter;
import io.indexr.io.ByteSlice;
import io.indexr.segment.PackExtIndexStr;
import io.indexr.segment.RSValue;
import io.indexr.util.MemoryUtil;

public class PackExtIndex_Str_Hash implements PackExtIndexStr {
    private ByteSlice data;
    private long dataAddr;
    private int rowCount;

    ByteSlice cmpData;

    public PackExtIndex_Str_Hash(int rowCount) {
        this(ByteSlice.allocateDirect(rowCount << 2), null, rowCount);
    }

    public PackExtIndex_Str_Hash(ByteSlice data, ByteSlice cmpData, int rowCount) {
        this.data = data;
        if (data != null && data.size() > 0) {
            this.dataAddr = data.address();
        }
        this.cmpData = cmpData;
        this.rowCount = rowCount;
    }

    @Override
    public int serializedSize() {return cmpData.size();}

    @Override
    public void putValue(int index, UTF8String value) {
        MemoryUtil.setInt(dataAddr + (index << 2), DataHasher.stringHash(value));
    }

    @Override
    public byte isValue(int rowId, UTF8String value) {
        return MemoryUtil.getInt(dataAddr + (rowId << 2)) == DataHasher.stringHash(value)
                ? RSValue.Some
                : RSValue.None;
    }

    @Override
    public byte isLike(int index, LikePattern pattern) {
        return RSValue.Some;
    }

    @Override
    public void compress(DataPackNode dpn) {
        long time = System.currentTimeMillis();

        assert data != null && cmpData == null;
        if (dpn.compress()) {
            assert dpn.numType() == NumType.NInt;
            cmpData = NumOp.compress(NumType.NInt, data, rowCount, dpn.uniformMin(), dpn.uniformMax());
            data.free();
        } else {
            cmpData = data;
        }

        data = null;
        dataAddr = 0;

        PackDurationStat.INSTANCE.add_compressPack(System.currentTimeMillis() - time);
    }

    @Override
    public void decompress(DataPackNode dpn) {
        long time = System.currentTimeMillis();

        assert data == null && cmpData != null;
        if (dpn.compress()) {
            assert dpn.numType() == NumType.NInt;
            data = NumOp.decompress(NumType.NInt, cmpData, rowCount, dpn.uniformMin(), dpn.uniformMax());
            cmpData.free();
        } else {
            data = cmpData;
        }
        dataAddr = data.address();
        cmpData = null;

        PackDurationStat.INSTANCE.add_decompressPack(System.currentTimeMillis() - time);
    }

    @Override
    public void write(ByteBufferWriter writer) throws IOException {
        writer.write(cmpData.byteBuffer(), cmpData.size());
    }

    @Override
    public void free() {
        if (data != null) {
            data.free();
            data = null;
        }
        if (cmpData != null) {
            cmpData.free();
            cmpData = null;
        }
    }
}
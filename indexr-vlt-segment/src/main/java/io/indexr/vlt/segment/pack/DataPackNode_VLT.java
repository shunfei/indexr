package io.indexr.vlt.segment.pack;

import io.indexr.vlt.codec.CodecType;

import java.io.IOException;

import io.indexr.io.ByteBufferWriter;
import io.indexr.io.ByteSlice;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.pack.DataPackNode;

public class DataPackNode_VLT extends DataPackNode {
    private int encodeType; // Indicates the encoding type.
    private int dataSize;   // Size of data in uncompressed status.
    private long sum;       // The sum of numeric type.

    public static final int SIZE = 4 + 1 + 8 + 4 + 8 + 4 + 8 + 4 + 8 + 8 + 4 + 4 + 8;

    public DataPackNode_VLT(int version, SegmentMode mode) {
        super(version, mode);
        this.compress = mode.versionAdapter.isCompress(version, mode);
    }

    @Override
    public boolean isDictEncoded() {return encodeType == CodecType.DICT_COMPRESS.id;}

    // @formatter:off
    public void setEncodeType(CodecType v) {this.encodeType = v.id;}
    public CodecType encodeType() {return CodecType.fromId(encodeType);}

    public void setDataSize(int v) {this.dataSize = v;}
    public int dataSize() {return dataSize;}

    public void setSum(long v){this.sum = v;}
    public long sum(){return sum;}
    // @formatter:on

    @Override
    public DataPackNode clone() {
        DataPackNode_VLT newDPN = new DataPackNode_VLT(this.version, this.mode);
        newDPN.objCount = this.objCount;
        newDPN.compress = this.compress;
        newDPN.packAddr = this.packAddr;
        newDPN.packSize = this.packSize;
        newDPN.indexAddr = this.indexAddr;
        newDPN.indexSize = this.indexSize;
        newDPN.extIndexAddr = this.extIndexAddr;
        newDPN.extIndexSize = this.extIndexSize;
        newDPN.minValue = this.minValue;
        newDPN.maxValue = this.maxValue;
        newDPN.encodeType = this.encodeType;
        newDPN.dataSize = this.dataSize;
        newDPN.sum = this.sum;
        return newDPN;
    }

    @Override
    public void write(ByteBufferWriter writer) throws IOException {
        ByteSlice tmp = ByteSlice.allocateDirect(SIZE);
        int offset = 0;
        tmp.putInt(offset += 0, objCount);
        tmp.put(offset += 4, (byte) (compress ? 1 : 0));
        tmp.putLong(offset += 1, packAddr);
        tmp.putInt(offset += 8, packSize);
        tmp.putLong(offset += 4, indexAddr);
        tmp.putInt(offset += 8, indexSize);
        tmp.putLong(offset += 4, extIndexAddr);
        tmp.putInt(offset += 8, extIndexSize);
        tmp.putLong(offset += 4, minValue);
        tmp.putLong(offset += 8, maxValue);
        tmp.putInt(offset += 8, encodeType);
        tmp.putInt(offset += 4, dataSize);
        tmp.putLong(offset += 4, sum);
        offset += 8;

        assert offset == SIZE && SIZE == mode.versionAdapter.dpnSize(version, mode);

        writer.write(tmp.byteBuffer());
    }

    public static DataPackNode_VLT from(int version, SegmentMode mode, ByteSlice buffer) {
        DataPackNode_VLT dpn = new DataPackNode_VLT(version, mode);
        int offset = 0;
        dpn.objCount = buffer.getInt(offset += 0);
        dpn.compress = (buffer.get(offset += 4) & 0x01) == 1;
        dpn.packAddr = buffer.getLong(offset += 1);
        dpn.packSize = buffer.getInt(offset += 8);
        dpn.indexAddr = buffer.getLong(offset += 4);
        dpn.indexSize = buffer.getInt(offset += 8);
        dpn.extIndexAddr = buffer.getLong(offset += 4);
        dpn.extIndexSize = buffer.getInt(offset += 8);
        dpn.minValue = buffer.getLong(offset += 4);
        dpn.maxValue = buffer.getLong(offset += 8);
        dpn.encodeType = buffer.getInt(offset += 8);
        dpn.dataSize = buffer.getInt(offset += 4);
        dpn.sum = buffer.getLong(offset += 4);
        offset += 8;

        assert offset == SIZE && SIZE == mode.versionAdapter.dpnSize(version, mode);

        return dpn;
    }

    public static final Factory factory = new Factory() {
        @Override
        public DataPackNode create(int version, SegmentMode mode) {
            return new DataPackNode_VLT(version, mode);
        }

        @Override
        public DataPackNode create(int version, SegmentMode mode, ByteSlice buffer) {
            return DataPackNode_VLT.from(version, mode, buffer);
        }
    };
}

package io.indexr.segment.index;

import com.google.common.base.Preconditions;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.indexr.io.ByteBufferWriter;
import io.indexr.io.ByteSlice;
import io.indexr.segment.Column;
import io.indexr.segment.ColumnType;
import io.indexr.segment.PackDurationStat;
import io.indexr.segment.pack.DataHasher;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.pack.DataPackNode;
import io.indexr.segment.pack.DataPackNode_Basic;
import io.indexr.segment.pack.NumOp;
import io.indexr.segment.pack.NumType;
import io.indexr.segment.storage.Version;
import io.indexr.util.BitMap;
import io.indexr.util.MemoryUtil;

public class ExtIndex_Str_Hash extends ExtIndex_SimpleBits {
    private ByteSlice data;
    private long dataAddr;
    private int rowCount;

    private ByteSlice.Supplier cmpDataSupplier;
    private ByteSlice cmpData;
    private int uniMaxHashCode;

    public ExtIndex_Str_Hash(byte dataType, DataPackNode _dpn, DataPack dataPack) {
        super(dataType);
        assert _dpn.version() >= Version.VERSION_6_ID && _dpn instanceof DataPackNode_Basic;
        DataPackNode_Basic dpn = (DataPackNode_Basic) _dpn;

        this.cmpDataSupplier = null;
        this.rowCount = dpn.objCount();

        if (dataType == ColumnType.STRING) {
            // Only meaningful for string.
            this.data = ByteSlice.allocateDirect(rowCount << 2);
            this.dataAddr = data.address();

            int uniMaxHashCode = 0;
            for (int i = 0; i < rowCount; i++) {
                int hashCode = DataHasher.stringHash(dataPack.stringValueAt(i));
                data.putInt(i << 2, hashCode);
                if (i == 0) {
                    uniMaxHashCode = hashCode;
                } else {
                    if (Integer.compareUnsigned(uniMaxHashCode, hashCode) < 0) {
                        uniMaxHashCode = hashCode;
                    }
                }
            }

            dpn.setNumType(NumType.NInt);
            dpn.setUniformMin(0);
            dpn.setUniformMax(uniMaxHashCode);

            long time = System.currentTimeMillis();
            this.cmpData = NumOp.bhcompress(NumType.NInt, data, rowCount, dpn.uniformMin(), dpn.uniformMax());
            PackDurationStat.INSTANCE.add_compressPack(System.currentTimeMillis() - time);
            this.uniMaxHashCode = uniMaxHashCode;
        } else {
            this.data = null;
            this.dataAddr = 0;
            this.cmpData = null;
            this.uniMaxHashCode = 0;
        }
    }

    public ExtIndex_Str_Hash(byte dataType, DataPackNode _dpn, ByteSlice.Supplier cmpDataSupplier) {
        super(dataType);
        assert _dpn.version() >= Version.VERSION_6_ID && _dpn instanceof DataPackNode_Basic;
        DataPackNode_Basic dpn = (DataPackNode_Basic) _dpn;

        this.rowCount = dpn.objCount();
        this.cmpDataSupplier = cmpDataSupplier;
        this.uniMaxHashCode = (int) dpn.uniformMax();
    }

    private ByteSlice getData() throws IOException {
        Preconditions.checkState(dataType == ColumnType.STRING);
        if (data == null) {
            long time = System.currentTimeMillis();
            data = NumOp.bhdecompress(NumType.NInt, getCmpData(), rowCount, 0, uniMaxHashCode);
            PackDurationStat.INSTANCE.add_decompressPack(System.currentTimeMillis() - time);
        }
        return data;
    }

    private ByteSlice getCmpData() throws IOException {
        Preconditions.checkState(dataType == ColumnType.STRING);
        if (cmpData == null) {
            cmpData = cmpDataSupplier.get();
        }
        return cmpData;
    }

    private long getDataAddr() throws IOException {
        Preconditions.checkState(dataType == ColumnType.STRING);
        if (dataAddr == 0) {
            dataAddr = getData().address();
        }
        return dataAddr;
    }

    private boolean hashCheck(UTF8String strValue) throws IOException {
        long dataAddr = getDataAddr();
        long strValueHashCode = DataHasher.stringHash(strValue);
        boolean found = false;
        for (int rowId = 0; rowId < rowCount; rowId++) {
            int hashCode = MemoryUtil.getInt(dataAddr + (rowId << 2));
            if (hashCode == strValueHashCode) {
                found = true;
                break;
            }
        }
        return found;
    }

    @Override
    public BitMap equal(Column column, int packId, long numValue, UTF8String strValue) throws IOException {
        if (dataType == ColumnType.STRING && !hashCheck(strValue)) {
            return BitMap.NONE;
        } else {
            return super.equal(column, packId, numValue, strValue);
        }
    }

    @Override
    public BitMap in(Column column, int packId, long[] numValues, UTF8String[] strValues) throws IOException {
        if (dataType == ColumnType.STRING) {
            boolean found = false;
            for (UTF8String v : strValues) {
                if (hashCheck(v)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return BitMap.NONE;
            } else {
                return super.in(column, packId, numValues, strValues);
            }
        } else {
            return super.in(column, packId, numValues, strValues);
        }
    }

    @Override
    public int serializedSize() {
        return cmpData.size();
    }

    @Override
    public int write(ByteBufferWriter writer) throws IOException {
        ByteBuffer buffer = getCmpData().byteBuffer();
        writer.write(buffer);
        buffer.clear();
        return buffer.capacity();
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
        cmpDataSupplier = null;
    }
}
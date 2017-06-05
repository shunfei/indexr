package io.indexr.vlt.segment;

import io.indexr.vlt.segment.index.ExtIndex_DictBits;
import io.indexr.vlt.segment.index.OuterIndex_Inverted;
import io.indexr.vlt.segment.pack.DataPackCreator_VLT;
import io.indexr.vlt.segment.pack.DataPackNode_VLT;
import io.indexr.vlt.segment.pack.PackCompressor_VLT;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.Arrays;

import io.indexr.data.DictStruct;
import io.indexr.io.ByteBufferReader;
import io.indexr.io.ByteSlice;
import io.indexr.segment.ColumnType;
import io.indexr.segment.OuterIndex;
import io.indexr.segment.PackExtIndex;
import io.indexr.segment.PackRSIndex;
import io.indexr.segment.RSIndex;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.index.ExtIndex_SimpleBits;
import io.indexr.segment.index.OuterIndex_Invalid;
import io.indexr.segment.index.RSIndex_CMap;
import io.indexr.segment.index.RSIndex_CMap_V2;
import io.indexr.segment.index.RSIndex_Histogram;
import io.indexr.segment.index.RSIndex_Histogram_V2;
import io.indexr.segment.index.RSIndex_Str_Invalid;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.pack.DataPackNode;
import io.indexr.segment.pack.DataPackNode_Basic;
import io.indexr.segment.pack.PackCompressor;
import io.indexr.segment.pack.VirtualDataPack;
import io.indexr.segment.storage.PackBundle;
import io.indexr.segment.storage.StorageColumn;
import io.indexr.segment.storage.VersionAdapter;
import io.indexr.segment.storage.VersionAdapter_Basic;
import io.indexr.util.Wrapper;

public class VersionAdapter_VLT implements VersionAdapter {
    private static final RSIndex.Factory RSIF_V0 = RSIndex.Factory.of(
            RSIndex_Str_Invalid.factory,
            RSIndex_Histogram.factory);
    private static final RSIndex.Factory RSIF_V1 = RSIndex.Factory.of(
            RSIndex_CMap.factory,
            RSIndex_Histogram.factory
    );
    private static final RSIndex.Factory RSIF_V2 = RSIndex.Factory.of(
            RSIndex_CMap_V2.factory,
            RSIndex_Histogram.factory
    );
    private static final RSIndex.Factory RSIF_V3 = RSIndex.Factory.of(
            RSIndex_CMap_V2.factory,
            RSIndex_Histogram_V2.factory
    );
    private static final RSIndex.Factory[] VersionRSIndexFactories = new RSIndex.Factory[]{
            RSIF_V0, // v0
            RSIF_V1, // v1
            RSIF_V1, // v2
            null, // ignored
            RSIF_V2, // v4
            RSIF_V2, // v5
            RSIF_V3, // v6
            RSIF_V3, // v7
            RSIF_V3, // v8
    };

    private static final DataPack.Factory DPF_Basic = VersionAdapter_Basic.DPF_Basic;
    private static final DataPack.Factory DPF_VLT = new DataPack.Factory() {
        @Override
        public PackBundle createPackBundle(int version, SegmentMode mode, byte dataType, boolean isIndexed, VirtualDataPack cache) {
            Object values = cache.cacheValues();
            int size = cache.valueCount();
            switch (dataType) {
                case ColumnType.INT:
                    return DataPackCreator_VLT.from(version, mode, isIndexed, (int[]) values, 0, size);
                case ColumnType.LONG:
                    return DataPackCreator_VLT.from(version, mode, isIndexed, (long[]) values, 0, size);
                case ColumnType.FLOAT:
                    return DataPackCreator_VLT.from(version, mode, isIndexed, (float[]) values, 0, size);
                case ColumnType.DOUBLE:
                    return DataPackCreator_VLT.from(version, mode, isIndexed, (double[]) values, 0, size);
                case ColumnType.STRING:
                    return DataPackCreator_VLT.from(version, mode, isIndexed, Arrays.asList((UTF8String[]) values).subList(0, size));
                default:
                    throw new IllegalArgumentException(String.format("Not support data type of %s", dataType));
            }
        }
    };
    private static final DataPack.Factory[] VersionPackFactories = new DataPack.Factory[]{
            DPF_Basic, // v0
            DPF_Basic, // v1
            DPF_Basic, // v2
            null, // ignored
            DPF_Basic, // v4
            DPF_Basic, // v5
            DPF_Basic, // v6
            DPF_VLT, // v7
            DPF_VLT, // v8
    };

    private static final int DPNS_V0_L_V6 = DataPackNode_Basic.SIZE_LE_V6;
    private static final int DPNS_V0_GE_V6 = DataPackNode_Basic.SIZE_GE_V6;
    private static final int DPNS_V1 = DataPackNode_VLT.SIZE;
    private static final int[] VersionDPNSizes = new int[]{
            DPNS_V0_L_V6, // v0
            DPNS_V0_L_V6, // v1
            DPNS_V0_L_V6, // v2
            0, // ignored
            DPNS_V0_L_V6, // v4
            DPNS_V0_L_V6, // v5
            DPNS_V0_GE_V6, // v6
            DPNS_V1, // v7
            DPNS_V1, // v8
    };

    private static final DataPackNode.Factory DPNF_V0 = DataPackNode_Basic.factory;
    private static final DataPackNode.Factory DPNF_VLT = DataPackNode_VLT.factory;
    private static final DataPackNode.Factory[] VersionDPNFactories = new DataPackNode.Factory[]{
            DPNF_V0, // v0
            DPNF_V0, // v1
            DPNF_V0, // v2
            null, // ignored
            DPNF_V0, // v4
            DPNF_V0, // v5
            DPNF_V0, // v6
            DPNF_VLT, // v7
            DPNF_VLT, // v8
    };

    private static final boolean[] VersionCompress = new boolean[]{
            true, // v0
            true, // v1
            true, // v2
            false, // ignored
            true, // v4
            true, // v5
            true, // v6
            true, // v7
            true, // v8
    };

    private static final PackCompressor BH_V0 = PackCompressor.BH_V0;
    private static final PackCompressor BH_V1 = PackCompressor.BH_V1;
    private static final PackCompressor PC_VLT = PackCompressor_VLT.VLT_CODEC;
    private static final PackCompressor[] VersionPackCompressors = new PackCompressor[]{
            BH_V0, // v0
            BH_V1, // v1
            BH_V1, // v2
            null, // ignored
            BH_V1, // v4
            BH_V1, // v5
            BH_V1, // v6
            PC_VLT, // v7
            PC_VLT, // v8
    };

    public PackRSIndex createPackRSIndex(int version, SegmentMode mode, byte dataType) {
        return VersionRSIndexFactories[version].createPack(dataType);
    }

    public RSIndex createRSIndex(int version, SegmentMode mode, byte dataType, ByteSlice buffer, int packCount) {
        return VersionRSIndexFactories[version].create(dataType, buffer, packCount);
    }

    public PackExtIndex createExtIndex(int version, SegmentMode mode, byte dataType, boolean isIndexed, DataPackNode dpn, DataPack dataPack, Object extraInfo) {
        if (dpn.isDictEncoded()) {
            return new ExtIndex_DictBits(dataType);
        } else {
            return new ExtIndex_SimpleBits(dataType);
        }
    }

    public PackExtIndex createExtIndex(int version, SegmentMode mode, byte dataType, boolean isIndexed, DataPackNode dpn, ByteSlice.Supplier data) {
        if (dpn.isDictEncoded()) {
            return new ExtIndex_DictBits(dataType);
        } else {
            return new ExtIndex_SimpleBits(dataType);
        }
    }

    @Override
    public OuterIndex.Cache createOuterIndex(int version, SegmentMode mode, StorageColumn column) throws IOException {
        if (!column.isIndexed()) {
            return new OuterIndex_Invalid.Cache();
        } else {
            return OuterIndex_Inverted.create(version, mode, column);
        }
    }

    @Override
    public OuterIndex loadOuterIndex(int version, SegmentMode mode, byte dataType, ByteBufferReader reader, long size) throws IOException {
        if (size == 0) {
            return new OuterIndex_Invalid();
        }
        return new OuterIndex_Inverted(dataType, reader);
    }

    public PackBundle createPackBundle(int version, SegmentMode mode, byte dataType, boolean isIndexed, VirtualDataPack cache) {
        return VersionPackFactories[version].createPackBundle(version, mode, dataType, isIndexed, cache);
    }

    public int dpnSize(int version, SegmentMode mode) {
        return VersionDPNSizes[version];
    }

    public DataPackNode createDPN(int version, SegmentMode mode) {
        return VersionDPNFactories[version].create(version, mode);
    }

    public DataPackNode createDPN(int verson, SegmentMode mode, ByteSlice buffer) {
        return VersionDPNFactories[verson].create(verson, mode, buffer);
    }

    public boolean isCompress(int version, SegmentMode mode) {
        return VersionCompress[version];
    }

    public ByteSlice compressPack(int version, SegmentMode mode, byte dataType, boolean isIndexed, DataPackNode dpn, ByteSlice data, Wrapper extraInfo) {
        return VersionPackCompressors[version].compress(dataType, isIndexed, dpn, data, extraInfo);
    }

    public ByteSlice decompressPack(int version, SegmentMode mode, byte dataType, DataPackNode dpn, ByteSlice cmpData) {
        return VersionPackCompressors[version].decompress(dataType, dpn, cmpData);
    }

    public ByteSlice getDictStruct(int version, SegmentMode mode, byte dataType, DataPackNode dpn, ByteSlice cmpData) {
        return VersionPackCompressors[version].getDictStruct(dataType, dpn, cmpData);
    }

    public ByteSlice getDataFromDictStruct(int version, SegmentMode mode, byte dataType, DataPackNode dpn, DictStruct dictStruct) {
        return VersionPackCompressors[version].getDataFromDictStruct(dataType, dpn, dictStruct);
    }
}

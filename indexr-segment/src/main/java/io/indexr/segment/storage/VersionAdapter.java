package io.indexr.segment.storage;

import io.indexr.data.DictStruct;
import io.indexr.io.ByteSlice;
import io.indexr.segment.PackExtIndex;
import io.indexr.segment.PackRSIndex;
import io.indexr.segment.RSIndex;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.pack.DataPackNode;
import io.indexr.segment.pack.VirtualDataPack;
import io.indexr.util.Wrapper;

public interface VersionAdapter {

    public PackRSIndex createPackRSIndex(int version, SegmentMode mode, byte dataType);

    public RSIndex createRSIndex(int version, SegmentMode mode, byte dataType, ByteSlice buffer, int packCount);

    public PackExtIndex createExtIndex(int version, SegmentMode mode, byte dataType, boolean isIndexed, DataPackNode dpn, DataPack dataPack, Object extraInfo);

    public PackExtIndex createExtIndex(int version, SegmentMode mode, byte dataType, boolean isIndexed, DataPackNode dpn, ByteSlice.Supplier data);

    public PackBundle createPackBundle(int version, SegmentMode mode, byte dataType, boolean isIndexed, VirtualDataPack cache);

    public int dpnSize(int version, SegmentMode mode);

    public DataPackNode createDPN(int version, SegmentMode mode);

    public DataPackNode createDPN(int verson, SegmentMode mode, ByteSlice buffer);

    public boolean isCompress(int version, SegmentMode mode);

    public ByteSlice compressPack(int version, SegmentMode mode, byte dataType, boolean isIndexed, DataPackNode dpn, ByteSlice data, Wrapper extraInfo);

    public ByteSlice decompressPack(int version, SegmentMode mode, byte dataType, DataPackNode dpn, ByteSlice cmpData);

    public ByteSlice getDictStruct(int version, SegmentMode mode, byte dataType, DataPackNode dpn, ByteSlice cmpData);

    public ByteSlice getDataFromDictStruct(int version, SegmentMode mode, byte dataType, DataPackNode dpn, DictStruct dictStruct);
}

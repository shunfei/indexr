package io.indexr.segment.pack;

import io.indexr.compress.bh.BHCompressor;
import io.indexr.data.DictStruct;
import io.indexr.io.ByteSlice;
import io.indexr.util.Wrapper;

public interface PackCompressor {
    ByteSlice compress(byte dataType, boolean isIndexed, DataPackNode dpn, ByteSlice data, Wrapper extraInfo);

    ByteSlice decompress(byte dataType, DataPackNode dpn, ByteSlice cmpData);

    default ByteSlice getDictStruct(byte dataType, DataPackNode dpn, ByteSlice cmpData) {
        throw new UnsupportedOperationException();
    }

    default ByteSlice getDataFromDictStruct(byte dataType, DataPackNode dpn, DictStruct dictStruct) {
        throw new UnsupportedOperationException();
    }

    public static final PackCompressor BH_V0 = new PackCompressor() {
        @Override
        public ByteSlice compress(byte dataType, boolean isIndexed, DataPackNode _dpn, ByteSlice data, Wrapper extraInfo) {
            DataPackNode_Basic dpn = (DataPackNode_Basic) _dpn;
            if (dpn.packType() == DataPackType.Number) {
                return NumOp.bhcompress(dpn.numType(), data, dpn.objCount(), dpn.uniformMin(), dpn.uniformMax());
            } else {
                return BHCompressor.compressIndexedStr(data, dpn.objCount());
            }
        }

        @Override
        public ByteSlice decompress(byte dataType, DataPackNode _dpn, ByteSlice cmpData) {
            DataPackNode_Basic dpn = (DataPackNode_Basic) _dpn;
            if (dpn.packType() == DataPackType.Number) {
                return NumOp.bhdecompress(dpn.numType(), cmpData, dpn.objCount(), dpn.uniformMin(), dpn.uniformMax());
            } else {
                if (dpn.objCount() == 0 || dpn.maxObjLen() == 0) {
                    return ByteSlice.empty();
                } else {
                    return BHCompressor.decompressIndexedStr(cmpData, dpn.objCount());
                }
            }
        }
    };

    public static final PackCompressor BH_V1 = new PackCompressor() {
        @Override
        public ByteSlice compress(byte dataType, boolean isIndexed, DataPackNode _dpn, ByteSlice data, Wrapper extraInfo) {
            DataPackNode_Basic dpn = (DataPackNode_Basic) _dpn;
            if (dpn.packType() == DataPackType.Number) {
                return NumOp.bhcompress(dpn.numType(), data, dpn.objCount(), dpn.uniformMin(), dpn.uniformMax());
            } else {
                return BHCompressor.compressIndexedStr_v1(data, dpn.objCount());
            }
        }

        @Override
        public ByteSlice decompress(byte dataType, DataPackNode _dpn, ByteSlice cmpData) {
            DataPackNode_Basic dpn = (DataPackNode_Basic) _dpn;
            if (dpn.packType() == DataPackType.Number) {
                return NumOp.bhdecompress(dpn.numType(), cmpData, dpn.objCount(), dpn.uniformMin(), dpn.uniformMax());
            } else {
                return BHCompressor.decompressIndexedStr_v1(cmpData, dpn.objCount());
            }
        }
    };
}

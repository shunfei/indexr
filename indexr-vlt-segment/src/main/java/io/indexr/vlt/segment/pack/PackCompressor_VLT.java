package io.indexr.vlt.segment.pack;

import com.google.common.base.Preconditions;

import io.indexr.vlt.codec.Codec;
import io.indexr.vlt.codec.CodecType;
import io.indexr.vlt.codec.delta.DeltaCodec;
import io.indexr.vlt.codec.dict.DictCompressCodec;
import io.indexr.vlt.codec.dict.DictStructUtil;
import io.indexr.vlt.codec.dict.SimpleDictCodec;

import io.indexr.data.DictStruct;
import io.indexr.io.ByteSlice;
import io.indexr.segment.ColumnType;
import io.indexr.segment.pack.DataPackNode;
import io.indexr.segment.pack.PackCompressor;
import io.indexr.util.MemoryUtil;
import io.indexr.util.Wrapper;

public class PackCompressor_VLT {
    public static final PackCompressor VLT_CODEC = new PackCompressor() {
        @Override
        public ByteSlice compress(byte dataType, boolean isIndexed, DataPackNode _dpn, ByteSlice data, Wrapper extraInfo) {
            DataPackNode_VLT dpn = (DataPackNode_VLT) _dpn;
            Codec codec;
            int cmpSize;
            ByteSlice cmpBuffer;
            switch (dataType) {
                case ColumnType.INT: {
                    if (isIndexed) {
                        codec = new DictCompressCodec();
                        cmpBuffer = ByteSlice.allocateDirect(Codec.outputBufferSize(codec.type(), dpn.objCount(), data.size()));
                        cmpSize = codec.encodeInts(dpn.objCount(), data.address(), cmpBuffer.address(), cmpBuffer.size(), extraInfo);
                    } else {
                        codec = new DictCompressCodec(0.2f);
                        cmpBuffer = ByteSlice.allocateDirect(Codec.outputBufferSize(codec.type(), dpn.objCount(), data.size()));
                        cmpSize = codec.encodeInts(dpn.objCount(), data.address(), cmpBuffer.address(), cmpBuffer.size());
                        if (cmpSize <= 0) {
                            codec = new DeltaCodec();
                            cmpSize = codec.encodeInts(dpn.objCount(), data.address(), cmpBuffer.address(), cmpBuffer.size());
                            Preconditions.checkState(cmpSize >= 0);
                        }
                    }
                    break;
                }
                case ColumnType.LONG: {
                    if (isIndexed) {
                        codec = new DictCompressCodec();
                        cmpBuffer = ByteSlice.allocateDirect(Codec.outputBufferSize(codec.type(), dpn.objCount(), data.size()));
                        cmpSize = codec.encodeLongs(dpn.objCount(), data.address(), cmpBuffer.address(), cmpBuffer.size(), extraInfo);
                    } else {
                        codec = new DictCompressCodec();
                        cmpBuffer = ByteSlice.allocateDirect(Codec.outputBufferSize(codec.type(), dpn.objCount(), data.size()));
                        cmpSize = codec.encodeLongs(dpn.objCount(), data.address(), cmpBuffer.address(), cmpBuffer.size());
                        if (cmpSize <= 0) {
                            codec = new DeltaCodec();
                            cmpSize = codec.encodeLongs(dpn.objCount(), data.address(), cmpBuffer.address(), cmpBuffer.size());
                            Preconditions.checkState(cmpSize >= 0);
                        }
                    }
                    break;
                }
                case ColumnType.FLOAT: {
                    if (isIndexed) {
                        codec = new DictCompressCodec();
                        cmpBuffer = ByteSlice.allocateDirect(Codec.outputBufferSize(codec.type(), dpn.objCount(), data.size()));
                        cmpSize = codec.encodeFloats(dpn.objCount(), data.address(), cmpBuffer.address(), cmpBuffer.size(), extraInfo);
                    } else {
                        codec = new DictCompressCodec();
                        cmpBuffer = ByteSlice.allocateDirect(Codec.outputBufferSize(codec.type(), dpn.objCount(), data.size()));
                        cmpSize = codec.encodeFloats(dpn.objCount(), data.address(), cmpBuffer.address(), cmpBuffer.size());
                        if (cmpSize <= 0) {
                            codec = new DeltaCodec();
                            cmpSize = codec.encodeFloats(dpn.objCount(), data.address(), cmpBuffer.address(), cmpBuffer.size());
                            Preconditions.checkState(cmpSize >= 0);
                        }
                    }
                    break;
                }
                case ColumnType.DOUBLE: {
                    if (isIndexed) {
                        codec = new DictCompressCodec();
                        cmpBuffer = ByteSlice.allocateDirect(Codec.outputBufferSize(codec.type(), dpn.objCount(), data.size()));
                        cmpSize = codec.encodeDoubles(dpn.objCount(), data.address(), cmpBuffer.address(), cmpBuffer.size(), extraInfo);
                    } else {
                        codec = new DictCompressCodec();
                        cmpBuffer = ByteSlice.allocateDirect(Codec.outputBufferSize(codec.type(), dpn.objCount(), data.size()));
                        cmpSize = codec.encodeDoubles(dpn.objCount(), data.address(), cmpBuffer.address(), cmpBuffer.size());
                        if (cmpSize <= 0) {
                            codec = new DeltaCodec();
                            cmpSize = codec.encodeDoubles(dpn.objCount(), data.address(), cmpBuffer.address(), cmpBuffer.size());
                            Preconditions.checkState(cmpSize >= 0);
                        }
                    }
                    break;
                }
                case ColumnType.STRING: {
                    if (isIndexed) {
                        codec = new DictCompressCodec();
                        cmpBuffer = ByteSlice.allocateDirect(Codec.outputBufferSize(codec.type(), dpn.objCount(), data.size()));
                        cmpSize = codec.encodeStrings(dpn.objCount(), data.address(), cmpBuffer.address(), cmpBuffer.size(), extraInfo);
                    } else {
                        codec = new DictCompressCodec();
                        cmpBuffer = ByteSlice.allocateDirect(Codec.outputBufferSize(codec.type(), dpn.objCount(), data.size()));
                        cmpSize = codec.encodeStrings(dpn.objCount(), data.address(), cmpBuffer.address(), cmpBuffer.size());
                        if (cmpSize <= 0) {
                            codec = new DeltaCodec();
                            cmpSize = codec.encodeStrings(dpn.objCount(), data.address(), cmpBuffer.address(), cmpBuffer.size());
                            Preconditions.checkState(cmpSize >= 0);
                        }
                    }
                    break;
                }
                default:
                    throw new RuntimeException("Unsupported data type " + data);
            }
            Preconditions.checkState(cmpSize >= 0);

            ByteSlice cmpData;
            cmpData = ByteSlice.allocateDirect(cmpSize);
            MemoryUtil.copyMemory(cmpBuffer.address(), cmpData.address(), cmpData.size());
            //}
            cmpBuffer.free();

            dpn.setEncodeType(codec.type());

            return cmpData;
        }

        @Override
        public ByteSlice decompress(byte dataType, DataPackNode _dpn, ByteSlice cmpData) {
            DataPackNode_VLT dpn = (DataPackNode_VLT) _dpn;
            ByteSlice data = ByteSlice.allocateDirect(dpn.dataSize());
            Codec codec;
            switch (dpn.encodeType()) {
                case DICT_COMPRESS:
                    codec = new DictCompressCodec();
                    break;
                case SIMPLE_DICT:
                    codec = new SimpleDictCodec();
                    break;
                case DELTA:
                    codec = new DeltaCodec();
                    break;
                default:
                    throw new RuntimeException("Illegal encode type: " + dpn.encodeType());
            }
            int consumedSize;
            switch (dataType) {
                case ColumnType.INT:
                    consumedSize = codec.decodeInts(dpn.objCount(), cmpData.address(), data.address(), data.size());
                    break;
                case ColumnType.LONG:
                    consumedSize = codec.decodeLongs(dpn.objCount(), cmpData.address(), data.address(), data.size());
                    break;
                case ColumnType.FLOAT:
                    consumedSize = codec.decodeFloats(dpn.objCount(), cmpData.address(), data.address(), data.size());
                    break;
                case ColumnType.DOUBLE:
                    consumedSize = codec.decodeDoubles(dpn.objCount(), cmpData.address(), data.address(), data.size());
                    break;
                case ColumnType.STRING:
                    consumedSize = codec.decodeStrings(dpn.objCount(), cmpData.address(), data.address(), data.size());
                    break;
                default:
                    throw new RuntimeException("Unsupported data type " + dataType);
            }
            Preconditions.checkState(consumedSize == dpn.packSize(), "decoder consumed size not equal to compress size");
            return data;
        }

        @Override
        public ByteSlice getDictStruct(byte dataType, DataPackNode _dpn, ByteSlice cmpData) {
            DataPackNode_VLT dpn = (DataPackNode_VLT) _dpn;
            Preconditions.checkState(dpn.encodeType() == CodecType.DICT_COMPRESS);
            int cmpDictStructSize = cmpData.getInt(0);
            ByteSlice dictStructBuffer = ByteSlice.allocateDirect(cmpDictStructSize);
            int consumedSize = DictStructUtil.decompress(
                    dataType,
                    dpn.objCount(),
                    cmpData.address() + 4,
                    dictStructBuffer.address(),
                    dictStructBuffer.size());
            assert 4 + consumedSize == dpn.packSize();
            return dictStructBuffer;
        }

        @Override
        public ByteSlice getDataFromDictStruct(byte dataType, DataPackNode _dpn, DictStruct dictStruct) {
            DataPackNode_VLT dpn = (DataPackNode_VLT) _dpn;
            Preconditions.checkState(dpn.encodeType() == CodecType.DICT_COMPRESS);
            ByteSlice dataBuffer = ByteSlice.allocateDirect(dpn.dataSize());
            switch (dataType) {
                case ColumnType.INT:
                    dictStruct.decodeInts(dpn.objCount(), dataBuffer.address());
                    break;
                case ColumnType.LONG:
                    dictStruct.decodeLongs(dpn.objCount(), dataBuffer.address());
                    break;
                case ColumnType.FLOAT:
                    dictStruct.decodeFloats(dpn.objCount(), dataBuffer.address());
                    break;
                case ColumnType.DOUBLE:
                    dictStruct.decodeDoubles(dpn.objCount(), dataBuffer.address());
                    break;
                case ColumnType.STRING:
                    dictStruct.decodeStrings(dpn.objCount(), dataBuffer.address());
                    break;
                default:
                    throw new RuntimeException("Unsupported data type " + dataType);
            }
            return dataBuffer;
        }
    };
}

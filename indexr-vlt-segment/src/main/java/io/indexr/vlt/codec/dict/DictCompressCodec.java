package io.indexr.vlt.codec.dict;

import io.indexr.vlt.codec.Codec;
import io.indexr.vlt.codec.CodecType;
import io.indexr.vlt.codec.DecoderItf;
import io.indexr.vlt.codec.EncoderItf;

import io.indexr.data.DataType;
import io.indexr.data.DictStruct;
import io.indexr.data.StringsStruct;
import io.indexr.io.ByteSlice;
import io.indexr.util.Wrapper;

import static io.indexr.vlt.codec.UnsafeUtil.getInt;
import static io.indexr.vlt.codec.UnsafeUtil.setInt;

/**
 * <pre>
 * |  cmpDictStructSize |     dictStruct    |
 *           4            cmpDictStructSize
 * </pre>
 */
public class DictCompressCodec implements Codec {
    public static final CodecType TYPE = CodecType.DICT_COMPRESS;
    private final DictCodec dictCodec;

    public DictCompressCodec() {
        this(1);
    }

    public DictCompressCodec(float dictEntryRatioThreshold) {
        this.dictCodec = new DictCodec(dictEntryRatioThreshold);
    }

    @Override
    public CodecType type() {
        return TYPE;
    }

    private int encode(byte dataType,
                       int valCount,
                       long inputAddr,
                       long outputAddr,
                       int outputLimit,
                       Wrapper extraInfo,
                       ByteSlice tmpBuffer,
                       EncoderItf encoder) {
        // Create DictStruct on the tmp buffer.
        int size = encoder.encode(valCount, inputAddr, tmpBuffer.address(), tmpBuffer.size(), extraInfo);
        if (size < 0) {
            tmpBuffer.free();
            return size;
        }

        DictStruct struct = new DictStruct(dataType, tmpBuffer.address());
        // First 4 bytes store the size of uncompressed dict struct.
        setInt(outputAddr, struct.size());

        // Compress and save the struct
        int dictStructEncodedSize = DictStructUtil.compress(struct, valCount, outputAddr + 4, outputLimit - 4);
        if (dictStructEncodedSize < 0) {
            tmpBuffer.free();
            return dictStructEncodedSize;
        }

        tmpBuffer.free();

        return 4 + dictStructEncodedSize;
    }

    private int decode(byte dataType,
                       int valCount,
                       long inputAddr,
                       long outputAddr,
                       int outputLimit,
                       DecoderItf decoder) {
        int dictStructSize = getInt(inputAddr);
        ByteSlice tmpBuffer = ByteSlice.allocateDirect(dictStructSize);
        int dictStructConsumedSize = DictStructUtil.decompress(dataType, valCount, inputAddr + 4, tmpBuffer.address(), tmpBuffer.size());

        decoder.decode(valCount, tmpBuffer.address(), outputAddr, outputLimit);
        tmpBuffer.free();

        return 4 + dictStructConsumedSize;
    }

    @Override
    public int encodeInts(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        int tmpBufferSize = Codec.outputBufferSize(CodecType.DICT, valCount, valCount << 2);
        ByteSlice tmpBuffer = ByteSlice.allocateDirect(tmpBufferSize);
        return encode(DataType.INT, valCount, inputAddr, outputAddr, outputLimit, extraInfo, tmpBuffer, dictCodec::encodeInts);
    }

    @Override
    public int decodeInts(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        return decode(DataType.INT, valCount, inputAddr, outputAddr, outputLimit, dictCodec::decodeInts);
    }

    @Override
    public int encodeLongs(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        int tmpBufferSize = Codec.outputBufferSize(CodecType.DICT, valCount, valCount << 3);
        ByteSlice tmpBuffer = ByteSlice.allocateDirect(tmpBufferSize);
        return encode(DataType.LONG, valCount, inputAddr, outputAddr, outputLimit, extraInfo, tmpBuffer, dictCodec::encodeLongs);
    }

    @Override
    public int decodeLongs(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        return decode(DataType.LONG, valCount, inputAddr, outputAddr, outputLimit, dictCodec::decodeLongs);
    }

    @Override
    public int encodeFloats(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        int tmpBufferSize = Codec.outputBufferSize(CodecType.DICT, valCount, valCount << 2);
        ByteSlice tmpBuffer = ByteSlice.allocateDirect(tmpBufferSize);
        return encode(DataType.FLOAT, valCount, inputAddr, outputAddr, outputLimit, extraInfo, tmpBuffer, dictCodec::encodeFloats);
    }

    @Override
    public int decodeFloats(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        return decode(DataType.FLOAT, valCount, inputAddr, outputAddr, outputLimit, dictCodec::decodeFloats);
    }

    @Override
    public int encodeDoubles(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        int tmpBufferSize = Codec.outputBufferSize(CodecType.DICT, valCount, valCount << 3);
        ByteSlice tmpBuffer = ByteSlice.allocateDirect(tmpBufferSize);
        return encode(DataType.DOUBLE, valCount, inputAddr, outputAddr, outputLimit, extraInfo, tmpBuffer, dictCodec::encodeDoubles);
    }

    @Override
    public int decodeDoubles(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        return decode(DataType.DOUBLE, valCount, inputAddr, outputAddr, outputLimit, dictCodec::decodeDoubles);
    }

    @Override
    public int encodeStrings(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        StringsStruct strings = new StringsStruct(valCount, inputAddr);
        int tmpBufferSize = Codec.outputBufferSize(CodecType.DICT, valCount, strings.totalLen());
        ByteSlice tmpBuffer = ByteSlice.allocateDirect(tmpBufferSize);
        return encode(DataType.STRING, valCount, inputAddr, outputAddr, outputLimit, extraInfo, tmpBuffer, dictCodec::encodeStrings);
    }

    @Override
    public int decodeStrings(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        return decode(DataType.STRING, valCount, inputAddr, outputAddr, outputLimit, dictCodec::decodeStrings);
    }
}

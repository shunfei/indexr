package io.indexr.vlt.codec.dict;

import io.indexr.vlt.codec.Codec;
import io.indexr.vlt.codec.VarInteger;
import io.indexr.vlt.codec.delta.DeltaCodec;
import io.indexr.vlt.codec.plain.PlainCodec;
import io.indexr.vlt.codec.rle.RLEPackingHybridCodec;

import io.indexr.data.DataType;
import io.indexr.data.DictStruct;
import io.indexr.data.StringsStruct;

import static io.indexr.vlt.codec.UnsafeUtil.getByte;
import static io.indexr.vlt.codec.UnsafeUtil.getInt;
import static io.indexr.vlt.codec.UnsafeUtil.setByte;
import static io.indexr.vlt.codec.UnsafeUtil.setInt;

/**
 * <pre>
 * |  dictEntryCount | isDictEntryCompressed | dictEntryData | compressed value data(entry ids) |
 *          4                   1                  size?                size?
 * </pre>
 */
public class DictStructUtil {
    public static int compress(DictStruct dictStruct, int valCount, long outputAddr, int outputLimit) {
        boolean isDictEntryCompressed = dictStruct.dictEntryCount() >= 64 && dictStruct.dictSize() >= 256;
        Codec dictEntryCodec = isDictEntryCompressed ? new DeltaCodec() : new PlainCodec();
        setInt(outputAddr, dictStruct.dictEntryCount());
        setByte(outputAddr + 4, (byte) (isDictEntryCompressed ? 1 : 0));
        int dictEntryEncodedSize;
        switch (dictStruct.dataType()) {
            case DataType.INT:
                dictEntryEncodedSize = dictEntryCodec.encodeInts(
                        dictStruct.dictEntryCount(),
                        dictStruct.dictEntriesAddr(),
                        outputAddr + (4 + 1),
                        outputLimit - (4 + 1));
                break;
            case DataType.LONG:
                dictEntryEncodedSize = dictEntryCodec.encodeLongs(
                        dictStruct.dictEntryCount(),
                        dictStruct.dictEntriesAddr(),
                        outputAddr + (4 + 1),
                        outputLimit - (4 + 1));
                break;
            case DataType.FLOAT:
                dictEntryEncodedSize = dictEntryCodec.encodeFloats(
                        dictStruct.dictEntryCount(),
                        dictStruct.dictEntriesAddr(),
                        outputAddr + (4 + 1),
                        outputLimit - (4 + 1));
                break;
            case DataType.DOUBLE:
                dictEntryEncodedSize = dictEntryCodec.encodeDoubles(
                        dictStruct.dictEntryCount(),
                        dictStruct.dictEntriesAddr(),
                        outputAddr + (4 + 1),
                        outputLimit - (4 + 1));
                break;
            case DataType.STRING:
                dictEntryEncodedSize = dictEntryCodec.encodeStrings(
                        dictStruct.dictEntryCount(),
                        dictStruct.dictEntriesAddr(),
                        outputAddr + (4 + 1),
                        outputLimit - (4 + 1));
                break;
            default:
                throw new RuntimeException("Illegal type: " + dictStruct.dataType());
        }
        if (dictEntryEncodedSize < 0) {
            return dictEntryEncodedSize;
        }

        Codec dataCodec = new RLEPackingHybridCodec(VarInteger.bitWidth(dictStruct.dictEntryCount() - 1));
        int dataEncodedSize = dataCodec.encodeInts(
                valCount,
                dictStruct.dataAddr(),
                outputAddr + 4 + 1 + dictEntryEncodedSize,
                outputLimit - (4 + 1 + dictEntryEncodedSize));
        return 4 + 1 + dictEntryEncodedSize + dataEncodedSize;
    }

    public static int decompress(byte dataType, int valCount, long inputAddr, long outputAddr, int outputLimit) {
        int dictEntryCount = getInt(inputAddr);
        boolean isDictEntryCompressed = getByte(inputAddr + 4) != 0;
        Codec dictEntryCodec = isDictEntryCompressed ? new DeltaCodec() : new PlainCodec();
        setInt(outputAddr, dictEntryCount);
        int dictEntryConsumedSize;
        int dictSize;
        switch (dataType) {
            case DataType.INT:
                dictEntryConsumedSize = dictEntryCodec.decodeInts(
                        dictEntryCount,
                        inputAddr + (4 + 1),
                        outputAddr + 4,
                        outputLimit - 4);
                dictSize = dictEntryCount << 2;
                break;
            case DataType.LONG:
                dictEntryConsumedSize = dictEntryCodec.decodeLongs(
                        dictEntryCount,
                        inputAddr + (4 + 1),
                        outputAddr + 4,
                        outputLimit - 4);
                dictSize = dictEntryCount << 3;
                break;
            case DataType.FLOAT:
                dictEntryConsumedSize = dictEntryCodec.decodeFloats(
                        dictEntryCount,
                        inputAddr + (4 + 1),
                        outputAddr + 4,
                        outputLimit - 4);
                dictSize = dictEntryCount << 2;
                break;
            case DataType.DOUBLE:
                dictEntryConsumedSize = dictEntryCodec.decodeDoubles(
                        dictEntryCount,
                        inputAddr + (4 + 1),
                        outputAddr + 4,
                        outputLimit - 4);
                dictSize = dictEntryCount << 3;
                break;
            case DataType.STRING:
                dictEntryConsumedSize = dictEntryCodec.decodeStrings(
                        dictEntryCount,
                        inputAddr + (4 + 1),
                        outputAddr + 4,
                        outputLimit - 4);
                dictSize = new StringsStruct(dictEntryCount, outputAddr + 4).totalLen();
                break;
            default:
                throw new RuntimeException("Illegal type: " + dataType);
        }
        Codec dataCodec = new RLEPackingHybridCodec(VarInteger.bitWidth(dictEntryCount - 1));
        setInt(outputAddr + 4 + dictSize, valCount << 2);
        int dataConsumedSize = dataCodec.decodeInts(
                valCount,
                inputAddr + (4 + 1) + dictEntryConsumedSize,
                outputAddr + 4 + dictSize + 4,
                outputLimit - (4 + dictSize + 4));
        return 4 + 1 + dictEntryConsumedSize + dataConsumedSize;
    }
}

package io.indexr.segment.pack;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

import io.indexr.compress.bh.BHCompressor;
import io.indexr.io.ByteSlice;
import io.indexr.segment.PackRSIndexStr;
import io.indexr.util.Pair;

/**
 * data:
 * <pre>
 * | str_total_len | start0 | end0 | start1 | end1 | start2 | end2 | s0 | s1 | s2 |
 *       int       | <-                index(int)               -> |<- str_data ->|
 * </pre>
 */
class DataPack_R {
    private static final int MAX_STR_LEN_RAW = Integer.MAX_VALUE / DataPack.MAX_COUNT;

    public static ByteSlice doCompress(DataPack pack, DataPackNode dpn, ByteSlice data) {
        switch (dpn.version()) {
            case Version.VERSION_0_ID: {
                if (dpn.objCount() == 0 || dpn.maxObjLen() == 0) {
                    return ByteSlice.empty();
                } else {
                    return BHCompressor.compressIndexedStr(data, pack.objCount);
                }
            }
            default: {
                return BHCompressor.compressIndexedStr_v1(data, pack.objCount);
            }
        }

    }

    public static ByteSlice doDecompress(DataPack pack, DataPackNode dpn, ByteSlice cmpData) {
        switch (dpn.version()) {
            case Version.VERSION_0_ID: {
                if (dpn.objCount() == 0 || dpn.maxObjLen() == 0) {
                    return ByteSlice.empty();
                } else {
                    return BHCompressor.decompressIndexedStr(cmpData, pack.objCount);
                }
            }
            default: {
                return BHCompressor.decompressIndexedStr_v1(cmpData, pack.objCount);
            }
        }
    }

    public static Pair<DataPack, DataPackNode> fromJavaString(int version, List<? extends CharSequence> strings, PackRSIndexStr index) {
        return from(version, Lists.transform(strings, s -> UTF8String.fromString(s.toString())), index);
    }

    public static Pair<DataPack, DataPackNode> from(int version, List<UTF8String> strings, PackRSIndexStr index) {
        switch (version) {
            case Version.VERSION_0_ID:
                index = index != null ? index : new EmptyRSIndexStr.EmptyPackIndex();
                return _from_v0(version, strings, index);
            case Version.VERSION_1_ID:
            case Version.VERSION_2_ID:
                index = index != null ? index : new RSIndex_CMap.CMapPackIndex();
                return _from_v1(version, strings, index);
            default:
                index = index != null ? index : new RSIndex_CMap_V2.CMapPackIndex();
                return _from_v1(version, strings, index);
        }
    }


    /**
     * We encode the string as UTF-8.
     */
    private static Pair<DataPack, DataPackNode> _from_v1(int version, List<UTF8String> strings, PackRSIndexStr index) {
        int size = strings.size();
        Preconditions.checkArgument(size > 0 && size <= DataPack.MAX_COUNT);

        int strTotalLen = 0;
        for (UTF8String s : strings) {
            int byteCount = s.numBytes();
            Preconditions.checkState(byteCount <= MAX_STR_LEN_RAW, "string in utf-8 should be smaller than %s bytes", MAX_STR_LEN_RAW);
            strTotalLen += byteCount;

            index.putValue(s);
        }
        int indexLen = (size + 1) << 2;

        ByteSlice data = ByteSlice.allocateDirect(indexLen + strTotalLen);

        int offset = 0;
        for (int i = 0; i < size; i++) {
            UTF8String s = strings.get(i);
            byte[] bytes = s.getBytes();
            int byteLen = bytes.length;
            int start = offset;
            int end = start + byteLen;
            data.putInt(i << 2, start);
            data.putInt((i + 1) << 2, end);
            data.put(indexLen + start, bytes);

            offset = end;
        }

        DataPackNode dpn = new DataPackNode(version);
        dpn.setPackType(DataPackType.Raw);
        dpn.setObjCount(size);
        dpn.setMaxObjLen(0); // Always 0
        return new Pair<>(new DataPack(data, null, dpn), dpn);
    }

    //=======================================================================
    // VERSION_0 deprecated

    /**
     * We encode the string as UTF-8.
     */
    private static Pair<DataPack, DataPackNode> _from_v0(int version, List<UTF8String> strings, PackRSIndexStr index) {
        int size = strings.size();
        Preconditions.checkArgument(size > 0 && size <= DataPack.MAX_COUNT);

        int strTotalLen = 0;
        int strMaxLen = 0;
        for (UTF8String s : strings) {
            int byteCount = s.numBytes();
            Preconditions.checkState(byteCount <= MAX_STR_LEN_RAW, "string in utf-8 should be smaller than %s bytes", MAX_STR_LEN_RAW);
            strTotalLen += byteCount;
            strMaxLen = Math.max(strMaxLen, byteCount);

            index.putValue(s);
        }
        int indexLen = size << 3;

        ByteSlice data = ByteSlice.allocateDirect(4 + indexLen + strTotalLen);
        data.putInt(0, strTotalLen);


        int lastEnd = 0;
        for (int i = 0; i < size; i++) {
            UTF8String s = strings.get(i);
            byte[] bytes = s.getBytes();
            int byteLen = bytes.length;
            int start = lastEnd;
            int end = start + byteLen;
            data.putInt((i << 3) + 4, start);
            data.putInt((i << 3) + 4 + 4, end);
            data.put(4 + indexLen + start, bytes);

            strMaxLen = Math.max(strMaxLen, byteLen);
            lastEnd = end;
        }

        DataPackNode dpn = new DataPackNode(version);
        dpn.setPackType(DataPackType.Raw);
        dpn.setObjCount(size);
        dpn.setMaxObjLen(strMaxLen);
        return new Pair<>(new DataPack(data, null, dpn), dpn);
    }
}

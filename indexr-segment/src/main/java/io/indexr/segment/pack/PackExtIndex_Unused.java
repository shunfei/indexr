package io.indexr.segment.pack;

import org.apache.spark.unsafe.types.UTF8String;

import io.indexr.data.LikePattern;
import io.indexr.segment.PackExtIndexNum;
import io.indexr.segment.PackExtIndexStr;
import io.indexr.segment.RSValue;

public class PackExtIndex_Unused implements PackExtIndexNum, PackExtIndexStr {
    @Override
    public void putValue(int rowId, long val) {}

    @Override
    public byte isValue(int rowId, long val) {return RSValue.Some;}

    @Override
    public void putValue(int rowId, UTF8String value) {}

    @Override
    public byte isValue(int rowId, UTF8String value) {return RSValue.Some;}

    @Override
    public byte isLike(int rowId, LikePattern pattern) {return RSValue.Some;}

    @Override
    public int serializedSize() {return 0;}
}

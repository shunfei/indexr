package io.indexr.segment;

import org.apache.spark.unsafe.types.UTF8String;

import io.indexr.data.LikePattern;

public interface PackExtIndexStr extends PackExtIndex {
    void putValue(int rowId, UTF8String value);

    byte isValue(int rowId, UTF8String value);

    public byte isLike(int rowId, LikePattern pattern);
}

package io.indexr.segment;

import org.apache.spark.unsafe.types.UTF8String;

import io.indexr.data.LikePattern;

public interface RSIndexStr extends RSIndex {
    byte isValue(int packId, UTF8String value);

    byte isLike(int packId, LikePattern pattern);
}

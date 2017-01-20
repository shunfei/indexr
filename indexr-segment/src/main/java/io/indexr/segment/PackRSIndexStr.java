package io.indexr.segment;

import org.apache.spark.unsafe.types.UTF8String;

import io.indexr.data.LikePattern;

public interface PackRSIndexStr extends PackRSIndex {
    void putValue(UTF8String value);

    byte isValue(UTF8String value);

    byte isLike(LikePattern pattern);
}

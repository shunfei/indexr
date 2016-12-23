package io.indexr.segment;

import org.apache.spark.unsafe.types.UTF8String;

public interface RSIndexStr extends RSIndex {
    byte isValue(int packId, UTF8String value);

    byte isLike(int packId, UTF8String value);
}

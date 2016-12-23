package io.indexr.segment;

import org.apache.spark.unsafe.types.UTF8String;

public interface PackRSIndexStr extends PackRSIndex {
    void putValue(UTF8String value);

    byte isValue(UTF8String value);

    byte isLike(UTF8String pattern);
}

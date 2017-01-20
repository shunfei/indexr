package io.indexr.segment.rc;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;

import io.indexr.data.LikePattern;
import io.indexr.segment.Column;
import io.indexr.segment.RSIndexStr;

import static io.indexr.segment.RSValue.All;
import static io.indexr.segment.RSValue.None;
import static io.indexr.segment.RSValue.Some;

public class RoughCheck_R {
    public static byte equalCheckOnPack(Column column, int packId, UTF8String value) throws IOException {
        RSIndexStr index = column.rsIndex();
        return index.isValue(packId, value);
    }

    public static byte likeCheckOnPack(Column column, int packId, LikePattern value) throws IOException {
        RSIndexStr index = column.rsIndex();
        return index.isLike(packId, value);
    }

    public static byte inCheckOnPack(Column column, int packId, UTF8String[] values) throws IOException {
        RSIndexStr index = column.rsIndex();
        boolean none = true;
        for (UTF8String v : values) {
            byte vRes = index.isValue(packId, v);
            if (vRes == All) {
                return All;
            } else if (vRes == Some) {
                none = false;
                // There are very little chances be All after Some, so jump out fast.
                break;
            }
        }
        if (none) {
            return None;
        } else {
            return Some;
        }
    }
}

package io.indexr.segment.rc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.BitSet;

import io.indexr.segment.InfoSegment;
import io.indexr.segment.RSValue;
import io.indexr.segment.Segment;

public class NotEqual extends Equal {
    @JsonCreator
    public NotEqual(@JsonProperty("attr") Attr attr,
                    @JsonProperty("numValue") long numValue,
                    @JsonProperty("strValue") String strValue) {
        super(attr, numValue, strValue);
    }

    public NotEqual(Attr attr,
                    long numValue,
                    UTF8String strValue) {
        super(attr, numValue, strValue);
    }

    @Override
    public String getType() {
        return "not_equal";
    }

    @Override
    public RCOperator applyNot() {
        return new Equal(attr, numValue, strValue);
    }

    @Override
    public byte roughCheckOnPack(Segment segment, int packId) throws IOException {
        return RSValue.not(super.roughCheckOnPack(segment, packId));
    }

    @Override
    public byte roughCheckOnColumn(InfoSegment segment) throws IOException {
        return RSValue.not(super.roughCheckOnColumn(segment));
    }

    @Override
    public byte roughCheckOnRow(Segment segment, int packId) throws IOException {
        return RSValue.not(super.roughCheckOnRow(segment, packId));
    }

    @Override
    public BitSet exactCheckOnRow(Segment segment, int packId) throws IOException {
        return RCHelper.not(super.exactCheckOnRow(segment, packId));
    }
}

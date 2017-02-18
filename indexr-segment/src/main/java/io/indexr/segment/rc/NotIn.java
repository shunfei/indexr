package io.indexr.segment.rc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.BitSet;

import io.indexr.segment.InfoSegment;
import io.indexr.segment.RSValue;
import io.indexr.segment.Segment;

public class NotIn extends In {
    @JsonCreator
    public NotIn(@JsonProperty("attr") Attr attr,
                 @JsonProperty("numValues") long[] numValues,
                 @JsonProperty("strValues") String[] strValues) {
        super(attr, numValues, strValues);
    }

    public NotIn(Attr attr,
                 long[] numValues,
                 UTF8String[] strValues) {
        super(attr, numValues, strValues);
    }

    @Override
    public String getType() {
        return "not_in";
    }

    @Override
    public RCOperator applyNot() {
        return new In(attr, numValues, strValues);
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

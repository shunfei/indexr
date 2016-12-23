package io.indexr.segment.rc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.BitSet;

import io.indexr.segment.InfoSegment;
import io.indexr.segment.RSValue;
import io.indexr.segment.Segment;
import io.indexr.segment.pack.DataPack;

public class NotLike extends Like {
    @JsonCreator
    public NotLike(@JsonProperty("attr") Attr attr,
                   @JsonProperty("numValue") long numValue,
                   @JsonProperty("strValue") String strValue) {
        super(attr, numValue, strValue);
    }

    public NotLike(Attr attr,
                   long numValue,
                   UTF8String strValue) {
        super(attr, numValue, strValue);
    }


    @Override
    public String getType() {
        return "not_like";
    }

    @Override
    public RCOperator applyNot() {
        return new Like(attr, numValue, strValue);
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
    public byte roughCheckOnRow(DataPack[] rowPacks) {
        return RSValue.not(super.roughCheckOnRow(rowPacks));
    }

    @Override
    public BitSet exactCheckOnRow(DataPack[] rowPacks) {
        return RCHelper.not(super.exactCheckOnRow(rowPacks));
    }
}

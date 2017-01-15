package io.indexr.segment.rc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;

import io.indexr.segment.InfoSegment;
import io.indexr.segment.RSValue;
import io.indexr.segment.Segment;
import io.indexr.segment.pack.DataPack;

public class UnknownOperator implements CmpOperator {
    @JsonProperty("content")
    public final String content;

    @JsonCreator
    public UnknownOperator(@JsonProperty("content") String content) {
        this.content = content;
    }

    @Override
    public String getType() {
        return "unknown";
    }

    @Override
    public String toString() {
        return String.format("%s(%s)", this.getClass().getSimpleName(), content);
    }

    @Override
    public Collection<Attr> attr() {
        return Collections.emptySet();
    }

    @Override
    public RCOperator applyNot() {
        return this;
    }

    @Override
    public byte roughCheckOnPack(Segment segment, int packId) throws IOException {
        return RSValue.Some;
    }

    @Override
    public byte roughCheckOnColumn(InfoSegment segment) throws IOException {
        return RSValue.Some;
    }

    @Override
    public byte roughCheckOnRow(DataPack[] rowPacks) {
        return RSValue.Some;
    }

    @Override
    public BitSet exactCheckOnRow(DataPack[] rowPacks) {
        // We don't know what this op is, so just assume every rows is ok.
        int rowCount = rowPacks[0].objCount();
        BitSet res = new BitSet(rowCount);
        res.set(0, rowCount, true);
        return res;
    }
}

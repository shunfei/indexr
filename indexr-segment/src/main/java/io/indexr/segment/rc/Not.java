package io.indexr.segment.rc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import io.indexr.segment.InfoSegment;
import io.indexr.segment.Segment;
import io.indexr.segment.pack.DataPack;

public class Not implements LogicalOperator {
    @JsonProperty("child")
    public final RCOperator child;

    @JsonCreator
    public Not(@JsonProperty("child") RCOperator child) {
        this.child = child;
    }

    @Override
    public String getType() {
        return "not";
    }

    @Override
    public List<RCOperator> children() {
        return Collections.singletonList(child);
    }

    @Override
    public RCOperator applyNot() {
        return child.applyNot();
    }

    @Override
    public RCOperator doOptimize() {
        RCOperator newOp = child.doOptimize();
        return newOp != child ? new Not(newOp) : this;
    }

    @Override
    public byte roughCheckOnPack(Segment segment, int packId) throws IOException {
        throw new IllegalStateException("Should not call this method!");
    }

    @Override
    public byte roughCheckOnColumn(InfoSegment segment) throws IOException {
        throw new IllegalStateException("Should not call this method!");
    }

    @Override
    public byte roughCheckOnRow(DataPack[] rowPacks) {
        throw new IllegalStateException("Should not call this method!");
    }

    @Override
    public BitSet exactCheckOnRow(DataPack[] rowPacks) {
        throw new IllegalStateException("Should not call this method!");
    }

    @Override
    public String toString() {
        return String.format("Not[%s]", child.toString());
    }
}

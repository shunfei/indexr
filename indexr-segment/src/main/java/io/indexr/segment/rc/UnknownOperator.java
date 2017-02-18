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
    @JsonProperty("not")
    public final boolean not;

    @JsonCreator
    public UnknownOperator(@JsonProperty("content") String content,
                           @JsonProperty("not") boolean not) {
        this.content = content;
        this.not = not;
    }

    public UnknownOperator(String content) {
        this(content, false);
    }

    @Override
    public String getType() {
        return "unknown";
    }

    @Override
    public String toString() {
        return String.format("%s(%s%s)", this.getClass().getSimpleName(), not ? "NOT " : "", content);
    }

    @Override
    public Collection<Attr> attr() {
        return Collections.emptySet();
    }

    @Override
    public RCOperator applyNot() {
        return new UnknownOperator(content, !not);
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
    public byte roughCheckOnRow(Segment segment, int packId) throws IOException {
        return RSValue.Some;
    }

    @Override
    public BitSet exactCheckOnRow(Segment segment, int packId) throws IOException {
        // We don't know what this op is, so just assume every rows is ok.
        int rowCount = DataPack.MAX_COUNT;
        BitSet res = new BitSet(rowCount);
        res.set(0, rowCount, true);
        return res;
    }
}

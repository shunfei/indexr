package io.indexr.segment.rt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;

import io.indexr.segment.InfoSegment;
import io.indexr.segment.Segment;
import io.indexr.segment.SegmentFd;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.pack.ColumnNode;
import io.indexr.segment.pack.IndexMemCache;
import io.indexr.segment.pack.PackMemCache;

public class RTSGroupInfo implements InfoSegment, SegmentFd {
    @JsonProperty("name")
    public final String name;
    @JsonProperty("rowCount")
    public final long rowCount;
    @JsonProperty("host")
    public final String host;
    @JsonProperty("columnNodes")
    public final ColumnNode[] columnNodes;
    @JsonProperty("schema")
    private SegmentSchema schema;

    @JsonIgnore
    private RTSGroup rtsGroup;

    @JsonCreator
    public RTSGroupInfo(@JsonProperty("name") String name,
                        @JsonProperty("schema") SegmentSchema schema,
                        @JsonProperty("rowCount") long rowCount,
                        @JsonProperty("columnNodes") ColumnNode[] columnNodes,
                        @JsonProperty("host") String host) {
        this.name = name;
        this.schema = schema;
        this.rowCount = rowCount;
        this.columnNodes = columnNodes;
        this.host = host;
    }

    @JsonIgnore
    @Override
    public boolean isRealtime() {
        return true;
    }

    @JsonIgnore
    public SegmentSchema getSchema() {
        return schema;
    }

    public void setRTSGroup(RTSGroup rtsGroup) {
        this.rtsGroup = rtsGroup;
    }

    @JsonIgnore
    public RTSGroup getRTSGroup() {
        return rtsGroup;
    }

    public String host() {
        return host;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long rowCount() {
        return rowCount;
    }

    @Override
    public ColumnNode columnNode(int colId) throws IOException {
        return columnNodes[colId];
    }

    @Override
    public InfoSegment info() {
        return this;
    }

    @Override
    public SegmentSchema schema() {
        return schema;
    }

    @Override
    public Segment open(IndexMemCache indexMemCache, PackMemCache packMemCache) throws IOException {
        return rtsGroup.open(indexMemCache, packMemCache);
    }
}

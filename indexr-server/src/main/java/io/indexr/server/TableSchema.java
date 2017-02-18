package io.indexr.server;

import com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.indexr.segment.SegmentMode;
import io.indexr.segment.SegmentSchema;
import io.indexr.server.rt.RealtimeConfig;

public class TableSchema {
    @JsonProperty("schema")
    public final SegmentSchema schema;
    @JsonProperty("mode")
    public final SegmentMode mode;
    @JsonProperty("realtime")
    public final RealtimeConfig realtimeConfig;

    public TableSchema(@JsonProperty("schema") SegmentSchema schema,
                       @JsonProperty("mode") String mode,
                       @JsonProperty("realtime") RealtimeConfig realtimeConfig) {
        Preconditions.checkState(schema != null && !schema.getColumns().isEmpty(), "Segment schema should not be empty");
        this.schema = schema;
        this.mode = SegmentMode.fromName(mode);
        this.realtimeConfig = realtimeConfig;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableSchema that = (TableSchema) o;

        if (schema != null ? !schema.equals(that.schema) : that.schema != null) return false;
        if (mode != null ? !mode.equals(that.mode) : that.mode != null) return false;
        return realtimeConfig != null ? realtimeConfig.equals(that.realtimeConfig) : that.realtimeConfig == null;
    }
}

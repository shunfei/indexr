package io.indexr.server;

import com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

import io.indexr.segment.SegmentMode;
import io.indexr.segment.SegmentSchema;
import io.indexr.server.rt.RealtimeConfig;
import io.indexr.util.Trick;

public class TableSchema {
    @JsonProperty("schema")
    public final SegmentSchema schema;
    @JsonProperty("mode")
    public final SegmentMode mode;
    @JsonProperty("sort.columns")
    public final List<String> sortColumns;
    @JsonProperty("realtime")
    public final RealtimeConfig realtimeConfig;

    public TableSchema(@JsonProperty("schema") SegmentSchema schema,
                       @JsonProperty("mode") String mode,
                       @JsonProperty("sort.columns") List<String> sortColumns,
                       @JsonProperty("realtime") RealtimeConfig realtimeConfig) {
        Preconditions.checkState(schema != null && !schema.getColumns().isEmpty(), "Segment schema should not be empty");
        sortColumns = sortColumns == null ? Collections.emptyList() : sortColumns;
        Trick.notRepeated(sortColumns, a -> a);

        this.schema = schema;
        this.mode = SegmentMode.fromName(mode);
        this.sortColumns = sortColumns;
        this.realtimeConfig = realtimeConfig;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableSchema that = (TableSchema) o;

        if (schema != null ? !schema.equals(that.schema) : that.schema != null) return false;
        if (mode != that.mode) return false;
        if (sortColumns != null ? !sortColumns.equals(that.sortColumns) : that.sortColumns != null)
            return false;
        return realtimeConfig != null ? realtimeConfig.equals(that.realtimeConfig) : that.realtimeConfig == null;

    }
}

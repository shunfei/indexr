package io.indexr.server;

import com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import io.indexr.segment.SegmentMode;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.rt.AggSchema;
import io.indexr.segment.rt.RealtimeHelper;
import io.indexr.server.rt.RealtimeConfig;
import io.indexr.util.Trick;

public class TableSchema {
    @JsonProperty("schema")
    public final SegmentSchema schema;
    @JsonProperty("mode")
    public String modeName;
    @JsonIgnore
    public SegmentMode mode;
    @JsonProperty("agg")
    public AggSchema aggSchema;
    @JsonProperty("realtime")
    public final RealtimeConfig realtimeConfig;

    public TableSchema(@JsonProperty("schema") SegmentSchema schema,
                       @JsonProperty("mode") String modeName,
                       @JsonProperty("agg") AggSchema aggSchema,
                       @JsonProperty("sort.columns") List<String> sortColumns,
                       @JsonProperty("realtime") RealtimeConfig realtimeConfig) {
        Preconditions.checkState(schema != null && !schema.getColumns().isEmpty(), "Segment schema should not be empty");

        this.schema = schema;
        this.mode = SegmentMode.fromName(modeName);
        this.modeName = this.mode.name();
        this.realtimeConfig = realtimeConfig;

        // Compatibility for old version configuration.
        if (aggSchema == null && this.realtimeConfig != null) {
            aggSchema = this.realtimeConfig.aggSchema;
        }
        if ((aggSchema == null || Trick.isEmpty(aggSchema.dims)) && !Trick.isEmpty(sortColumns)) {
            aggSchema = new AggSchema(false, sortColumns, null);
        }
        if (aggSchema == null) {
            aggSchema = new AggSchema(false, null, null);
        }

        // Make sure agg schemas are the same.
        this.aggSchema = aggSchema;
        if (this.realtimeConfig != null) {
            this.realtimeConfig.setAggSchema(this.aggSchema);
        }

        String error = RealtimeHelper.validateSetting(
                this.schema.columns,
                this.aggSchema.dims,
                this.aggSchema.metrics,
                this.aggSchema.grouping);
        Preconditions.checkState(error == null, error);
    }

    public void setMode(SegmentMode mode) {
        this.modeName = mode.name();
        this.mode = mode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableSchema that = (TableSchema) o;

        if (schema != null ? !schema.equals(that.schema) : that.schema != null) return false;
        if (modeName != null ? !modeName.equals(that.modeName) : that.modeName != null)
            return false;
        if (mode != null ? !mode.equals(that.mode) : that.mode != null) return false;
        if (aggSchema != null ? !aggSchema.equals(that.aggSchema) : that.aggSchema != null)
            return false;
        return realtimeConfig != null ? realtimeConfig.equals(that.realtimeConfig) : that.realtimeConfig == null;
    }
}

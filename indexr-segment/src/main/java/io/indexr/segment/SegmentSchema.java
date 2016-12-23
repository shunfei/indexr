package io.indexr.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import io.indexr.util.JsonUtil;
import io.indexr.util.Trick;

public class SegmentSchema {
    @JsonIgnore
    public List<ColumnSchema> columns;

    @JsonCreator
    public SegmentSchema(@JsonProperty("columns") List<ColumnSchema> columns) {
        Trick.notRepeated(columns, ColumnSchema::getName);

        this.columns = columns;
    }

    public SegmentSchema() {}

    @JsonProperty("columns")
    public List<ColumnSchema> getColumns() {
        return columns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SegmentSchema schema = (SegmentSchema) o;
        return columns != null ? columns.equals(schema.columns) : schema.columns == null;
    }

    @Override
    public String toString() {
        return JsonUtil.toJson(this);
    }
}

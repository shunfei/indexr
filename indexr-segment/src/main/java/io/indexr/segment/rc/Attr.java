package io.indexr.segment.rc;

import com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import io.indexr.segment.ColumnSchema;

/**
 * Attr is a pointer to a column in a table. It could point to different columnIds in different segments.
 */
public class Attr {
    @JsonProperty("columnName")
    public final String columnName;
    @JsonProperty("columnType")
    public final byte columnType;

    private int columnId = -1;

    @JsonCreator
    public Attr(@JsonProperty("columnName") String columnName,
                @JsonProperty("columnType") byte columnType) {
        this.columnName = columnName;
        this.columnType = columnType;
    }

    @JsonIgnore
    public String columnName() {
        return columnName;
    }

    @JsonIgnore
    public int columnId() {
        return columnId;
    }

    @JsonIgnore
    public byte columType() {
        return columnType;
    }

    private static int find(List<ColumnSchema> schemas, String colName, byte columnType) {
        int colId = -1;
        byte colType = 0;
        int ordinal = 0;
        for (ColumnSchema cs : schemas) {
            if (cs.name.equalsIgnoreCase(colName)) {
                colId = ordinal;
                colType = cs.getDataType();
                break;
            }
            ordinal++;
        }
        if (colId == -1) {
            throw new RuntimeException(String.format("column [name: %s, type: %d] not found in columns [%s]", colName, columnType, schemas));
        }
        Preconditions.checkState(colType == columnType, "Column type not match");
        return colId;
    }

    public boolean checkCurrent(List<ColumnSchema> schemas) {
        int colId = find(schemas, columnName, columnType);
        return colId == this.columnId;
    }

    /**
     * Set the real column id in specific schemas.
     */
    public void materialize(List<ColumnSchema> schemas) {
        this.columnId = find(schemas, columnName, columnType);
    }

    @Override
    public String toString() {
        return columnName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Attr attr = (Attr) o;
        return columnName.equals(attr.columnName);
    }

    @Override
    public int hashCode() {
        return columnName != null ? columnName.hashCode() : 0;
    }
}

package io.indexr.segment.rc;

import com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.SQLType;

/**
 * Attr is a pointer to a column in a table. It could point to different columnIds in different segments.
 */
public class Attr {
    @JsonProperty("name")
    public final String name;
    @JsonProperty("type")
    public final SQLType type;

    private int columnId = -1;

    @JsonCreator
    public Attr(@JsonProperty("name") String name,
                @JsonProperty("type") SQLType type) {
        this.name = name;
        this.type = type;
    }

    @JsonIgnore
    public String name() {
        return name;
    }

    @JsonIgnore
    public int columnId() {
        return columnId;
    }

    @JsonIgnore
    public byte dataType() {
        return type.dataType;
    }

    @JsonIgnore
    public SQLType sqlType() {
        return type;
    }

    private static int find(List<ColumnSchema> schemas, String colName, SQLType type) {
        int colId = -1;
        SQLType colType = null;
        int ordinal = 0;
        for (ColumnSchema cs : schemas) {
            if (cs.name.equalsIgnoreCase(colName)) {
                colId = ordinal;
                colType = cs.getSqlType();
                break;
            }
            ordinal++;
        }
        if (colId == -1) {
            throw new RuntimeException(String.format("column [name: %s, type: %s] not found in columns [%s]", colName, type, schemas));
        }
        Preconditions.checkState(colType == type, "Column type not match");
        return colId;
    }

    public boolean checkCurrent(List<ColumnSchema> schemas) {
        int colId = find(schemas, name, type);
        return colId == this.columnId;
    }

    /**
     * Set the real column id in specific schemas.
     */
    public void materialize(List<ColumnSchema> schemas) {
        this.columnId = find(schemas, name, type);
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Attr attr = (Attr) o;
        return name.equals(attr.name);
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }
}

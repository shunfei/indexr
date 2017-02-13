package io.indexr.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang.StringUtils;

import io.indexr.util.JsonUtil;

public class ColumnSchema {
    @JsonIgnore
    public final String name;
    @JsonIgnore
    public final SQLType sqlType;

    // Those fields below only used by realtime ingestion.
    @JsonIgnore
    public final long defaultNumberValue;
    @JsonIgnore
    public final String defaultStringValue;

    @JsonCreator
    public ColumnSchema(@JsonProperty("name") String name,
                        @JsonProperty("dataType") String sqlTypeName,
                        @JsonProperty("default") String defaultValue) {
        this(name, SQLType.fromName(sqlTypeName), defaultValue);
    }

    public ColumnSchema(String name,
                        SQLType sqlType) {
        this(name, sqlType, "");
    }

    public ColumnSchema(String name,
                        SQLType sqlType,
                        String defaultValue) {
        this.name = name.intern();
        this.sqlType = sqlType;
        this.defaultStringValue = defaultValue == null ? "" : defaultValue.intern();
        this.defaultNumberValue = sqlType.isNumber() ? SQLType.parseNumber(sqlType, defaultValue) : 0;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonIgnore
    public byte getDataType() {
        return sqlType.dataType;
    }

    @JsonIgnore
    public SQLType getSqlType() {
        return sqlType;
    }

    @JsonProperty("dataType")
    public String getSQLTypeName() {
        return sqlType.name();
    }

    @JsonProperty("default")
    public String getDefaultStringValue() {
        return defaultStringValue;
    }

    @JsonIgnore
    public long getDefaultNumberValue() {
        return defaultNumberValue;
    }

    @Override
    public String toString() {
        return JsonUtil.toJson(this);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ColumnSchema)) {
            return false;
        }
        ColumnSchema otherCS = (ColumnSchema) other;
        return StringUtils.equals(name, otherCS.name)
                && sqlType == otherCS.sqlType;
    }
}

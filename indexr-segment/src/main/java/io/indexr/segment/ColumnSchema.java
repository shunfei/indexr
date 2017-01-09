package io.indexr.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang.StringUtils;
import org.apache.directory.api.util.Strings;

import io.indexr.util.JsonUtil;

public class ColumnSchema {
    @JsonIgnore
    public final String name;
    @JsonIgnore
    /** @see {@link ColumnType} */
    public final byte dataType;
    @JsonIgnore
    public final String dataTypeName;

    // Those fields below only used by realtime ingestion.
    @JsonIgnore
    public final long defaultNumberValue;
    @JsonIgnore
    public final String defaultStringValue;

    @JsonCreator
    public ColumnSchema(@JsonProperty("name") String name,
                        @JsonProperty("dataType") String dataTypeName,
                        @JsonProperty("default") String defaultValue) {
        this(name, ColumnType.fromName(dataTypeName), defaultValue);
    }

    public ColumnSchema(String name,
                        byte dataType) {
        this(name, dataType, "");
    }

    public ColumnSchema(String name,
                        byte dataType,
                        String defaultValue) {
        this.name = name;
        this.dataTypeName = ColumnType.toName(dataType);
        this.dataType = dataType;
        this.defaultStringValue = defaultValue == null ? "" : defaultValue;
        if (ColumnType.isNumber(dataType)) {
            this.defaultNumberValue = Strings.isEmpty(defaultStringValue)
                    ? 0 : ColumnType.castStringToNumber(defaultStringValue, dataType);
        } else {
            this.defaultNumberValue = 0;
        }
    }


    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonIgnore
    public byte getDataType() {
        return dataType;
    }

    @JsonProperty("dataType")
    public String getDataTypeName() {
        return dataTypeName;
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
                && dataType == otherCS.dataType;
    }
}

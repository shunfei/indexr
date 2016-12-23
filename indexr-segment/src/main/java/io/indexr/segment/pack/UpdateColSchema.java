package io.indexr.segment.pack;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.indexr.segment.ColumnType;

public class UpdateColSchema {
    @JsonProperty("name")
    public final String name;
    @JsonIgnore
    public final byte dataType;
    @JsonProperty("dataType")
    public final String dataTypeName;
    @JsonProperty("value")
    public final String value; // The value of this column, supports sql format, like "a + b" or "if((a > 100), a - 100, a + 100)".

    @JsonCreator
    public UpdateColSchema(@JsonProperty("name") String name,
                           @JsonProperty("dataType") String dataTypeName,
                           @JsonProperty("value") String value) {

        this(name, ColumnType.fromName(dataTypeName), value);
    }

    public UpdateColSchema(String name, byte dataType, String value) {
        this.name = name;
        this.dataType = dataType;
        this.dataTypeName = ColumnType.toName(dataType);
        this.value = value == null ? name : value;
    }

    public UpdateColSchema(String name,
                           byte dataType) {
        this(name, dataType, null);
    }
}

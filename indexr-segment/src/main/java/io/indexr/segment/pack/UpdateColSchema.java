package io.indexr.segment.pack;

import com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.directory.api.util.Strings;

import io.indexr.segment.SQLType;

public class UpdateColSchema {
    @JsonProperty("name")
    public final String name;
    @JsonIgnore
    public final SQLType sqlType;
    @JsonProperty("dataType")
    public final String dataTypeName;
    @JsonProperty("value")
    public final String value; // The value of this column, supports sql format, like "a + b" or "if((a > 100), a - 100, a + 100)".

    @JsonCreator
    public UpdateColSchema(@JsonProperty("name") String name,
                           @JsonProperty("dataType") String dataTypeName,
                           @JsonProperty("value") String value) {

        this(name, SQLType.fromName(dataTypeName), value);
    }

    public UpdateColSchema(String name, SQLType sqlType, String value) {
        Preconditions.checkArgument(!Strings.isEmpty(value));

        this.name = name;
        this.sqlType = sqlType;
        this.dataTypeName = sqlType.toString();
        this.value = value;
    }
}

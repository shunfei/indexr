package io.indexr.segment.storage;

import com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.indexr.segment.SQLType;
import io.indexr.util.Strings;

public class UpdateColSchema {
    @JsonProperty("name")
    public final String name;
    @JsonIgnore
    public final SQLType sqlType;
    @JsonProperty("dataType")
    public final String dataTypeName;
    @JsonProperty("index")
    public final boolean isIndexed;
    @JsonProperty("value")
    public final String value; // The value of this column, supports sql format, like "a + b" or "if((a > 100), a - 100, a + 100)".

    @JsonCreator
    public UpdateColSchema(@JsonProperty("name") String name,
                           @JsonProperty("dataType") String dataTypeName,
                           @JsonProperty("index") Boolean isIndexed,
                           @JsonProperty("value") String value) {

        this(name, SQLType.fromName(dataTypeName), isIndexed == null ? false : isIndexed, value);
    }

    public UpdateColSchema(String name, SQLType sqlType, boolean isIndexed, String value) {
        Preconditions.checkArgument(!Strings.isEmpty(value));

        this.name = name;
        this.sqlType = sqlType;
        this.dataTypeName = sqlType.toString();
        this.isIndexed = isIndexed;
        this.value = value;
    }
}

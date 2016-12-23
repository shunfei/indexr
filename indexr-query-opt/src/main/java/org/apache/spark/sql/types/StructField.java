package org.apache.spark.sql.types;

import io.indexr.query.types.DataType;

public class StructField {
    public final String name;
    public final DataType dataType;
    public final Metadata metadata;

    public StructField(String name, DataType dataType, Metadata metadata) {
        this.name = name;
        this.dataType = dataType;
        this.metadata = metadata;
    }

    public StructField(String name, DataType dataType) {
        this(name, dataType, Metadata.empty);
    }

    public String name() {return name;}

    public DataType dataType() {return dataType;}

    public Metadata metadata() {return metadata;}
}

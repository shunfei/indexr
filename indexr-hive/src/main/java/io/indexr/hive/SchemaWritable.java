package io.indexr.hive;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

import io.indexr.segment.ColumnSchema;

public class SchemaWritable extends ArrayWritable {
    public ColumnSchema[] columns;

    public SchemaWritable() {
        super(Writable.class);
    }
}

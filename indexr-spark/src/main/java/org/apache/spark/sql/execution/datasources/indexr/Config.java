package org.apache.spark.sql.execution.datasources.indexr;

public class Config {
    @Deprecated
    public static final String KEY_COMPRESS = "indexr.compress";
    @Deprecated
    public static final String KEY_SORT_COLUMNS = "indexr.sort.columns";

    public static final String KEY_SEGMENT_MODE = "indexr.segment.mode";
    public static final String KEY_INDEX_COLUMNS = "indexr.index.columns";
    public static final String KEY_AGG_GROUPING = "indexr.agg.grouping";
    public static final String KEY_AGG_DIMS = "indexr.agg.dims";
    public static final String KEY_AGG_METRICS = "indexr.agg.metrics";

    public static final String SPARK_PROJECT_SCHEMA="org.apache.spark.sql.indexr.row.project";
}

package io.indexr.server.rt2his;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.rt.AggSchema;
import io.indexr.util.Trick;

public class HiveHelper {
    @Deprecated
    public static final String KEY_SORT_COLUMNS = "indexr.sort.columns";

    public static final String KEY_SEGMENT_MODE = "indexr.segment.mode";
    public static final String KEY_INDEX_COLUMNS = "indexr.index.columns";
    public static final String KEY_AGG_GROUPING = "indexr.agg.grouping";
    public static final String KEY_AGG_DIMS = "indexr.agg.dims";
    public static final String KEY_AGG_METRICS = "indexr.agg.metrics";

    public static String getHiveTableCreateSql(String tableName,
                                               boolean external,
                                               SegmentSchema schema,
                                               SegmentMode mode,
                                               AggSchema aggSchema,
                                               String location,
                                               String partitionColumn) throws Exception {
        String colDefStr = Joiner.on(",\n").join(Lists.transform(schema.getColumns(), sc -> {
            // Hive is case unsensitive.
            String name = sc.getName().toLowerCase();
            switch (sc.getSqlType()) {
                case INT:
                    return String.format("  `%s` int", name);
                case BIGINT:
                    return String.format("  `%s` bigint", name);
                case FLOAT:
                    return String.format("  `%s` float", name);
                case DOUBLE:
                    return String.format("  `%s` double", name);
                case VARCHAR:
                    return String.format("  `%s` string", name);
                case DATE:
                    return String.format("  `%s` date", name);
                case DATETIME:
                    return String.format("  `%s` timestamp", name);
                default:
                    throw new IllegalStateException("Illegal type :" + sc.getSqlType());
            }
        }));

        String createTableSql = "CREATE ";
        if (external) {
            createTableSql += "EXTERNAL ";
        }
        createTableSql += "TABLE IF NOT EXISTS " + tableName + " (\n" + colDefStr + "\n) \n";
        if (partitionColumn != null) {
            List<String> pc = new ArrayList<>();
            for (String c : partitionColumn.split(",")) {
                pc.add("`" + c + "` string");
            }
            createTableSql += "PARTITIONED BY (" + StringUtils.join(pc, ", ") + ")\n";
        }
        createTableSql += "ROW FORMAT SERDE 'io.indexr.hive.IndexRSerde' \n";
        createTableSql += "STORED AS INPUTFORMAT 'io.indexr.hive.IndexRInputFormat' \n";
        createTableSql += "OUTPUTFORMAT 'io.indexr.hive.IndexROutputFormat' \n";
        if (location != null) {
            createTableSql += "LOCATION '" + location + "' \n";
        }
        createTableSql += "TBLPROPERTIES (\n";
        createTableSql += "'" + KEY_SEGMENT_MODE + "'='" + mode + "'";
        if (aggSchema.grouping) {
            createTableSql += ",\n'" + KEY_AGG_GROUPING + "'='" + String.valueOf(aggSchema.grouping) + "'";
        }
        if (!Trick.isEmpty(aggSchema.dims)) {
            // Hive is case unsensitive.
            List<String> dims = aggSchema.dims.stream().map(String::toLowerCase).collect(Collectors.toList());
            createTableSql += ",\n'" + KEY_AGG_DIMS + "'='" + StringUtils.join(dims, ",") + "'";
        }
        if (!Trick.isEmpty(aggSchema.metrics)) {
            // Hive is case unsensitive.
            createTableSql += ",\n'" + KEY_AGG_METRICS + "'='" + StringUtils.join(aggSchema.metrics.stream().map(m -> m.name.toLowerCase() + ":" + m.aggName()).collect(Collectors.toList()), ",") + "'";
        }

        List<String> indexedColumns = schema.getColumns().stream().filter(ColumnSchema::isIndexed).map(ColumnSchema::getName).collect(Collectors.toList());
        if (!indexedColumns.isEmpty()) {
            createTableSql += ",\n'" + KEY_INDEX_COLUMNS + "'='" + StringUtils.join(indexedColumns, ",") + "'";
        }

        createTableSql += "\n) \n";

        return createTableSql;
    }
}

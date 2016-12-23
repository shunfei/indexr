package io.indexr.server.rt2his;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import io.indexr.segment.ColumnType;
import io.indexr.segment.SegmentSchema;

public class HiveHelper {

    public static String getHiveTableCreateSql(String tableName,
                                               boolean external,
                                               SegmentSchema schema,
                                               String location,
                                               String partitionColumn) throws Exception {
        String colDefStr = Joiner.on(",\n").join(Lists.transform(schema.getColumns(), sc -> {
            switch (sc.getDataType()) {
                case ColumnType.INT:
                    return String.format("  `%s` int", sc.getName());
                case ColumnType.LONG:
                    return String.format("  `%s` bigint", sc.getName());
                case ColumnType.FLOAT:
                    return String.format("  `%s` float", sc.getName());
                case ColumnType.DOUBLE:
                    return String.format("  `%s` double", sc.getName());
                case ColumnType.STRING:
                    return String.format("  `%s` string", sc.getName());
                default:
                    throw new IllegalStateException("column type " + sc.getDataTypeName() + " is illegal");
            }
        }));

        String createTableSql = "CREATE ";
        if (external) {
            createTableSql += "EXTERNAL ";
        }
        createTableSql += "TABLE IF NOT EXISTS " + tableName + " (\n" + colDefStr + "\n) \n";
        if (partitionColumn != null) {
            createTableSql += "PARTITIONED BY (`" + partitionColumn + "` string)\n";
        }
        createTableSql += "ROW FORMAT SERDE 'io.indexr.hive.IndexRSerde' \n";
        createTableSql += "STORED AS INPUTFORMAT 'io.indexr.hive.IndexRInputFormat' \n";
        createTableSql += "OUTPUTFORMAT 'io.indexr.hive.IndexROutputFormat' \n";
        if (location != null) {
            createTableSql += "LOCATION '" + location + "' \n";
        }

        return createTableSql;
    }
}

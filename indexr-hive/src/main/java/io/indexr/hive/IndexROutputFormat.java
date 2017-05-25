package io.indexr.hive;

import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.SQLType;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.rt.AggSchema;
import io.indexr.segment.rt.Metric;
import io.indexr.segment.rt.RealtimeHelper;
import io.indexr.util.Strings;
import io.indexr.util.Trick;

public class IndexROutputFormat implements HiveOutputFormat<Void, ArrayWritable> {

    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(
            JobConf jc,
            Path finalOutPath,
            Class<? extends Writable> valueClass,
            boolean isCompressed,
            Properties tableProperties,
            Progressable progress
    ) throws IOException {
        String columnNameProperty = tableProperties.getProperty(IOConstants.COLUMNS);
        String columnTypeProperty = tableProperties.getProperty(IOConstants.COLUMNS_TYPES);
        Path tableLocation = new Path(tableProperties.getProperty(Config.KEY_LOCATION));
        String compressStr = tableProperties.getProperty(Config.KEY_COMPRESS, "true");
        String modeStr = tableProperties.getProperty(Config.KEY_SEGMENT_MODE);
        String indexColumnsStr = tableProperties.getProperty(Config.KEY_INDEX_COLUMNS);
        String sortColumnsStr = tableProperties.getProperty(Config.KEY_SORT_COLUMNS, "");
        String aggGroupingStr = tableProperties.getProperty(Config.KEY_AGG_GROUPING, "false");
        String aggDimsStr = tableProperties.getProperty(Config.KEY_AGG_DIMS, "");
        String aggMetricsStr = tableProperties.getProperty(Config.KEY_AGG_METRICS, "");

        boolean compress = Boolean.parseBoolean(compressStr.trim());
        SegmentMode mode = SegmentMode.fromNameWithCompress(modeStr, compress);


        List<String> columnNames = new ArrayList<>();
        List<TypeInfo> columnTypes = new ArrayList<>();
        Set<String> indexColumns = new HashSet<>();

        if (!Strings.isEmpty(columnNameProperty)) {
            for (String s : columnNameProperty.trim().split(",")) {
                columnNames.add(s.trim());
            }
        }
        if (!Strings.isEmpty(columnTypeProperty)) {
            columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
        }
        if (!Strings.isEmpty(indexColumnsStr)) {
            for (String s : indexColumnsStr.trim().split(",")) {
                indexColumns.add(s.trim().toLowerCase());
            }
        }
        SegmentSchema schema = convertToIndexRSchema(columnNames, columnTypes, indexColumns);

        boolean grouping = Boolean.parseBoolean(aggGroupingStr.trim());
        List<String> sortColumns = Trick.split(sortColumnsStr, ",", String::trim);
        List<String> dims = Trick.split(aggDimsStr, ",", String::trim);
        if (dims.isEmpty()) {
            dims = sortColumns;
        }
        List<Metric> metrics = Trick.split(aggMetricsStr, ",", s -> {
            String[] ss = s.trim().split(":", 2);
            return new Metric(ss[0].trim(), ss[1].trim());
        });
        AggSchema aggSchema = new AggSchema(
                grouping,
                dims,
                metrics);

        String error = RealtimeHelper.validateSetting(schema.columns, dims, metrics, grouping);
        Preconditions.checkState(error == null, error);

        return new IndexRRecordWriter(jc, schema, finalOutPath, tableLocation, mode, aggSchema);
    }

    private static SegmentSchema convertToIndexRSchema(List<String> columnNames,
                                                       List<TypeInfo> columnTypes,
                                                       Set<String> indexColumns) throws IOException {
        List<ColumnSchema> schemas = new ArrayList<ColumnSchema>();
        for (int i = 0; i < columnNames.size(); i++) {
            String currentColumn = columnNames.get(i);
            TypeInfo currentType = columnTypes.get(i);
            SQLType convertedType = null;

            if (currentType.equals(TypeInfoFactory.intTypeInfo)) {
                convertedType = SQLType.INT;
            } else if (currentType.equals(TypeInfoFactory.longTypeInfo)) {
                convertedType = SQLType.BIGINT;
            } else if (currentType.equals(TypeInfoFactory.floatTypeInfo)) {
                convertedType = SQLType.FLOAT;
            } else if (currentType.equals(TypeInfoFactory.doubleTypeInfo)) {
                convertedType = SQLType.DOUBLE;
            } else if (currentType.equals(TypeInfoFactory.stringTypeInfo)) {
                convertedType = SQLType.VARCHAR;
            } else if (currentType.equals(TypeInfoFactory.dateTypeInfo)) {
                convertedType = SQLType.DATE;
            } else if (currentType.equals(TypeInfoFactory.timestampTypeInfo)) {
                convertedType = SQLType.DATETIME;
            } else {
                throw new IOException("can't recognize this type [" + currentType.getTypeName() + "]");
            }

            boolean isIndexed = indexColumns.contains(currentColumn.toLowerCase());
            schemas.add(new ColumnSchema(currentColumn, convertedType, isIndexed));
        }
        return new SegmentSchema(schemas);
    }

    @Override
    public org.apache.hadoop.mapred.RecordWriter<Void, ArrayWritable> getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable) throws IOException {
        throw new RuntimeException("Should never be used");
    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {

    }
}

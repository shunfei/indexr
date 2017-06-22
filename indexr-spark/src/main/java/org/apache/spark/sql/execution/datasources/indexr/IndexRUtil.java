package org.apache.spark.sql.execution.datasources.indexr;

import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.TimestampType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.indexr.io.ByteBufferReader;
import io.indexr.segment.ColumnSchema;
import io.indexr.segment.SQLType;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.rt.AggSchema;
import io.indexr.segment.rt.Metric;
import io.indexr.segment.rt.RealtimeHelper;
import io.indexr.segment.storage.itg.IntegratedSegment;
import io.indexr.util.Strings;
import io.indexr.util.Trick;

public class IndexRUtil {
    public static class SchemaStruct implements Serializable {
        private static final long serialVersionUID = 1L;

        public SegmentSchema schema;
        public String mode;
        public AggSchema aggSchema;

        public SchemaStruct(SegmentSchema schema, SegmentMode mode, AggSchema aggSchema) {
            this.schema = schema;
            this.mode = mode.name();
            this.aggSchema = aggSchema;
        }
    }

    @FunctionalInterface
    public static interface IsIndexed {
        boolean apply(String name);
    }

    public static SchemaStruct getSchemaStruct(List<StructField> sparkSchema, Map<String, String> options) {
        String modeStr = options.getOrDefault(Config.KEY_SEGMENT_MODE, "");
        String indexColumnsStr = options.getOrDefault(Config.KEY_INDEX_COLUMNS, "");
        String sortColumnsStr = options.getOrDefault(Config.KEY_SORT_COLUMNS, "");
        String aggGroupingStr = options.getOrDefault(Config.KEY_AGG_GROUPING, "false");
        String aggDimsStr = options.getOrDefault(Config.KEY_AGG_DIMS, "");
        String aggMetricsStr = options.getOrDefault(Config.KEY_AGG_METRICS, "");

        SegmentMode mode = SegmentMode.fromName(modeStr);

        Set<String> indexColumns = new HashSet<>();
        if (!Strings.isEmpty(indexColumnsStr)) {
            for (String s : indexColumnsStr.trim().split(",")) {
                indexColumns.add(s.trim().toLowerCase());
            }
        }

        SegmentSchema schema = sparkSchemaToIndexRSchema(sparkSchema, n -> indexColumns.contains(n.toLowerCase()));

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

        return new SchemaStruct(schema, mode, aggSchema);
    }

    public static SegmentSchema sparkSchemaToIndexRSchema(List<StructField> sparkSchema) {
        return sparkSchemaToIndexRSchema(sparkSchema, a -> false);
    }

    public static SegmentSchema sparkSchemaToIndexRSchema(List<StructField> sparkSchema, IsIndexed isIndexed) {
        List<ColumnSchema> columns = new ArrayList<>();
        for (StructField f : sparkSchema) {
            SQLType type;
            if (f.dataType() instanceof IntegerType) {
                type = SQLType.INT;
            } else if (f.dataType() instanceof LongType) {
                type = SQLType.BIGINT;
            } else if (f.dataType() instanceof FloatType) {
                type = SQLType.FLOAT;
            } else if (f.dataType() instanceof DoubleType) {
                type = SQLType.DOUBLE;
            } else if (f.dataType() instanceof StringType) {
                type = SQLType.VARCHAR;
            } else if (f.dataType() instanceof DateType) {
                type = SQLType.DATE;
            } else if (f.dataType() instanceof TimestampType) {
                type = SQLType.DATETIME;
            } else {
                throw new IllegalStateException("Unsupported type: " + f.dataType());
            }
            columns.add(new ColumnSchema(f.name(), type, isIndexed.apply(f.name())));
        }
        return new SegmentSchema(columns);
    }

    public static List<StructField> indexrSchemaToSparkSchema(SegmentSchema schema) {
        List<StructField> fields = new ArrayList<>();
        for (ColumnSchema cs : schema.getColumns()) {
            DataType dataType;
            switch (cs.getSqlType()) {
                case INT:
                    dataType = DataTypes.IntegerType;
                    break;
                case BIGINT:
                    dataType = DataTypes.LongType;
                    break;
                case FLOAT:
                    dataType = DataTypes.FloatType;
                    break;
                case DOUBLE:
                    dataType = DataTypes.DoubleType;
                    break;
                case VARCHAR:
                    dataType = DataTypes.StringType;
                    break;
                case DATE:
                    dataType = DataTypes.DateType;
                    break;
                case DATETIME:
                    dataType = DataTypes.TimestampType;
                    break;
                default:
                    throw new IllegalStateException("Unsupported type: " + cs.getSqlType());
            }
            fields.add(new StructField(cs.getName(), dataType, scala.Boolean.box(false), Metadata.empty()));
        }
        return fields;
    }

    public static List<StructField> inferSchema(List<FileStatus> files, Configuration configuration) {
        try {
            for (FileStatus fileStatus : files) {
                Path path = fileStatus.getPath();
                if (!SegmentHelper.checkSegmentByPath(path)
                        || fileStatus.getLen() == 0) {
                    continue;
                }

                FileSystem fileSystem = path.getFileSystem(configuration);
                ByteBufferReader.Opener opener = ByteBufferReader.Opener.create(fileSystem, path);
                IntegratedSegment.Fd fd = IntegratedSegment.Fd.create(path.toString(), opener);
                if (fd != null) {
                    return indexrSchemaToSparkSchema(fd.info().schema());
                }
            }
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}

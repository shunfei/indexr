package io.indexr.hive;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import io.indexr.segment.SegmentMode;

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
        boolean compress = Boolean.parseBoolean(compressStr);
        SegmentMode mode = SegmentMode.fromNameWithCompress(modeStr, compress);

        List<String> columnNames;
        List<TypeInfo> columnTypes;

        if (columnNameProperty.length() == 0) {
            columnNames = new ArrayList<String>();
        } else {
            columnNames = Arrays.asList(columnNameProperty.split(","));
        }

        if (columnTypeProperty.length() == 0) {
            columnTypes = new ArrayList<TypeInfo>();
        } else {
            columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
        }

        return new IndexRRecordWriter(jc, columnNames, columnTypes, finalOutPath, tableLocation, mode);
    }

    @Override
    public org.apache.hadoop.mapred.RecordWriter<Void, ArrayWritable> getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable) throws IOException {
        throw new RuntimeException("Should never be used");
    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {

    }
}

package io.indexr.hive;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.nio.file.Files;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.Row;
import io.indexr.segment.SQLType;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.helper.SimpleRow;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.rt.AggSchema;
import io.indexr.segment.storage.DPSegment;
import io.indexr.segment.storage.OpenOption;
import io.indexr.segment.storage.SortedSegmentGenerator;
import io.indexr.segment.storage.Version;
import io.indexr.util.DateTimeUtil;

public class IndexRRecordWriter implements FileSinkOperator.RecordWriter, RecordWriter<Void, ArrayWritable> {
    private static final Log logger = LogFactory.getLog(IndexRRecordWriter.class);

    private SegmentGen segmentGen;
    private SQLType[] sqlTypes;
    private SimpleRow.Builder rowBuilder;
    private FileSystem fileSystem;
    private java.nio.file.Path localSegmentPath;
    private String segmentName;

    private Path tableLocation;
    private Path segmentOutPath;

    public IndexRRecordWriter(JobConf jobConf,
                              SegmentSchema schema,
                              Path finalOutPath,
                              Path tableLocation,
                              SegmentMode mode,
                              AggSchema aggSchema) throws IOException {
        // Hive may ask to create a file located on local file system.
        // We have to get the real file system by path's schema.
        this.fileSystem = FileSystem.get(finalOutPath.toUri(), FileSystem.get(jobConf).getConf());

        this.localSegmentPath = Files.createTempDirectory("_segment_gen_");
        this.tableLocation = tableLocation;
        this.segmentOutPath = finalOutPath;

        this.sqlTypes = new SQLType[schema.columns.size()];
        int i = 0;
        for (ColumnSchema sc : schema.columns) {
            this.sqlTypes[i] = sc.getSqlType();
            i++;
        }
        this.rowBuilder = SimpleRow.Builder.createByColumnSchemas(schema.columns);

        if (aggSchema.dims.isEmpty()) {
            DPSegment segment = DPSegment.open(
                    Version.LATEST_ID,
                    mode,
                    localSegmentPath,
                    segmentName,
                    schema,
                    OpenOption.Overwrite).update();
            this.segmentGen = new SegmentGen() {
                @Override
                public void add(Row row) throws IOException {
                    segment.add(row);
                }

                @Override
                public DPSegment gen() throws IOException {
                    segment.seal();
                    return segment;
                }
            };
        } else {
            SortedSegmentGenerator generator = new SortedSegmentGenerator(
                    Version.LATEST_ID,
                    mode,
                    localSegmentPath,
                    segmentName,
                    schema,
                    aggSchema.grouping,
                    aggSchema.dims,
                    aggSchema.metrics,
                    DataPack.MAX_COUNT * 10);
            this.segmentGen = new SegmentGen() {
                @Override
                public void add(Row row) throws IOException {
                    generator.add(row);
                }

                @Override
                public DPSegment gen() throws IOException {
                    return generator.seal();
                }
            };
        }
    }

    @Override
    public void write(Writable w) throws IOException {
        ArrayWritable datas = (ArrayWritable) w;
        for (int colId = 0; colId < sqlTypes.length; colId++) {
            SQLType type = sqlTypes[colId];
            Writable currentValue = datas.get()[colId];
            switch (type) {
                case INT:
                    if (currentValue == null) {
                        rowBuilder.appendInt(0);
                    } else {
                        rowBuilder.appendInt(((IntWritable) currentValue).get());
                    }
                    break;
                case BIGINT:
                    if (currentValue == null) {
                        rowBuilder.appendLong(0L);
                    } else {
                        rowBuilder.appendLong(((LongWritable) currentValue).get());
                    }
                    break;
                case FLOAT:
                    if (currentValue == null) {
                        rowBuilder.appendFloat(0f);
                    } else {
                        rowBuilder.appendFloat(((FloatWritable) currentValue).get());
                    }
                    break;
                case DOUBLE:
                    if (currentValue == null) {
                        rowBuilder.appendDouble(0d);
                    } else {
                        rowBuilder.appendDouble(((DoubleWritable) currentValue).get());
                    }
                    break;
                case VARCHAR:
                    if (currentValue == null) {
                        rowBuilder.appendString("");
                    } else {
                        Text v = (Text) currentValue;
                        rowBuilder.appendUTF8String(v.getBytes(), 0, v.getLength());
                    }
                    break;
                case DATE:
                    if (currentValue == null) {
                        rowBuilder.appendLong(0);
                    } else {
                        rowBuilder.appendLong(DateTimeUtil.getEpochMillisecond(((DateWritable) currentValue).get()));
                    }
                    break;
                case DATETIME:
                    if (currentValue == null) {
                        rowBuilder.appendLong(0);
                    } else {
                        rowBuilder.appendLong(DateTimeUtil.getEpochMillisecond(((TimestampWritable) currentValue).getTimestamp()));
                    }
                    break;
                default:
                    throw new IOException("can't recognize this type [" + type + "]");
            }
        }
        segmentGen.add(rowBuilder.buildAndReset());
    }

    @Override
    public void close(boolean abort) throws IOException {
        DPSegment segment = null;
        try {
            segment = segmentGen.gen();
            rowBuilder = null;
            if (!abort) {
                if (segment.rowCount() == 0) {
                    // Only create an empty file.
                    // We cannot just ignore this as hive will complain.
                    IOUtils.closeQuietly(fileSystem.create(segmentOutPath));
                } else {
                    SegmentHelper.uploadSegment(segment, fileSystem, segmentOutPath, tableLocation);
                }
            }
        } finally {
            IOUtils.closeQuietly(segment);
            // Remove temporary dir.
            FileUtils.deleteDirectory(localSegmentPath.toFile());
        }
    }

    @Override
    public void write(Void key, ArrayWritable value) throws IOException {}

    @Override
    public void close(Reporter reporter) throws IOException {
        close(true);
    }

    private static interface SegmentGen {
        void add(Row row) throws IOException;

        DPSegment gen() throws IOException;
    }
}

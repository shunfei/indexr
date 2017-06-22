package org.apache.spark.sql.execution.datasources.indexr;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.OutputWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;

import io.indexr.segment.ColumnSchema;
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
import io.indexr.util.Try;


public class IndexROutputWriter extends OutputWriter {
    private static final Logger logger = LoggerFactory.getLogger(IndexROutputWriter.class);

    private SegmentGen segmentGen;
    private SQLType[] sqlTypes;
    private SimpleRow.Builder rowBuilder;
    private FileSystem fileSystem;
    private java.nio.file.Path localSegmentPath;
    private String segmentName;

    private Path segmentOutPath;

    public IndexROutputWriter(Path finalOutPath,
                              Configuration configuration,
                              SegmentSchema schema,
                              SegmentMode mode,
                              AggSchema aggSchema) throws IOException {
        this.fileSystem = finalOutPath.getFileSystem(configuration);

        this.localSegmentPath = Files.createTempDirectory("_segment_gen_");
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
                public void add(io.indexr.segment.Row row) throws IOException {
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
                public void add(io.indexr.segment.Row row) throws IOException {
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
    public void write(Row row) {
        throw new UnsupportedOperationException("call writeInternal");
    }

    @Override
    public void writeInternal(InternalRow row) {
        for (int colId = 0; colId < sqlTypes.length; colId++) {
            SQLType type = sqlTypes[colId];
            switch (type) {
                case INT:
                    if (row.isNullAt(colId)) {
                        rowBuilder.appendInt(0);
                    } else {
                        rowBuilder.appendInt(row.getInt(colId));
                    }
                    break;
                case BIGINT:
                    if (row.isNullAt(colId)) {
                        rowBuilder.appendLong(0L);
                    } else {
                        rowBuilder.appendLong(row.getLong(colId));
                    }
                    break;
                case FLOAT:
                    if (row.isNullAt(colId)) {
                        rowBuilder.appendFloat(0f);
                    } else {
                        rowBuilder.appendFloat(row.getFloat(colId));
                    }
                    break;
                case DOUBLE:
                    if (row.isNullAt(colId)) {
                        rowBuilder.appendDouble(0d);
                    } else {
                        rowBuilder.appendDouble(row.getDouble(colId));
                    }
                    break;
                case VARCHAR:
                    if (row.isNullAt(colId)) {
                        rowBuilder.appendString("");
                    } else {
                        rowBuilder.appendUTF8String(row.getUTF8String(colId));
                    }
                    break;
                case DATE:
                    if (row.isNullAt(colId)) {
                        rowBuilder.appendLong(0);
                    } else {
                        int days = row.getInt(colId);
                        rowBuilder.appendLong(days * DateTimeUtil.MILLIS_PER_DAY);
                    }
                    break;
                case DATETIME:
                    if (row.isNullAt(colId)) {
                        rowBuilder.appendLong(0);
                    } else {
                        // microsecond in spark, indexr only have millisecond.
                        rowBuilder.appendLong(row.getLong(colId) / 1000);
                    }
                    break;
                default:
                    throw new RuntimeException("can't recognize this type [" + type + "]");
            }
        }
        try {
            segmentGen.add(rowBuilder.buildAndReset());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        DPSegment segment = null;
        try {
            segment = segmentGen.gen();
            rowBuilder = null;
            if (segment.rowCount() == 0) {
                // Only create an empty file.
                // We cannot just ignore this as hive will complain.
                IOUtils.closeQuietly(fileSystem.create(segmentOutPath));
            } else {
                SegmentHelper.uploadSegment(segment, fileSystem, segmentOutPath);
            }
        } catch (Exception e) {
            logger.error("write to {} failed", segmentOutPath, e);
        } finally {
            IOUtils.closeQuietly(segment);
            // Remove temporary dir.
            Try.on(() -> FileUtils.deleteDirectory(localSegmentPath.toFile()), logger);
        }
    }

    private static interface SegmentGen {
        void add(io.indexr.segment.Row row) throws IOException;

        DPSegment gen() throws IOException;
    }
}

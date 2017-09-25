package org.apache.spark.sql.execution.datasources.indexr;


import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructType$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import io.indexr.io.ByteBufferReader;
import io.indexr.segment.ColumnSchema;
import io.indexr.segment.RSValue;
import io.indexr.segment.Segment;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.rc.RCOperator;
import io.indexr.segment.storage.CachedSegment;
import io.indexr.segment.storage.itg.IntegratedSegment;
import io.indexr.util.BitMap;
import io.indexr.util.IOUtil;
import io.indexr.util.Trick;
import scala.collection.JavaConversions;

public class IndexRRecordReader extends RecordReader<Void, Object> {
    private static final Logger logger = LoggerFactory.getLogger(IndexRRecordReader.class);
    private static final MemoryMode DEFAULT_MEM_MODE = MemoryMode.OFF_HEAP; // We found that off heap has better performance.
    private static final int BATCH_SIZE = DataPack.MAX_COUNT;

    private Segment segment;
    private RCOperator rsFilter;
    private boolean[] validPacks;

    private StructType sparkSchema;
    private ColumnSchema[] projectColumns;
    private int[] projectColIdsInSegment;
    private boolean[] projectExists;

    private long totalRowCount;
    private int packCount;

    private int curPackId = 0;
    private int curRowIdInPack = 0;
    private long rowsReturned = 0;

    private PackReader packReader;

    private boolean returnColumnarBatch = false;
    private ColumnarBatch columnarBatch;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException, UnsupportedOperationException {
        Configuration configuration = taskAttemptContext.getConfiguration();
        FileSplit split = (FileSplit) inputSplit;
        Preconditions.checkState(split.getStart() == 0, "Segment should not splited");

        Path filePath = split.getPath();

        String sparkRequestedSchemaString = configuration.get(Config.SPARK_PROJECT_SCHEMA);
        this.sparkSchema = StructType$.MODULE$.fromString(sparkRequestedSchemaString);

        FileSystem fileSystem = filePath.getFileSystem(taskAttemptContext.getConfiguration());
        if (!SegmentHelper.checkSegmentByPath(filePath)) {
            logger.info("ignore: " + filePath);
            return;
        }

        ByteBufferReader.Opener opener = ByteBufferReader.Opener.create(fileSystem, filePath);
        IntegratedSegment.Fd fd = IntegratedSegment.Fd.create(filePath.toString(), opener);
        if (fd == null) {
            logger.warn("illegal segment: " + filePath);
            return;
        }
        segment = fd.open();
        if (segment == null) {
            logger.warn("illegal segment: " + filePath);
            return;
        }
        this.totalRowCount = segment.rowCount();
        this.packCount = segment.packCount();

        SegmentSchema segmentSchema = segment.schema();
        this.projectColumns = IndexRUtil.sparkSchemaToIndexRSchema(
                JavaConversions.seqAsJavaList(sparkSchema),
                a -> false)
                .getColumns().toArray(new ColumnSchema[0]);
        this.projectColIdsInSegment = new int[projectColumns.length];
        this.projectExists = new boolean[projectColumns.length];
        for (int i = 0; i < projectColumns.length; i++) {
            String columnName = projectColumns[i].getName();
            int colId = Trick.indexWhere(segmentSchema.getColumns(), cs -> cs.getName().equalsIgnoreCase(columnName));
            projectColIdsInSegment[i] = colId;
            projectExists[i] = colId >= 0;
        }
    }


    // Creates a columnar batch that includes the schema from the data files and the additional
    // partition columns appended to the end of the batch.
    // For example, if the data contains two columns, with 2 partition columns:
    // Columns 0,1: data columns
    // Column 2: partitionValues[0]
    // Column 3: partitionValues[1]
    public void init2(StructType partitionColumns,
                      InternalRow partitionValues,
                      boolean returnBatch,
                      RCOperator rsFilter) throws IOException {
        if (segment == null) {
            return;
        }
        this.returnColumnarBatch = returnBatch;
        this.rsFilter = rsFilter;

        if (rsFilter != null) {
            rsFilter.materialize(segment.schema().getColumns());
            byte res = rsFilter.roughCheckOnColumn(segment);
            if (res == RSValue.None) {
                close();
                return;
            }
            int packCount = segment.packCount();
            boolean[] validPacks = new boolean[packCount];
            for (int packId = 0; packId < packCount; packId += 1) {
                res = rsFilter.roughCheckOnPack(segment, packId);
                validPacks[packId] = res != RSValue.None;
            }
            if (Trick.hasOne(validPacks)) {
                BitMap map = rsFilter.exactCheckOnPack(segment);
                for (int packId = 0; packId < packCount; packId += 1) {
                    validPacks[packId] &= map.get(packId);
                }
                map.free();
                if (!Trick.hasOne(validPacks)) {
                    close();
                    return;
                }
            }
            this.validPacks = validPacks;
        }

        StructType batchSchema = new StructType();
        for (StructField f : sparkSchema.fields()) {
            batchSchema = batchSchema.add(f);
        }
        if (partitionColumns != null) {
            for (StructField f : partitionColumns.fields()) {
                batchSchema = batchSchema.add(f);
            }
        }

        columnarBatch = ColumnarBatch.allocate(batchSchema, DEFAULT_MEM_MODE, BATCH_SIZE);
        if (partitionColumns != null) {
            int partitionIdx = sparkSchema.fields().length;
            for (int i = 0; i < partitionColumns.fields().length; i++) {
                ColumnVectorUtils.populate(columnarBatch.column(i + partitionIdx), partitionValues, i);
                columnarBatch.column(i + partitionIdx).setIsConstant();
            }
        }

        // Initialize missing columns with nulls.
        for (int i = 0; i < projectExists.length; i++) {
            if (!projectExists[i]) {
                columnarBatch.column(i).putNulls(0, columnarBatch.capacity());
                columnarBatch.column(i).setIsConstant();
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (columnarBatch != null) {
            columnarBatch.close();
            columnarBatch = null;
        }
        if (packReader != null) {
            packReader.clear();
            packReader = null;
        }
        if (segment != null) {
            IOUtil.closeQuietly(segment);
            segment = null;
        }
    }

    private boolean checkAndFillBatch() throws IOException {
        while (packReader == null || !packReader.hasMore()) {
            if (curPackId >= packCount) {
                return false;
            }
            int myPackId = curPackId;
            curPackId++;

            if (validPacks != null && !validPacks[myPackId]) {
                continue;
            }
            CachedSegment cachedSegment = new CachedSegment(segment);
            if (rsFilter != null) {
                BitMap bitMap = rsFilter.exactCheckOnRow(cachedSegment, myPackId);
                if (bitMap.isEmpty()) {
                    cachedSegment.free();
                    bitMap.free();
                    continue;
                }
                bitMap.free();
            }

            packReader = new DefaultPackReader(
                    cachedSegment,
                    myPackId,
                    projectColumns,
                    projectColIdsInSegment,
                    columnarBatch.capacity());
        }

        columnarBatch.reset();
        rowsReturned += packReader.read(columnarBatch);

        return true;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (segment == null) {
            return false;
        }
        if (returnColumnarBatch) {
            // By patch.
            return checkAndFillBatch();
        } else {
            // By row.
            if (curRowIdInPack >= columnarBatch.numRows()) {
                if (checkAndFillBatch()) {
                    curRowIdInPack = 0;
                    return true;
                } else {
                    return false;
                }
            } else {
                curRowIdInPack++;
                return true;
            }
        }
    }

    @Override
    public Void getCurrentKey() throws IOException, InterruptedException {return null;}

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
        if (returnColumnarBatch) return columnarBatch;
        return columnarBatch.getRow(curRowIdInPack);
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (float) rowsReturned / totalRowCount;
    }
}

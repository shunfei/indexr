package io.indexr.hive;

import com.google.common.base.Preconditions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.directory.api.util.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import io.indexr.io.ByteBufferReader;
import io.indexr.segment.ColumnSchema;
import io.indexr.segment.Row;
import io.indexr.segment.Segment;
import io.indexr.segment.pack.IntegratedSegment;
import io.indexr.util.DateTimeUtil;
import io.indexr.util.Trick;

public class IndexRRecordReader implements RecordReader<Void, SchemaWritable> {
    private static final Log LOG = LogFactory.getLog(IndexRRecordReader.class);

    private Segment segment;
    private Iterator<Row> rowIterator;
    private long offset; // Current row id offset.

    private ColumnSchema[] projectCols;
    private int[] projectColIds;

    public IndexRRecordReader(InputSplit inputSplit, Configuration configuration) throws IOException {
        FileSplit fileSplit = (FileSplit) inputSplit;
        Preconditions.checkState(fileSplit.getStart() == 0, "Segment should not splited");

        Path filePath = fileSplit.getPath();
        // Hive may ask to read a file located on local file system.
        // We have to get the real file system by path's schema.
        FileSystem fileSystem = FileSystem.get(filePath.toUri(), FileSystem.get(configuration).getConf());

        if (SegmentHelper.checkSegmentByPath(filePath)) {
            ByteBufferReader.Opener opener = ByteBufferReader.Opener.create(fileSystem, filePath);
            IntegratedSegment.Fd fd = IntegratedSegment.Fd.create(filePath.toString(), opener);
            if (fd != null) {
                segment = fd.open();
                offset = 0L;
                rowIterator = segment.rowTraversal().iterator();
                getIncludeColumns(configuration, segment);
            }
        } else {
            LOG.warn("ignore " + filePath);
        }
    }


    private void getIncludeColumns(Configuration conf, Segment segment) {
        List<ColumnSchema> segColSchemas = segment.schema().getColumns();
        String columnNamesStr = conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR);
        if (ColumnProjectionUtils.isReadAllColumns(conf) ||
                columnNamesStr == null) {
            projectCols = new ColumnSchema[segColSchemas.size()];
            projectColIds = new int[segColSchemas.size()];
            for (int i = 0; i < segColSchemas.size(); i++) {
                projectCols[i] = segColSchemas.get(i);
                projectColIds[i] = i;
            }
        } else {
            String[] ss = Strings.isEmpty(columnNamesStr.trim()) ? new String[]{} : columnNamesStr.split(",");
            projectCols = new ColumnSchema[ss.length];
            projectColIds = new int[ss.length];
            for (int i = 0; i < ss.length; i++) {
                String col = ss[i];
                int colId = Trick.indexFirst(segColSchemas, c -> c.getName().equalsIgnoreCase(col));
                Preconditions.checkState(colId >= 0, String.format("Column [%s] not found in segment [%s]", col, segment.name()));
                projectCols[i] = segColSchemas.get(colId);
                projectColIds[i] = colId;
            }
        }
    }

    @Override
    public boolean next(Void aVoid, SchemaWritable writable) throws IOException {
        if (segment == null) {
            return false;
        }
        if (offset >= segment.rowCount()) {
            return false;
        }

        Row current = rowIterator.next();
        Writable[] writables = new Writable[projectCols.length];
        for (int i = 0; i < projectCols.length; i++) {
            ColumnSchema columnSchema = projectCols[i];
            int colId = projectColIds[i];
            switch (columnSchema.getSqlType()) {
                case INT:
                    writables[i] = new IntWritable(current.getInt(colId));
                    break;
                case BIGINT:
                    writables[i] = new LongWritable(current.getLong(colId));
                    break;
                case FLOAT:
                    writables[i] = new FloatWritable(current.getFloat(colId));
                    break;
                case DOUBLE:
                    writables[i] = new DoubleWritable(current.getDouble(colId));
                    break;
                case VARCHAR:
                    writables[i] = new BytesWritable(current.getString(colId).getBytes());
                    break;
                case DATE:
                    writables[i] = new DateWritable(DateTimeUtil.getJavaSQLDate(current.getLong(colId)));
                    break;
                case DATETIME:
                    writables[i] = new TimestampWritable(DateTimeUtil.getJavaSQLTimeStamp(current.getLong(colId)));
                    break;
                default:
                    throw new IllegalStateException("Illegal type: " + columnSchema.getSqlType());
            }
        }

        offset++;

        // Must set it every fucking time, as writable could be reused between segments!
        writable.columns = projectCols;
        writable.set(writables);

        return true;
    }

    @Override
    public Void createKey() {
        return null;
    }

    @Override
    public SchemaWritable createValue() {
        return new SchemaWritable();
    }

    @Override
    public long getPos() throws IOException {
        return offset;
    }

    @Override
    public void close() throws IOException {
        IOUtils.cleanup(LOG, segment);
    }

    @Override
    public float getProgress() throws IOException {
        if (segment == null) {
            return 0;
        }
        return (float) (offset + 1) / segment.rowCount();
    }
}

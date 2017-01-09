package io.indexr.segment.pack;

import com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import io.indexr.segment.ColumnType;
import io.indexr.segment.Row;
import io.indexr.segment.RowTraversal;
import io.indexr.segment.SegmentSchema;
import io.indexr.util.JsonUtil;

/**
 * DPSegment is a segment which only resides on local disk. It is column based, supports rows literal, ingestion, and merge.
 * 
 * Usually DPSegment is used as an intermediate format. You can create a new one, ingest rows into it, merge with another segments,
 * maybe check correctness by literal over it. After you done play with it, transform into {@link IntegratedSegment}.
 * 
 * Dir structure:
 * <pre>
 * .../segment/path/
 *                  metadata.json
 *                  schema.json
 *                  colName0.pack
 *                  colName0.dpn
 *                  colName0.index
 *                  colName1.pack
 *                  colName1.dpn
 *                  colName1.index
 *                  ...
 * </pre>
 * 
 * An updating DPSegment can not be quried, i.e. those methods like {@link #rowTraversal()},
 * {@link #column(int)} will return <code>null</code> while {@link #isUpdate()} return <code>true</code>.
 * 
 * This class is <b>NOT</b> multi-thread safe.
 */
public class DPSegment extends StorageSegment<DPColumn> {
    private static final Logger logger = LoggerFactory.getLogger(DPSegment.class);

    private final Path segmentPath;
    private boolean update;

    DPSegment(int version, Path segmentPath, String name, SegmentSchema schema, long rowCount) throws IOException {
        super(version, name, schema, rowCount, (ci, sc, rc) -> new DPColumn(version, ci, sc.name, sc.dataType, rc, segmentPath));
        this.segmentPath = segmentPath;
    }

    /**
     * A simple structure used to pack metadata of DPSegment, convenient for JSON ser/desr.
     */
    private static class Metadata {
        @JsonProperty("version")
        public int version;
        @JsonProperty("rowCount")
        public long rowCount;

        @JsonCreator
        public Metadata(@JsonProperty("version") int version,
                        @JsonProperty("rowCount") long rowCount) {
            this.version = version;
            this.rowCount = rowCount;
        }
    }

    /**
     * Open a DPSegment, same as <code>open(path, null, null)</code>
     */
    public static DPSegment open(String path) throws IOException {
        return open(path, null, null);
    }

    /**
     * Open a DPSegment, same as <code>open(path, null, null)</code>
     */
    public static DPSegment open(Path path) throws IOException {
        return open(Version.LATEST_ID, path, null, null);
    }

    public static DPSegment open(String path, String name, SegmentSchema schema, OpenOption... options) throws IOException {
        return open(Version.LATEST_ID, Paths.get(path), name, schema, options);
    }

    /**
     * Open a DPSegment.
     * 
     * The newly open segment is default in compressed state, and cannot be updated.
     * You can Use {@link #setCompress(boolean)} to set compress status of next pack and {@link #update()} to enable update.
     *
     * @param version The segment version, check {@link Version}.
     * @param path    The path of segment.
     * @param name    The unique segment identifier in the whole system.
     * @param schema  The schema of segment.
     * @param options The open options.
     * @return A segment resides on the path.
     */
    public static DPSegment open(int version, Path path, String name, SegmentSchema schema, OpenOption... options) throws IOException {
        //Preconditions.checkArgument(name != null);
        Preconditions.checkArgument(path != null);

        if (OpenOption.Overwrite.in(options)) {
            FileUtils.deleteDirectory(path.toFile());
        }

        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }

        Metadata metadata;
        SegmentSchema segmentSchema;

        Path metadataPath = path.resolve("metadata.json");
        Path schemaPath = path.resolve("schema.json");
        if (Files.exists(metadataPath) && Files.exists(schemaPath)) {
            metadata = JsonUtil.loadWithRE(metadataPath, Metadata.class);
            segmentSchema = JsonUtil.loadWithRE(schemaPath, SegmentSchema.class);

            if (schema != null) {
                Preconditions.checkState(segmentSchema.equals(schema), "Provided segment schema and the current one not match!");
            }
        } else {
            Preconditions.checkArgument(schema != null, "Segment schema should be provided while not exists yet!");
            metadata = new Metadata(version, 0);
            segmentSchema = schema;

            saveInfo(path, metadata, segmentSchema);
        }

        return new DPSegment(metadata.version, path, name, segmentSchema, metadata.rowCount);
    }

    private static void saveInfo(Path segmentPath, Metadata metadata, SegmentSchema schema) {
        JsonUtil.saveWithRE(segmentPath.resolve("metadata.json"), metadata);
        JsonUtil.saveWithRE(segmentPath.resolve("schema.json"), schema);
    }

    public Path path() {
        return segmentPath;
    }

    @Override
    public boolean isColumned() {
        return !update;
    }

    @Override
    public int packCount() {
        if (update) {
            return 0;
        }
        return super.packCount();
    }

    @Override
    public ColumnNode columnNode(int colId) throws IOException {
        if (update) {
            return null;
        }
        return super.columnNode(colId);
    }

    @Override
    public DPColumn column(int colId) {
        if (update) {
            return null;
        }
        return super.column(colId);
    }

    @Override
    public RowTraversal rowTraversal(long offset, long count) {
        if (update) {
            return null;
        }
        return super.rowTraversal(offset, count);
    }

    // ------------------------------------------------------------------
    // Update stuff
    // ------------------------------------------------------------------

    /**
     * Ingest one row.
     */
    public void add(Row row) throws IOException {
        Preconditions.checkState(rowCount < MAX_ROW_COUNT,
                "Try to ingest too many rows into one segment, limit is %s, ingested %s",
                MAX_ROW_COUNT, rowCount);
        Preconditions.checkState(update, "This segment is immutable for now!");

        for (int colId = 0; colId < columns.size(); colId++) {
            DPColumn column = columns.get(colId);
            switch (segmentSchema.columns.get(colId).dataType) {
                case ColumnType.INT:
                    column.add(row.getInt(colId));
                    break;
                case ColumnType.LONG:
                    column.add(row.getLong(colId));
                    break;
                case ColumnType.FLOAT:
                    column.add(row.getFloat(colId));
                    break;
                case ColumnType.DOUBLE:
                    column.add(row.getDouble(colId));
                    break;
                case ColumnType.STRING:
                    column.add(row.getString(colId).clone());
                    break;
                default:
                    throw new IllegalStateException();
            }
        }

        rowCount++;
    }

    public DPSegment setCompress(boolean compress) {
        for (DPColumn column : columns) {
            column.setCompress(compress);
        }
        return this;
    }

    public boolean isUpdate() {
        return update;
    }

    /**
     * Indicate that this segment can be updated now.
     * It will initialize the resources like open files, set up caches.
     */
    public DPSegment update() throws IOException {
        if (update) {
            return this;
        }
        // Remove outdate info.
        for (int i = 0; i < columnNodes.length; i++) {
            columnNodes[i] = null;
        }
        for (DPColumn col : columns) {
            col.initUpdate();
        }
        update = true;
        return this;
    }

    /**
     * End up update this Segment. Flush the cache to file and close the open files.
     * After this call, any ingesting operation will rise exceptions.
     */
    public void seal() throws IOException {
        if (!update) {
            return;
        }
        saveInfo(segmentPath, new Metadata(version(), rowCount), segmentSchema);
        for (DPColumn col : columns) {
            col.seal();
        }
        update = false;
    }

    @Override
    public void close() throws IOException {
        seal();
        super.close();
    }

    // ------------------------------------------------------------------
    // Merge stuff
    // ------------------------------------------------------------------

    public void merge(List<StorageSegment> segments) throws IOException {
        if (segments.isEmpty()) {
            return;
        }
        long rowCount = this.rowCount();
        for (StorageSegment segment : segments) {
            for (DPColumn column : columns) {
                boolean ok = false;
                for (Object oc : segment.columns) {
                    StorageColumn otherCol = (StorageColumn) oc;
                    if (StringUtils.equals(column.name(), otherCol.name()) && column.dataType() == otherCol.dataType()) {
                        ok = true;
                    }
                }
                if (!ok) {
                    throw new IllegalStateException(String.format("segment with %s cannot merge into %s", segment.schema(), segmentSchema));
                }
            }
            rowCount += segment.rowCount();
        }

        update();

        for (DPColumn column : columns) {
            List<StorageColumn> toMerge = new ArrayList<>(segments.size());
            for (StorageSegment otherSeg : segments) {
                StorageColumn match = null;
                for (Object oc : otherSeg.columns) {
                    StorageColumn otherCol = (StorageColumn) oc;
                    if (StringUtils.equals(column.name(), otherCol.name()) && column.dataType() == otherCol.dataType()) {
                        match = otherCol;
                    }
                }
                toMerge.add(match);
            }
            column.merge(toMerge);
        }
        this.rowCount = rowCount;

        seal();
    }
}

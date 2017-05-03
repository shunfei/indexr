package io.indexr.segment.storage;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import io.indexr.segment.Row;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.rt.EventIgnoreStrategy;
import io.indexr.segment.rt.Metric;
import io.indexr.segment.rt.RTSMerge;
import io.indexr.segment.rt.UTF8Row;
import io.indexr.segment.storage.itg.IntegratedSegment;
import io.indexr.util.Try;

public class SortedSegmentGenerator {
    private static Logger logger = LoggerFactory.getLogger(SortedSegmentGenerator.class);

    private final int version;
    private final SegmentMode mode;
    private final Path path;
    private final String name;
    private final SegmentSchema schema;
    private final int split;

    private final UTF8Row.Creator utf8Creator;

    private SortedMap<UTF8Row, UTF8Row> currentChunk;
    private int currentChunkId;

    private List<Path> dumpedSegmentPaths;

    public SortedSegmentGenerator(int version,
                                  SegmentMode mode,
                                  Path path,
                                  String name,
                                  SegmentSchema schema,
                                  boolean grouping,
                                  List<String> dims,
                                  List<Metric> metrics,
                                  int split) throws IOException {
        this.version = version;
        this.mode = mode;
        this.path = path;
        this.name = name;
        this.schema = schema;
        this.split = split;
        this.utf8Creator = new UTF8Row.Creator(
                grouping,
                schema.getColumns(),
                dims,
                metrics,
                null,
                null,
                EventIgnoreStrategy.NO_IGNORE
        );

        this.currentChunk = createSortedSet();
        this.currentChunkId = 0;
        this.dumpedSegmentPaths = new ArrayList<>();

        if (Files.exists(path)) {
            FileUtils.deleteDirectory(path.toFile());
        }
    }

    private static SortedMap<UTF8Row, UTF8Row> createSortedSet() {
        return new TreeMap<>(UTF8Row.dimBytesComparator());
    }

    public void add(Row row) throws IOException {
        UTF8Row utf8Row = UTF8Row.from(utf8Creator, row);
        UTF8Row oldRow = currentChunk.get(utf8Row);
        if (oldRow != null) {
            oldRow.merge(utf8Row);
            utf8Row.free();
        } else {
            currentChunk.put(utf8Row, utf8Row);
            if (currentChunk.size() >= split) {
                split();
            }
        }
    }

    public DPSegment seal() throws IOException {
        split();
        return merge();
    }

    private void split() throws IOException {
        dumpAndFree(currentChunkId, currentChunk);
        currentChunk = createSortedSet();
        currentChunkId++;
    }

    private void dumpAndFree(int id, SortedMap<UTF8Row, UTF8Row> rows) throws IOException {
        DPSegment segment = null;
        String name = "_chunk_" + id;
        Path segmentPath = path.resolve(name);
        try {
            segment = DPSegment.open(version, SegmentMode.FAST, segmentPath, name, schema, OpenOption.Overwrite);
            segment.update();

            for (UTF8Row row : rows.values()) {
                segment.add(row);
                row.free();
            }
            rows.clear();

            segment.seal();

            Path integratedPath = path.resolve(name + ".integrated");
            IntegratedSegment.Fd.create(segment, integratedPath, false);

            dumpedSegmentPaths.add(integratedPath);
        } finally {
            FileUtils.deleteDirectory(segmentPath.toFile());
            IOUtils.closeQuietly(segment);
        }
    }

    private DPSegment merge() throws IOException {
        DPSegment mergeSegment = null;
        List<IntegratedSegment> segments = new ArrayList<>();
        try {
            for (Path path : dumpedSegmentPaths) {
                segments.add(IntegratedSegment.Fd.create(path.toString(), path).open());
            }

            mergeSegment = DPSegment.open(version, mode, path, name, schema);
            mergeSegment.update();

            RTSMerge.sortedMerge(utf8Creator, mergeSegment, segments);
        } finally {
            for (IntegratedSegment segment : segments) {
                IOUtils.closeQuietly(segment);
            }
            for (Path path : dumpedSegmentPaths) {
                FileUtils.deleteQuietly(path.toFile());
            }
            dumpedSegmentPaths.clear();

            if (mergeSegment != null) {
                DPSegment fs = mergeSegment;
                Try.on(fs::seal, logger);
            }
        }
        return mergeSegment;
    }
}

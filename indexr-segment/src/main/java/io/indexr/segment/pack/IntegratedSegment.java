package io.indexr.segment.pack;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.indexr.io.ByteBufferReader;
import io.indexr.io.ByteBufferWriter;
import io.indexr.segment.ColumnSchema;
import io.indexr.segment.InfoSegment;
import io.indexr.segment.SegmentFd;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.pack.Integrated.ColumnInfo;
import io.indexr.segment.pack.Integrated.ColumnNodeInfo;
import io.indexr.segment.pack.Integrated.SectionInfo;

/**
 * An integrated segment combines all its data into a single file, including schema, index and data.
 */
public class IntegratedSegment extends StorageSegment<IntegratedColumn> {
    private Closeable close;

    IntegratedSegment(int version,
                      String name,
                      SegmentSchema schema,
                      long rowCount,
                      ColumnNode[] columnNodes,
                      StorageColumnCreator<IntegratedColumn> columnCreator,
                      Closeable close) throws IOException {
        super(version, name, schema, rowCount, columnCreator);
        // Set the columnNodes here and never change.
        this.columnNodes = columnNodes;
        this.close = close;
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (close != null) {
            close.close();
            close = null;
        }
    }

    public static class Fd implements SegmentFd {
        private final long segmentId;
        private final StorageInfoSegment infoSegment;
        private final ByteBufferReader.Opener dataSource;

        // Hold it here, so that we can dump it to local cache directly.
        private final SectionInfo sectionInfo;

        private DataPackNode[][] _dpns;
        // We cache the dpn info inside fd, to fast filter out segments without reading any data from segment file.
        private DpnCache dpnCache;

        Fd(String name, SectionInfo sectionInfo, ByteBufferReader.Opener dataSource) {
            ColumnNode[] columnNodes = new ColumnNode[sectionInfo.columnCount];
            List<ColumnSchema> columnSchemas = new ArrayList<>(sectionInfo.columnCount);
            for (int i = 0; i < sectionInfo.columnCount; i++) {
                ColumnNodeInfo cni = sectionInfo.columnNodeInfos[i];
                columnNodes[i] = new ColumnNode(cni.minNumValue, cni.maxNumValue);

                ColumnInfo ci = sectionInfo.columnInfos[i];
                columnSchemas.add(new ColumnSchema(ci.name, ci.dataType));
            }

            this.segmentId = MemCache.nextSegmentId();
            this.infoSegment = new StorageInfoSegment(
                    name,
                    sectionInfo.rowCount,
                    new SegmentSchema(columnSchemas),
                    columnNodes);
            this.dataSource = dataSource;

            this.sectionInfo = sectionInfo;
            this._dpns = new DataPackNode[sectionInfo.columnCount][];
            this.dpnCache = new DpnCache() {
                @Override
                public DataPackNode[] get(int columnId) {
                    return _dpns[columnId];
                }

                @Override
                public void put(int columnId, DataPackNode[] dpns) {
                    _dpns[columnId] = dpns;
                }
            };
        }

        public SectionInfo sectionInfo() {
            return sectionInfo;
        }

        @Override
        public String name() {
            return infoSegment.name();
        }

        @Override
        public InfoSegment info() {
            return infoSegment;
        }

        public IntegratedSegment open() throws IOException {
            return open(null, null);
        }

        @Override
        public IntegratedSegment open(IndexMemCache indexMemCache, PackMemCache packMemCache) throws IOException {
            // Open file here. The user will close it by Segment#close().
            ByteBufferReader reader = dataSource.open(0);
            // Create a wrapped reader, all open peration will directly return the opening file.
            ByteBufferReader.Opener wrappedDataSource = ByteBufferReader.Opener.create(reader);

            StorageSegment.StorageColumnCreator<IntegratedColumn> columnCreator = (ci, sc, rc) -> {
                ColumnInfo info = sectionInfo.columnInfos[ci];
                return new IntegratedColumn(
                        sectionInfo.version,
                        segmentId,
                        ci,
                        sc.name,
                        sc.dataType,
                        rc,
                        wrappedDataSource,
                        info.dpnOffset,
                        info.indexOffset,
                        info.packOffset,
                        dpnCache,
                        indexMemCache,
                        packMemCache);
            };
            return new IntegratedSegment(
                    sectionInfo.version,
                    infoSegment.name,
                    infoSegment.schema,
                    infoSegment.rowCount,
                    infoSegment.columnNodes,
                    columnCreator,
                    reader);
        }

        /**
         * Integrate a segment into an {@link IntegratedSegment}.
         *
         * @param segment The segment to integrate.
         * @param path    The destination file which generated segment will write into.
         * @param openFd  Open segment fd or not.
         * @return A {@link Fd} pointed to the integrated segment.
         */
        public static Fd create(StorageSegment segment, Path path, boolean openFd) throws IOException {
            ByteBufferWriter.PredictSizeOpener opener = size -> {
                FileChannel file = FileChannel.open(path,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING);
                ByteBufferWriter writer = ByteBufferWriter.of(file, 0);
                writer.setName(path.toString());
                return writer;
            };
            ByteBufferReader.Opener reader = openFd ? ByteBufferReader.Opener.create(path) : null;
            return create(segment, opener, reader);
        }

        /**
         * Integrate a segment into an {@link IntegratedSegment}.
         *
         * @param segment      The segment to integrate.
         * @param writerOpener The destination file which generated segment will write into.
         * @param dstReader    The same file of <i>write</i>, used to generate the returned segment fd.
         *                     It could be <i>null</i> if you are not plan to really open the segment by {@link SegmentFd#open(IndexMemCache, PackMemCache)}.
         *                     You can create the same segment fd later by {@link IntegratedSegment.Fd#create(String, ByteBufferReader.Opener)}.
         * @return A {@link Fd} pointed to the integrated segment.
         */
        public static Fd create(StorageSegment segment,
                                ByteBufferWriter.PredictSizeOpener writerOpener,
                                ByteBufferReader.Opener dstReader) throws IOException {
            SectionInfo sectionInfo = Integrated.write(segment, writerOpener);
            return IntegratedSegment.Fd.create(segment.name, sectionInfo, dstReader);
        }

        public static Fd create(String name, Path path) throws IOException {
            return create(name, ByteBufferReader.Opener.create(path));
        }

        /**
         * Create an {@link IntegratedSegment} from specific file.
         *
         * @param name       The name of this segment.
         * @param dataSource The file where segment resides.
         * @return A {@link Fd} pointed to the file.
         */
        public static Fd create(String name, ByteBufferReader.Opener dataSource) throws IOException {
            SectionInfo sectionInfo;
            try (ByteBufferReader reader = dataSource.open(0)) {
                sectionInfo = Integrated.read(reader);
            }
            if (sectionInfo == null) {
                return null;
            }
            return create(name, sectionInfo, dataSource);
        }

        /**
         * Create a {@link IntegratedSegment} from an opening data source.
         * 
         * It will be the creator's responsibility to close the data source.
         *
         * @param name       The name of segment.
         * @param dataSource An opening data source.
         * @return A {@link Fd} pointed to the data source.
         */
        public static Fd create(String name, ByteBufferReader dataSource) throws IOException {
            SectionInfo sectionInfo = Integrated.read(dataSource);
            if (sectionInfo == null) {
                return null;
            }
            return create(name, sectionInfo, ByteBufferReader.Opener.create(dataSource));
        }

        /**
         * Create an {@link IntegratedSegment} with the {@link SectionInfo} already provided.
         *
         * @param name        The name of segment.
         * @param sectionInfo The section info.
         * @param dataSource  The file where segment resides.
         * @return A {@link Fd} pointed to the file.
         */
        public static Fd create(String name, SectionInfo sectionInfo, ByteBufferReader.Opener dataSource) {
            return new Fd(name, sectionInfo, dataSource);
        }

        // Only for test
        public static List<SegmentFd> loadFromLocalCache(Path cachePath, BBROCreator creator) throws IOException {
            Map<String, SectionInfo> sectionInfos = SectionInfo.loadFromLocalFile(cachePath);
            List<SegmentFd> segmentFds = new ArrayList<>(sectionInfos.size());
            for (Map.Entry<String, SectionInfo> entry : sectionInfos.entrySet()) {
                String name = entry.getKey();
                SectionInfo si = entry.getValue();
                Fd fd = Fd.create(name, si, creator.create(name));
                segmentFds.add(fd);
            }
            return segmentFds;
        }

        // Only for test
        public static void saveToLocalCache(Path cachePath, List<? extends SegmentFd> segmentFds) throws IOException {
            Map<String, SectionInfo> sectionInfos = new HashMap<>(segmentFds.size());
            for (SegmentFd fd : segmentFds) {
                sectionInfos.put(fd.name(), ((Fd) fd).sectionInfo);
            }
            SectionInfo.saveToLocalFile(cachePath, sectionInfos);
        }

        @FunctionalInterface
        public static interface BBROCreator {
            ByteBufferReader.Opener create(String name);
        }
    }
}

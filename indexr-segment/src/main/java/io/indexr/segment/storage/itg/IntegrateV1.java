package io.indexr.segment.storage.itg;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import io.indexr.io.ByteBufferReader;
import io.indexr.io.ByteBufferWriter;
import io.indexr.io.ByteSlice;
import io.indexr.segment.PackExtIndex;
import io.indexr.segment.RSIndex;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.pack.DataPackNode;
import io.indexr.segment.storage.ColumnNode;
import io.indexr.segment.storage.StorageColumn;
import io.indexr.segment.storage.StorageSegment;
import io.indexr.segment.storage.Version;
import io.indexr.util.ObjectSaver;
import io.indexr.util.UTF8Util;

/**
 * Store segment column by column.
 *
 * <pre>
 * Segment storage structure:
 * |  IXRSEGxx | sementMeta | column0 | column1 ... |
 *
 * Column structure:
 * |  dpn | rsIndex | extIndex | dataPack |
 * </pre>
 */
public class IntegrateV1 implements Integrate {
    public static IntegrateV1 INSTANCE = new IntegrateV1();

    @Override
    public SegmentMeta write(StorageSegment segment, ByteBufferWriter.PredictSizeOpener writerOpener) throws IOException {
        List<StorageColumn> columns = segment.columns();
        SegmentMode mode = segment.mode();
        SegmentMetaV1 sectionInfo = new SegmentMetaV1(segment.version(), segment.rowCount(), columns.size(), mode.id);
        sectionInfo.sectionOffset = Version.INDEXR_SEG_FILE_FLAG_SIZE; // The first section, and the only one.

        for (int colId = 0; colId < columns.size(); colId++) {
            ColumnNode columnNode = segment.columnNode(colId);
            sectionInfo.columnNodeInfos[colId] = new ColumnNodeMeta(columnNode.getMinNumValue(), columnNode.getMaxNumValue());

            StorageColumn column = columns.get(colId);
            DataPackNode[] dpns = column.getDPNs();
            int dpnSize = mode.versionAdapter.dpnSize(segment.version(), mode) * dpns.length;
            int indexSize = 0;
            int extIndexSize = 0;
            long packSize = 0;
            for (DataPackNode dpn : dpns) {
                indexSize += dpn.indexSize();
                extIndexSize += dpn.extIndexSize();
                packSize += dpn.packSize();
            }

            // Hack!
            boolean isIndexed = (segment.version() >= Version.VERSION_7_ID && column.isIndexed());
            ColumnMetaV1 info = new ColumnMetaV1(column.name(), column.sqlType().id, isIndexed, dpnSize, indexSize, extIndexSize, packSize);
            sectionInfo.columnInfos[colId] = info;
        }

        // Calculate the whole size of this section.
        long dataSize = 0;
        for (ColumnMeta ci : sectionInfo.columnInfos) {
            dataSize += ((ColumnMetaV1) ci).dataSize();
        }
        // First 4 bytes are the size of info. See ObjSaver#save() for detail.
        sectionInfo.sectionSize = 4 + sectionInfo.metaSize() + dataSize;

        // Set the dataOffset.
        long offset = sectionInfo.sectionOffset + 4 + sectionInfo.metaSize();
        for (int colId = 0; colId < columns.size(); colId++) {
            ColumnMetaV1 columnInfo = (ColumnMetaV1) sectionInfo.columnInfos[colId];
            columnInfo.setDataOffset(segment.version(), offset);
            offset += columnInfo.dataSize();
        }

        assert offset - sectionInfo.sectionOffset == sectionInfo.sectionSize;

        long totalSize = offset;

        // Now everything is ready, let's move data into new segment.
        try (ByteBufferWriter writer = writerOpener.open(totalSize)) {
            // First write version message.
            writer.write(Version.fromId(segment.version()).flag);

            // Write info.
            ObjectSaver.save(writer, sectionInfo, sectionInfo.metaSize(), SegmentMetaV1::write);

            // Write data.
            for (StorageColumn column : columns) {
                DataPackNode[] dpns = column.getDPNs();
                for (DataPackNode dpn : dpns) {
                    dpn.write(writer);
                }

                RSIndex index = column.loadIndex();
                if (index != null) {
                    index.write(writer);
                    index.free();
                }

                for (DataPackNode dpn : dpns) {
                    PackExtIndex extIndex = column.loadExtIndex(dpn);
                    extIndex.write(writer);
                    extIndex.free();
                }

                for (DataPackNode dpn : dpns) {
                    ByteSlice pack = column.loadPack(dpn);
                    writer.write(pack.byteBuffer());
                    pack.free();
                }
            }

            writer.flush();

            return sectionInfo;
        }
    }

    @Override
    public SegmentMeta read(ByteBufferReader reader) throws IOException {
        ByteBuffer infoBuffer = null;
        Version version = Version.check(reader);
        if (version == null) {
            return null;
        }

        long sectionOffset = Version.INDEXR_SEG_FILE_FLAG_SIZE;
        SegmentMetaV1 sectionInfo = ObjectSaver.load(reader, sectionOffset, SegmentMetaV1::read);
        assert sectionInfo.version == version.id;
        return sectionInfo;
    }

    public static class SegmentMetaV1 extends SegmentMeta {

        SegmentMetaV1(int version, long rowCount, int columnCount, int mode) {
            super(version, rowCount, columnCount, mode);
        }

        SegmentMetaV1(int version) {
            this.version = version;
        }

        public int version() {
            return version;
        }

        int metaSize() {
            int cniSize = 0;
            for (ColumnNodeMeta cni : columnNodeInfos) {
                cniSize += cni.infoSize();
            }
            int colInfoSize = 0;
            for (ColumnMeta info : columnInfos) {
                colInfoSize += ((ColumnMetaV1) info).metaSize(version);
            }
            int size = 8 + 8 + 8 + 4 + cniSize + colInfoSize;

            switch (version) {
                case Version.VERSION_0_ID:
                    return size;
                case Version.VERSION_1_ID:
                case Version.VERSION_2_ID:
                case Version.VERSION_4_ID:
                case Version.VERSION_5_ID:
                    return size + 8 + 4;
                default:
                    return size + 8 + 4 + 4;
            }
        }

        void write(ByteBuffer buffer) {
            if (version != Version.VERSION_0_ID) {
                buffer.putLong(VERSION_FLAG);
                buffer.putInt(version);
            }
            buffer.putLong(sectionOffset);
            buffer.putLong(sectionSize);
            buffer.putLong(rowCount);
            buffer.putInt(columnCount);
            if (version >= Version.VERSION_6_ID) {
                buffer.putInt(mode);
            }

            for (ColumnNodeMeta cni : columnNodeInfos) {
                ColumnNodeMeta.write(cni, buffer);
            }
            for (ColumnMeta ci : columnInfos) {
                ColumnMetaV1.write(version, (ColumnMetaV1) ci, buffer);
            }
        }

        static SegmentMetaV1 read(ByteBuffer buffer) {
            long flag = buffer.getLong();
            int version;
            long sectionOffset;
            if (flag == VERSION_FLAG) {
                version = buffer.getInt();
                sectionOffset = buffer.getLong();
            } else {
                version = Version.VERSION_0_ID;
                sectionOffset = flag;
            }

            SegmentMetaV1 info = new SegmentMetaV1(version);
            info.sectionOffset = sectionOffset;
            info.sectionSize = buffer.getLong();
            info.rowCount = buffer.getLong();
            info.columnCount = buffer.getInt();
            if (version >= Version.VERSION_6_ID) {
                info.mode = buffer.getInt();
            } else {
                // We don't have segment mode before v6.
                info.mode = SegmentMode.DEFAULT.id;
            }

            info.columnNodeInfos = new ColumnNodeMeta[info.columnCount];
            for (int i = 0; i < info.columnCount; i++) {
                info.columnNodeInfos[i] = ColumnNodeMeta.read(buffer);
            }

            info.columnInfos = new ColumnMeta[info.columnCount];
            for (int i = 0; i < info.columnCount; i++) {
                info.columnInfos[i] = ColumnMetaV1.read(version, buffer);
            }

            return info;
        }
    }

    public static class ColumnMetaV1 extends ColumnMeta {
        public long dpnOffset;
        public long indexOffset;
        public long extIndexOffset;
        public long packOffset;

        // Those will not store.
        private int dpnSize;
        private int indexSize;
        private int extIndexSize;
        private long packSize;

        ColumnMetaV1(String name, int sqlType, boolean isIndexed, int dpnSize, int indexSize, int extIndexSize, long packSize) {
            this.nameSize = UTF8Util.toUtf8(name).length;
            this.name = name.intern();
            this.sqlType = sqlType;
            this.isIndexed = isIndexed;
            this.dpnSize = dpnSize;
            this.indexSize = indexSize;
            this.extIndexSize = extIndexSize;
            this.packSize = packSize;
        }

        private ColumnMetaV1() {}

        // @formatter:off
        public long dpnOffset() {return dpnOffset;}
        public long indexOffset() {return indexOffset;}
        public long extIndexOffset() {return extIndexOffset;}
        public long packOffset() {return packOffset;}
        // @formatter:off

        void setDataOffset(int version, long dataOffset) {
            dpnOffset = dataOffset;
            indexOffset = dpnOffset + dpnSize;
            if (version < Version.VERSION_6_ID) {
                packOffset = indexOffset + indexSize;
            } else {
                extIndexOffset = indexOffset + indexSize;
                packOffset = extIndexOffset + extIndexSize;
            }
        }

        int metaSize(int version) {
            if (version < Version.VERSION_5_ID) {
                return 4 + nameSize + 1 + 8 + 8 + 8;
            } else if (version == Version.VERSION_5_ID) {
                return 4 + nameSize + 4 + 8 + 8 + 8;
            } else if (version == Version.VERSION_6_ID) {
                return 4 + nameSize + 4 + 8 + 8 + 8 + 8;
            } else {
                return 4 + nameSize + 4 + 1 + 8 + 8 + 8 + 8;
            }
        }

        long dataSize() {
            return (long) dpnSize + (long) indexSize + (long) extIndexSize + packSize;
        }

        static void write(int version, ColumnMetaV1 ci, ByteBuffer buffer) {
            int pos = buffer.position();

            buffer.putInt(ci.nameSize);
            UTF8Util.toUtf8(buffer, ci.name);
            if (version < Version.VERSION_5_ID) {
                buffer.put((byte) ci.sqlType);
            } else {
                buffer.putInt(ci.sqlType);
            }
            if (version >= Version.VERSION_7_ID) {
                buffer.put((byte) (ci.isIndexed ? 1 : 0));
            }
            buffer.putLong(ci.dpnOffset);
            buffer.putLong(ci.indexOffset);
            if (version >= Version.VERSION_6_ID) {
                buffer.putLong(ci.extIndexOffset);
            }
            buffer.putLong(ci.packOffset);

            assert buffer.position() - pos == ci.metaSize(version);
        }

        static ColumnMetaV1 read(int version, ByteBuffer buffer) {
            int pos = buffer.position();

            ColumnMetaV1 info = new ColumnMetaV1();

            info.nameSize = buffer.getInt();
            info.name = UTF8Util.fromUtf8(buffer, info.nameSize).intern();
            if (version < Version.VERSION_5_ID) {
                info.sqlType = buffer.get();
            } else {
                info.sqlType = buffer.getInt();
            }

            if (version >= Version.VERSION_7_ID) {
                info.isIndexed = buffer.get() == 1;
            }
            info.dpnOffset = buffer.getLong();
            info.indexOffset = buffer.getLong();
            if (version >= Version.VERSION_6_ID) {
                info.extIndexOffset = buffer.getLong();
            }
            info.packOffset = buffer.getLong();

            assert buffer.position() - pos == info.metaSize(version);

            return info;
        }
    }
}

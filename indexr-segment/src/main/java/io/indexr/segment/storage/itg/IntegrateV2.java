package io.indexr.segment.storage.itg;

import com.google.common.base.Preconditions;

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
 * Storge segment packrow by packrow.
 *
 * <pre>
 * Segment storage structure:
 * | IXRSEGxx | sementMeta | col0 dpn | col1 dpn ... | col0 index | col1 index ... | packrow0 | packrow1 ... |
 *
 * PackRow structure:
 * | extIndex0 | extIndex1 ... | dataPack0 | dataPack1 ... |
 * </pre>
 */
public class IntegrateV2 implements Integrate {
    public static IntegrateV2 INSTANCE = new IntegrateV2();

    @Override
    public SegmentMeta write(StorageSegment segment, ByteBufferWriter.PredictSizeOpener writerOpener) throws IOException {
        assert segment.version() >= Version.VERSION_7_ID;

        List<StorageColumn> columns = segment.columns();
        SegmentMode mode = segment.mode();
        SegmentMetaV2 sectionInfo = new SegmentMetaV2(segment.version(), segment.rowCount(), columns.size(), mode.id);
        sectionInfo.setSectionOffset(Version.INDEXR_SEG_FILE_FLAG_SIZE);

        int colCount = columns.size();
        for (int colId = 0; colId < colCount; colId++) {
            StorageColumn column = columns.get(colId);

            ColumnNode columnNode = segment.columnNode(colId);
            sectionInfo.columnNodeInfos[colId] = new ColumnNodeMeta(columnNode.getMinNumValue(), columnNode.getMaxNumValue());

            // Hack!
            boolean isIndexed = (segment.version() >= Version.VERSION_7_ID && column.isIndexed());
            ColumnMetaV2 info = new ColumnMetaV2(column.name(), column.sqlType().id, isIndexed);
            sectionInfo.columnInfos[colId] = info;
        }

        int packCount = segment.packCount();
        // The 4 bytes are the size of info. See ObjSaver#save() for detail.
        long offset = sectionInfo.sectionOffset + 4 + sectionInfo.metaSize();

        // dpn
        int dpnSize = mode.versionAdapter.dpnSize(segment.version(), mode);
        for (int colId = 0; colId < colCount; colId++) {
            ((ColumnMetaV2) sectionInfo.columnInfos[colId]).setDpnOffset(offset);
            int columnDPNSize = dpnSize * packCount;
            offset += columnDPNSize;
        }
        // index
        for (int colId = 0; colId < colCount; colId++) {
            ((ColumnMetaV2) sectionInfo.columnInfos[colId]).setIndexOffset(offset);

            StorageColumn column = columns.get(colId);
            DataPackNode[] dpns = column.getDPNs();
            int columnIndexSize = 0;
            for (DataPackNode dpn : dpns) {
                columnIndexSize += dpn.indexSize();
            }
            offset += columnIndexSize;
        }

        // Update the dpn's packAddr and extIndexAddr
        // Those values are different between DPSegment and IntegratedSegment.
        DataPackNode[][] packRowDPNs = new DataPackNode[colCount][packCount];
        for (int packId = 0; packId < packCount; packId++) {
            for (int colId = 0; colId < colCount; colId++) {
                StorageColumn column = segment.column(colId);
                DataPackNode dpn = column.dpn(packId);
                DataPackNode newDpn = dpn.clone();

                newDpn.setExtIndexAddr(offset);
                offset += newDpn.extIndexSize();

                packRowDPNs[colId][packId] = newDpn;
            }

            for (int colId = 0; colId < colCount; colId++) {
                DataPackNode newDpn = packRowDPNs[colId][packId];

                newDpn.setPackAddr(offset);
                offset += newDpn.packSize();
            }
        }

        // sectionSize is the size of this segment file.
        long sectionSize = offset;
        sectionInfo.setSectionSize(sectionSize);
        // Start to move real data in.

        try (ByteBufferWriter writer = writerOpener.open(sectionSize)) {
            long writeOffset = 0;
            // First write version message.
            writer.write(Version.fromId(segment.version()).flag);
            writeOffset += Version.INDEXR_SEG_FILE_FLAG_SIZE;

            // Write info.
            int sectionMetaSize = sectionInfo.metaSize();
            writeOffset += ObjectSaver.save(writer, sectionInfo, sectionMetaSize, SegmentMetaV2::write);

            // Write dpns.
            for (int colId = 0; colId < colCount; colId++) {
                for (int packId = 0; packId < packCount; packId++) {
                    // Store the modified dpns.
                    packRowDPNs[colId][packId].write(writer);
                }
            }
            writeOffset += dpnSize * colCount * packCount;

            // Write indices.
            for (int colId = 0; colId < colCount; colId++) {
                StorageColumn column = segment.column(colId);
                RSIndex index = column.loadIndex();
                if (index != null) {
                    writeOffset += index.write(writer);
                    index.free();
                }
            }

            // Write pack rows.
            for (int packId = 0; packId < packCount; packId++) {
                for (int colId = 0; colId < colCount; colId++) {
                    StorageColumn column = segment.column(colId);
                    DataPackNode dpn = column.dpn(packId);

                    PackExtIndex extIndex = column.loadExtIndex(dpn);
                    writeOffset += extIndex.write(writer);
                    extIndex.free();
                }

                for (int colId = 0; colId < colCount; colId++) {
                    StorageColumn column = segment.column(colId);
                    DataPackNode dpn = column.dpn(packId);

                    ByteSlice pack = column.loadPack(dpn);
                    writer.write(pack.byteBuffer());
                    writeOffset += pack.size();
                    pack.free();
                }
            }

            assert writeOffset == sectionSize;
        }

        return sectionInfo;
    }

    @Override
    public SegmentMeta read(ByteBufferReader reader) throws IOException {
        ByteBuffer infoBuffer = null;
        Version version = Version.check(reader);
        if (version == null) {
            return null;
        }

        assert version.id >= Version.VERSION_7_ID;

        long sectionOffset = Version.INDEXR_SEG_FILE_FLAG_SIZE;
        SegmentMetaV2 sectionInfo = ObjectSaver.load(reader, sectionOffset, SegmentMetaV2::read);
        assert sectionInfo.version == version.id;
        return sectionInfo;
    }

    public static class SegmentMetaV2 extends SegmentMeta {
        SegmentMetaV2(int version, long rowCount, int columnCount, int mode) {
            super(version, rowCount, columnCount, mode);
        }

        SegmentMetaV2() {}

        int metaSize() {
            int cniSize = 0;
            for (ColumnNodeMeta cni : columnNodeInfos) {
                cniSize += cni.infoSize();
            }
            int colInfoSize = 0;
            for (ColumnMeta info : columnInfos) {
                colInfoSize += ((ColumnMetaV2) info).metaSize(version);
            }
            return 8 + 4 + 4 + 8 + 4 + 8 + 8 + cniSize + colInfoSize;
        }

        void setSectionOffset(long v) {
            this.sectionOffset = v;
        }

        void setSectionSize(long v) {
            this.sectionSize = v;
        }

        @Override
        void write(ByteBuffer buffer) {
            buffer.putLong(VERSION_FLAG);
            buffer.putInt(version);
            buffer.putInt(mode);
            buffer.putLong(rowCount);
            buffer.putInt(columnCount);
            buffer.putLong(sectionOffset);
            buffer.putLong(sectionSize);

            for (ColumnNodeMeta cni : columnNodeInfos) {
                ColumnNodeMeta.write(cni, buffer);
            }
            for (ColumnMeta ci : columnInfos) {
                ((ColumnMetaV2) ci).write(version, buffer);
            }
        }

        static SegmentMetaV2 read(ByteBuffer buffer) {
            SegmentMetaV2 info = new SegmentMetaV2();
            long flag = buffer.getLong();
            Preconditions.checkState(flag == VERSION_FLAG);
            info.version = buffer.getInt();
            info.mode = buffer.getInt();
            info.rowCount = buffer.getLong();
            info.columnCount = buffer.getInt();
            info.sectionOffset = buffer.getLong();
            info.sectionSize = buffer.getLong();

            info.columnNodeInfos = new ColumnNodeMeta[info.columnCount];
            for (int i = 0; i < info.columnCount; i++) {
                info.columnNodeInfos[i] = ColumnNodeMeta.read(buffer);
            }

            info.columnInfos = new ColumnMeta[info.columnCount];
            for (int i = 0; i < info.columnCount; i++) {
                info.columnInfos[i] = ColumnMetaV2.read(info.version, buffer);
            }

            return info;
        }
    }

    public static class ColumnMetaV2 extends ColumnMeta {
        public long dpnOffset;
        public long indexOffset;

        ColumnMetaV2(String name, int sqlType, boolean isIndexed) {
            this.nameSize = UTF8Util.toUtf8(name).length;
            this.name = name.intern();
            this.sqlType = sqlType;
            this.isIndexed = isIndexed;
        }

        public ColumnMetaV2() {}

        // @formatter:off
        public long dpnOffset() {return dpnOffset;}
        public void setDpnOffset(long v) {this.dpnOffset = v;}
        public long indexOffset(){return indexOffset;}
        public void setIndexOffset(long v){this.indexOffset = v;}
        // @formatter:off

        int metaSize(int version) {
            return 4 + nameSize + 4 + 1 + 8 + 8;
        }

        void write(int version, ByteBuffer buffer) {
            int pos = buffer.position();

            buffer.putInt(nameSize);
            UTF8Util.toUtf8(buffer, name);
            buffer.putInt(sqlType);
            buffer.put((byte) (isIndexed ? 1 : 0));
            buffer.putLong(dpnOffset);
            buffer.putLong(indexOffset);

            assert buffer.position() - pos == metaSize(version);
        }

        static ColumnMetaV2 read(int version, ByteBuffer buffer) {
            int pos = buffer.position();

            ColumnMetaV2 info = new ColumnMetaV2();

            info.nameSize = buffer.getInt();
            info.name = UTF8Util.fromUtf8(buffer, info.nameSize).intern();
            info.sqlType = buffer.getInt();
            info.isIndexed = buffer.get() == 1;
            info.dpnOffset = buffer.getLong();
            info.indexOffset = buffer.getLong();

            assert buffer.position() - pos == info.metaSize(version);

            return info;
        }
    }
}

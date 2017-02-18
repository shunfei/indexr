package io.indexr.segment.pack;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.indexr.io.ByteBufferReader;
import io.indexr.io.ByteBufferWriter;
import io.indexr.segment.PackExtIndex;
import io.indexr.segment.RSIndex;
import io.indexr.segment.SegmentMode;
import io.indexr.util.ByteBufferUtil;
import io.indexr.util.IOUtil;
import io.indexr.util.ObjectLoader;
import io.indexr.util.ObjectSaver;
import io.indexr.util.UTF8Util;


public class Integrated {
    static SectionInfo write(StorageSegment segment, ByteBufferWriter.PredictSizeOpener writerOpener) throws IOException {
        List<StorageColumn> columns = segment.columns();
        SectionInfo sectionInfo = new SectionInfo(segment.version(), segment.rowCount(), columns.size(), segment.mode().id);
        sectionInfo.sectionOffset = Version.INDEXR_SEG_FILE_FLAG_SIZE; // The first section, and the only one.

        for (int colId = 0; colId < columns.size(); colId++) {
            ColumnNode columnNode = segment.columnNode(colId);
            sectionInfo.columnNodeInfos[colId] = new ColumnNodeInfo(columnNode.getMinNumValue(), columnNode.getMaxNumValue());

            StorageColumn column = columns.get(colId);
            DataPackNode[] dpns = column.getDPNs();
            int dpnSize = DataPackNode.serializedSize(segment.version()) * dpns.length;
            int indexSize = 0;
            int extIndexSize = 0;
            long packSize = 0;
            for (DataPackNode dpn : dpns) {
                indexSize += dpn.indexSize();
                extIndexSize += dpn.extIndexSize();
                packSize += dpn.packSize();
            }

            ColumnInfo info = new ColumnInfo(column.name(), column.sqlType().id, dpnSize, indexSize, extIndexSize, packSize);
            sectionInfo.columnInfos[colId] = info;
        }

        // Calculate the whole size of this section.
        long dataSize = 0;
        for (ColumnInfo ci : sectionInfo.columnInfos) {
            dataSize += ci.dataSize();
        }
        // First 4 bytes are the size of info. See ObjSaver#save() for detail.
        sectionInfo.sectionSize = 4 + sectionInfo.infoSize() + dataSize;

        // Set the dataOffset.
        long offset = sectionInfo.sectionOffset + 4 + sectionInfo.infoSize();
        for (int colId = 0; colId < columns.size(); colId++) {
            ColumnInfo columnInfo = sectionInfo.columnInfos[colId];
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
            ObjectSaver.save(writer, sectionInfo, sectionInfo.infoSize(), SectionInfo::write);

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
                    DataPack pack = column.loadPack(dpn);
                    pack.write(writer);
                    pack.free();
                }
            }

            writer.flush();

            return sectionInfo;
        }
    }

    /**
     * Read the section info from this file.
     *
     * @param reader the file to read
     * @return null means this file is not a segment
     */
    public static SectionInfo read(ByteBufferReader reader) throws IOException {
        List<SectionInfo> infos = SectionInfo.read(reader);
        return infos == null ? null : SectionInfo.merge(infos);
    }

    /**
     * A section info store the information of a section in a segment.
     * A segment file may consisted by several sections, each section consisted by several full rows of columns.
     */
    public static class SectionInfo {
        // If the first 8 bytes of SectionInfo equals to this,
        // means the next 4 bytes is version.
        static final long VERSION_FLAG = 0xFFFFFFFF_FFFFFFFFL;

        @JsonIgnore
        public final long versionFlag = VERSION_FLAG;
        // We store version here because SectionInfo can be stored seperated besides segment file.
        public final int version;

        public long rowCount;
        public int columnCount;
        public int mode;

        public long sectionOffset;
        public long sectionSize;

        public ColumnNodeInfo[] columnNodeInfos;
        public ColumnInfo[] columnInfos;

        SectionInfo(int version, long rowCount, int columnCount, int mode) {
            this.version = version;
            this.rowCount = rowCount;
            this.columnCount = columnCount;
            this.columnNodeInfos = new ColumnNodeInfo[columnCount];
            this.columnInfos = new ColumnInfo[columnCount];
            this.mode = mode;
        }

        SectionInfo(int version) {
            this.version = version;
        }

        public int version() {
            return version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SectionInfo info = (SectionInfo) o;

            if (version != info.version) return false;
            if (sectionOffset != info.sectionOffset) return false;
            if (sectionSize != info.sectionSize) return false;
            if (rowCount != info.rowCount) return false;
            if (columnCount != info.columnCount) return false;
            if (version >= Version.VERSION_6_ID) {
                if (mode != info.mode) return false;
            }
            if (!Arrays.equals(columnNodeInfos, info.columnNodeInfos)) return false;
            return Arrays.equals(columnInfos, info.columnInfos);
        }

        int infoSize() {
            int cniSize = 0;
            for (ColumnNodeInfo cni : columnNodeInfos) {
                cniSize += cni.infoSize();
            }
            int colInfoSize = 0;
            for (ColumnInfo info : columnInfos) {
                colInfoSize += info.infoSize(version);
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

        static void write(SectionInfo si, ByteBuffer buffer) {
            if (si.version != Version.VERSION_0_ID) {
                buffer.putLong(si.versionFlag);
                buffer.putInt(si.version);
            }
            buffer.putLong(si.sectionOffset);
            buffer.putLong(si.sectionSize);
            buffer.putLong(si.rowCount);
            buffer.putInt(si.columnCount);
            if (si.version >= Version.VERSION_6_ID) {
                buffer.putInt(si.mode);
            }

            for (ColumnNodeInfo cni : si.columnNodeInfos) {
                ColumnNodeInfo.write(cni, buffer);
            }
            for (ColumnInfo ci : si.columnInfos) {
                ColumnInfo.write(si.version, ci, buffer);
            }
        }

        static SectionInfo read(ByteBuffer buffer) {
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

            SectionInfo info = new SectionInfo(version);
            info.sectionOffset = sectionOffset;
            info.sectionSize = buffer.getLong();
            info.rowCount = buffer.getLong();
            info.columnCount = buffer.getInt();
            if (version >= Version.VERSION_6_ID) {
                info.mode = buffer.getInt();
            } else {
                info.mode = SegmentMode.DEFAULT.id;
            }

            info.columnNodeInfos = new ColumnNodeInfo[info.columnCount];
            for (int i = 0; i < info.columnCount; i++) {
                info.columnNodeInfos[i] = ColumnNodeInfo.read(buffer);
            }

            info.columnInfos = new ColumnInfo[info.columnCount];
            for (int i = 0; i < info.columnCount; i++) {
                info.columnInfos[i] = ColumnInfo.read(version, buffer);
            }

            return info;
        }

        static List<SectionInfo> read(ByteBufferReader reader) throws IOException {
            List<SectionInfo> sectionInfos = new ArrayList<>();

            ByteBuffer infoBuffer = null;
            Version version = Version.check(reader);
            if (version == null) {
                return null;
            }

            long sectionOffset = Version.INDEXR_SEG_FILE_FLAG_SIZE;
            while (reader.exists(sectionOffset)) {
                SectionInfo sectionInfo = ObjectSaver.load(reader, sectionOffset, SectionInfo::read);
                assert sectionInfo.version == version.id;
                sectionOffset += sectionInfo.sectionSize;
                sectionInfos.add(sectionInfo);
            }
            return sectionInfos;
        }

        static SectionInfo merge(List<SectionInfo> sectionInfos) {
            if (sectionInfos.size() == 1) {
                return sectionInfos.get(0);
            }
            long sectionOffset = sectionInfos.get(0).sectionOffset;
            long rowCount = sectionInfos.get(0).rowCount;

            long sectionSize = 0;
            int columnCount = 0;
            for (SectionInfo si : sectionInfos) {
                sectionSize += si.sectionSize;
                columnCount += si.columnCount;
            }
            ColumnNodeInfo[] allColumnNodeInfos = new ColumnNodeInfo[columnCount];
            ColumnInfo[] allColumnInfos = new ColumnInfo[columnCount];
            int colId = 0;
            for (SectionInfo si : sectionInfos) {
                assert rowCount == si.rowCount;
                for (int i = 0; i < si.columnCount; i++, colId++) {
                    allColumnNodeInfos[colId] = si.columnNodeInfos[i];
                    allColumnInfos[colId] = si.columnInfos[i];
                }
            }
            assert colId == columnCount;

            SectionInfo merge = new SectionInfo(sectionInfos.get(0).version);
            merge.sectionOffset = sectionOffset;
            merge.sectionSize = sectionSize;
            merge.rowCount = rowCount;
            merge.columnCount = columnCount;
            merge.columnNodeInfos = allColumnNodeInfos;
            merge.columnInfos = allColumnInfos;
            merge.mode = sectionInfos.get(0).mode;
            return merge;
        }

        public static Map<String, SectionInfo> loadFromLocalFile(Path path) throws IOException {
            if (!Files.exists(path)) {
                return Collections.emptyMap();
            }
            Map<String, SectionInfo> sectionInfos = new HashMap<>(2046);
            ObjectLoader.EntryListener listener = (buffer, size) -> {
                int nameSize = buffer.getInt();
                byte[] bs = new byte[nameSize];
                buffer.get(bs);
                String name = UTF8Util.fromUtf8(bs);
                int siSize = buffer.getInt();
                SectionInfo info = SectionInfo.read(buffer);
                sectionInfos.put(name, info);
            };
            try (ObjectLoader loader = new ObjectLoader(path)) {
                while (loader.hasNext()) {
                    loader.next(listener);
                }
            }
            return sectionInfos;
        }

        public static void saveToLocalFile(Path path, Map<String, SectionInfo> sectionInfos) throws IOException {
            try (FileChannel cacheFile = FileChannel.open(path,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                int patchSize = 64 << 20;// 64MB.
                ByteBuffer buffer = ByteBufferUtil.allocateDirect(patchSize);
                for (Map.Entry<String, SectionInfo> entry : sectionInfos.entrySet()) {
                    String name = entry.getKey();
                    SectionInfo si = entry.getValue();
                    byte[] bs = UTF8Util.toUtf8(name);

                    int entrySize = 4 + bs.length + 4 + si.infoSize();
                    if (buffer.position() + entrySize > buffer.capacity()) {
                        // Full already, flush it.
                        buffer.flip();
                        IOUtil.writeFully(cacheFile, buffer, buffer.remaining());

                        if (entrySize > buffer.capacity()) {
                            // we need to expand the buffer.
                            ByteBufferUtil.free(buffer);
                            buffer = ByteBufferUtil.allocateDirect(entrySize << 1);
                        } else {
                            buffer.clear();
                        }
                    }

                    buffer.putInt(entrySize);
                    buffer.putInt(bs.length);
                    buffer.put(bs);
                    buffer.putInt(si.infoSize());
                    SectionInfo.write(si, buffer);
                }
                buffer.flip();
                IOUtil.writeFully(cacheFile, buffer, buffer.remaining());
                cacheFile.force(false);
                ByteBufferUtil.free(buffer);
            }
        }
    }

    public static class ColumnNodeInfo {
        public final long minNumValue;
        public final long maxNumValue;

        //private static

        public ColumnNodeInfo(long minNumValue, long maxNumValue) {
            this.minNumValue = minNumValue;
            this.maxNumValue = maxNumValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ColumnNodeInfo that = (ColumnNodeInfo) o;
            return minNumValue == that.minNumValue && maxNumValue == that.maxNumValue;
        }

        int infoSize() {
            return 8 + 8;
        }

        static void write(ColumnNodeInfo cni, ByteBuffer buffer) {
            buffer.putLong(cni.minNumValue);
            buffer.putLong(cni.maxNumValue);
        }

        static ColumnNodeInfo read(ByteBuffer buffer) {
            return new ColumnNodeInfo(buffer.getLong(), buffer.getLong());
        }
    }

    public static class ColumnInfo {
        public int nameSize;
        public String name;
        public int sqlType;

        public long dpnOffset;
        public long indexOffset;
        public long extIndexOffset;
        public long packOffset;

        // Those will not store.
        private int dpnSize;
        private int indexSize;
        private int extIndexSize;
        private long packSize;

        ColumnInfo(String name, int sqlType, int dpnSize, int indexSize, int extIndexSize, long packSize) {
            nameSize = UTF8Util.toUtf8(name).length;
            this.name = name.intern();
            this.sqlType = sqlType;
            this.dpnSize = dpnSize;
            this.indexSize = indexSize;
            this.extIndexSize = extIndexSize;
            this.packSize = packSize;
        }

        private ColumnInfo() {}

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ColumnInfo info = (ColumnInfo) o;

            return nameSize == info.nameSize
                    && sqlType == info.sqlType
                    && dpnOffset == info.dpnOffset
                    && indexOffset == info.indexOffset
                    && extIndexOffset == info.extIndexOffset
                    && packOffset == info.packOffset
                    && (name != null ? name.equals(info.name) : info.name == null);
        }

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

        int infoSize(int version) {
            if (version < Version.VERSION_5_ID) {
                return 4 + nameSize + 1 + 8 + 8 + 8;
            } else if (version == Version.VERSION_5_ID) {
                return 4 + nameSize + 4 + 8 + 8 + 8;
            } else {
                return 4 + nameSize + 4 + 8 + 8 + 8 + 8;
            }
        }

        long dataSize() {
            return (long) dpnSize + (long) indexSize + (long) extIndexSize + packSize;
        }

        static void write(int version, ColumnInfo ci, ByteBuffer buffer) {
            int pos = buffer.position();

            buffer.putInt(ci.nameSize);
            UTF8Util.toUtf8(buffer, ci.name);
            if (version < Version.VERSION_5_ID) {
                buffer.put((byte) ci.sqlType);
            } else {
                buffer.putInt(ci.sqlType);
            }
            buffer.putLong(ci.dpnOffset);
            buffer.putLong(ci.indexOffset);
            if (version >= Version.VERSION_6_ID) {
                buffer.putLong(ci.extIndexOffset);
            }
            buffer.putLong(ci.packOffset);

            assert buffer.position() - pos == ci.infoSize(version);
        }

        static ColumnInfo read(int version, ByteBuffer buffer) {
            int pos = buffer.position();

            ColumnInfo info = new ColumnInfo();

            info.nameSize = buffer.getInt();
            info.name = UTF8Util.fromUtf8(buffer, info.nameSize).intern();
            if (version < Version.VERSION_5_ID) {
                info.sqlType = buffer.get();
            } else {
                info.sqlType = buffer.getInt();
            }
            info.dpnOffset = buffer.getLong();
            info.indexOffset = buffer.getLong();
            if (version >= Version.VERSION_6_ID) {
                info.extIndexOffset = buffer.getLong();
            }
            info.packOffset = buffer.getLong();

            assert buffer.position() - pos == info.infoSize(version);

            return info;
        }
    }
}
package io.indexr.segment.storage.itg;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.indexr.segment.storage.Version;
import io.indexr.util.ByteBufferUtil;
import io.indexr.util.IOUtil;
import io.indexr.util.ObjectLoader;
import io.indexr.util.UTF8Util;

public abstract class SegmentMeta {
    // If the first 8 bytes of SectionInfo equals to this,
    // means the next 4 bytes is version.
    static final long VERSION_FLAG = 0xFFFFFFFF_FFFFFFFFL;

    public int version;
    public int mode;

    public long rowCount;
    public int columnCount;

    public long sectionOffset;
    public long sectionSize;

    public ColumnNodeMeta[] columnNodeInfos;
    public ColumnMeta[] columnInfos;

    SegmentMeta(int version, long rowCount, int columnCount, int mode) {
        this.version = version;
        this.rowCount = rowCount;
        this.columnCount = columnCount;
        this.columnNodeInfos = new ColumnNodeMeta[columnCount];
        this.columnInfos = new ColumnMeta[columnCount];
        this.mode = mode;
    }

    SegmentMeta() {}

    int version() {return version;}

    abstract int metaSize();

    abstract void write(ByteBuffer buffer);

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SegmentMeta info = (SegmentMeta) o;

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

    static SegmentMeta readSegmentMeta(ByteBuffer buffer) {
        return IntegrateV1.SegmentMetaV1.read(buffer);
        // Try fetch the version, and call sub classes to handle the rest.
        //int bufferPos = buffer.position();
        //long val = buffer.getLong();
        //int version;
        //if (val == VERSION_FLAG) {
        //    version = buffer.getInt();
        //} else {
        //    version = Version.VERSION_0_ID;
        //}
        //// Reset the pos, like nothing happen.
        //buffer.position(bufferPos);
        //if (version >= Version.VERSION_7_ID) {
        //    return IntegrateV2.SegmentMetaV2.read(buffer);
        //} else {
        //    return IntegrateV1.SegmentMetaV1.read(buffer);
        //}
    }


    public static Map<String, SegmentMeta> loadFromLocalFile(Path path) throws IOException {
        if (!Files.exists(path)) {
            return Collections.emptyMap();
        }
        Map<String, SegmentMeta> sectionInfos = new HashMap<>(2046);
        ObjectLoader.EntryListener listener = (buffer, size) -> {
            int nameSize = buffer.getInt();
            byte[] nameData = new byte[nameSize];
            buffer.get(nameData);
            String name = UTF8Util.fromUtf8(nameData);
            int siSize = buffer.getInt();
            SegmentMeta info = SegmentMeta.readSegmentMeta(buffer);
            sectionInfos.put(name, info);
        };
        try (ObjectLoader loader = new ObjectLoader(path)) {
            while (loader.hasNext()) {
                loader.next(listener);
            }
        }
        return sectionInfos;
    }

    public static void saveToLocalFile(Path path, Map<String, SegmentMeta> sectionInfos) throws IOException {
        try (FileChannel cacheFile = FileChannel.open(path,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            int patchSize = 64 << 20;// 64MB.
            ByteBuffer buffer = ByteBufferUtil.allocateDirect(patchSize);
            for (Map.Entry<String, SegmentMeta> entry : sectionInfos.entrySet()) {
                String name = entry.getKey();
                SegmentMeta si = entry.getValue();
                byte[] nameData = UTF8Util.toUtf8(name);

                int entrySize = 4 + nameData.length + 4 + si.metaSize();
                if (buffer.position() + entrySize > buffer.capacity()) {
                    // Full already, flush it.
                    buffer.flip();
                    IOUtil.writeFully(cacheFile, buffer);

                    if (entrySize > buffer.capacity()) {
                        // we need to expand the buffer.
                        ByteBufferUtil.free(buffer);
                        buffer = ByteBufferUtil.allocateDirect(entrySize << 1);
                    } else {
                        buffer.clear();
                    }
                }

                buffer.putInt(entrySize);
                buffer.putInt(nameData.length);
                buffer.put(nameData);
                buffer.putInt(si.metaSize());
                si.write(buffer);
            }
            buffer.flip();
            IOUtil.writeFully(cacheFile, buffer);
            cacheFile.force(false);
            ByteBufferUtil.free(buffer);
        }
    }
}

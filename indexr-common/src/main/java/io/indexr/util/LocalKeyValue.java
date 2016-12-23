package io.indexr.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LocalKeyValue implements Iterable<LocalKeyValue.KV>, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(LocalKeyValue.class);
    private static final int dumpThreshold = 256;

    private Path storePath;
    private int conflictCount = 0;

    private ConcurrentHashMap<ByteArrayWrapper, byte[]> keyValues = new ConcurrentHashMap<>();
    private ByteBuffer buffer = ByteBufferUtil.allocateDirect(1 << 20);

    private ThreadLocal<ByteArrayWrapper> keyWrapper = ThreadLocal.withInitial(ByteArrayWrapper::new);

    public LocalKeyValue(Path storePath) throws IOException {
        this.storePath = storePath;

        ObjectLoader.EntryListener listener = (buffer, size) -> {
            int pos = buffer.position();

            int keySize = buffer.getInt();
            byte[] key = new byte[keySize];
            buffer.get(key);
            int valueSize = buffer.getInt();
            byte[] value = new byte[valueSize];
            buffer.get(value);

            assert buffer.position() == pos + size;

            if (keyValues.put(new ByteArrayWrapper(key), value) != null) {
                conflictCount++;
            }
        };
        try (ObjectLoader loader = new ObjectLoader(storePath)) {
            while (loader.hasNext()) {
                loader.next(listener);
            }
        }
    }

    public synchronized void set(byte[] key, byte[] value) throws IOException {
        if (keyValues.put(new ByteArrayWrapper(key), value) != null) {
            conflictCount++;
        }
        if (conflictCount >= dumpThreshold) {
            dump();
        } else {
            try (FileChannel fileChannel = FileChannel.open(storePath,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND)) {
                setBuffer(key, value);
                fileChannel.write(buffer);
                fileChannel.force(false);
            }
        }
    }

    private void setBuffer(byte[] key, byte[] value) {
        int totalSize = 4 + key.length + 4 + value.length;
        if (buffer.capacity() < totalSize) {
            ByteBufferUtil.free(buffer);
            buffer = ByteBufferUtil.allocateDirect(totalSize + 4);
        }
        buffer.clear();
        buffer.putInt(totalSize);
        buffer.putInt(key.length);
        buffer.put(key);
        buffer.putInt(value.length);
        buffer.put(value);
        buffer.flip();
    }

    public byte[] get(byte[] key) {
        ByteArrayWrapper wrapper = keyWrapper.get();
        wrapper.set(key);
        return keyValues.get(wrapper);
    }

    private void dump() throws IOException {
        conflictCount = 0;
        try (FileChannel fileChannel = FileChannel.open(storePath,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            for (Map.Entry<ByteArrayWrapper, byte[]> entry : keyValues.entrySet()) {
                setBuffer((byte[]) entry.getKey().base(), entry.getValue());
                fileChannel.write(buffer);
            }
            fileChannel.force(false);
        }
    }

    @Override
    public Iterator<KV> iterator() {
        Iterator<Map.Entry<ByteArrayWrapper, byte[]>> kvIterator = keyValues.entrySet().iterator();
        return new Iterator<KV>() {
            @Override
            public boolean hasNext() {
                return kvIterator.hasNext();
            }

            @Override
            public KV next() {
                Map.Entry<ByteArrayWrapper, byte[]> entry = kvIterator.next();
                return new KV((byte[]) entry.getKey().base(), entry.getValue());
            }
        };
    }

    @Override
    public void close() throws IOException {
        if (keyValues != null) {
            keyValues.clear();
            ByteBufferUtil.free(buffer);

            keyValues = null;
            buffer = null;
        }
    }

    public static class KV {
        public final byte[] key;
        public final byte[] value;

        public KV(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }
}

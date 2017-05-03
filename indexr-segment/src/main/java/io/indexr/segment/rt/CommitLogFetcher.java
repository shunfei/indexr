package io.indexr.segment.rt;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import io.indexr.segment.SegmentSchema;
import io.indexr.util.ObjectLoader;
import io.indexr.util.Strings;

public class CommitLogFetcher implements Fetcher, ObjectLoader.EntryListener {
    private Path path;
    private ObjectLoader objectLoader;
    private UTF8Row.Creator rowCreator;

    private UTF8Row nextRow;

    private long msgCount = 0;

    public CommitLogFetcher(Path path) throws IOException {
        assert path != null;

        this.path = path;
    }

    @Override
    public void setRowCreator(String name, UTF8Row.Creator rowCreator) {
        this.rowCreator = rowCreator;
    }

    @Override
    public boolean ensure(SegmentSchema schema) throws Exception {
        if (objectLoader != null) {
            return true;
        }
        objectLoader = new ObjectLoader(path);
        return true;
    }

    @Override
    public boolean hasNext() throws Exception {
        return objectLoader != null && objectLoader.hasNext();
    }

    @Override
    public void onEntry(ByteBuffer buffer, int size) {
        msgCount++;
        nextRow = rowCreator.deserialize(buffer, size);
    }

    @Override
    public List<UTF8Row> next() throws Exception {
        objectLoader.next(this);
        return Collections.singletonList(nextRow);
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(objectLoader);
        objectLoader = null;
    }

    @Override
    public void commit() {}

    @Override
    public boolean equals(Fetcher o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CommitLogFetcher that = (CommitLogFetcher) o;
        return Strings.equals(path.toString(), that.path.toString());
    }

    @Override
    public String toString() {
        return "CommitLogFetcher{" +
                "path=" + path +
                '}';
    }

    @Override
    public long statConsume() {
        return msgCount;
    }

    @Override
    public long statProduce() {
        return msgCount;
    }

    @Override
    public long statIgnore() {
        return 0;
    }

    @Override
    public long statFail() {
        return 0;
    }

    @Override
    public void statReset() {
        msgCount = 0;
    }
}

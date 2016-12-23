package io.indexr.server.rt.fetcher;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;

import io.indexr.segment.SegmentSchema;
import io.indexr.segment.rt.Fetcher;
import io.indexr.segment.rt.UTF8JsonRowCreator;
import io.indexr.segment.rt.UTF8Row;

/**
 * This class does not work properly, only used in tests.
 */
public class ConsoleFetcher implements Fetcher {

    private final UTF8JsonRowCreator utf8JsonRowCreator = new UTF8JsonRowCreator();
    private volatile boolean closed = false;

    private volatile BufferedReader reader;

    @JsonCreator
    public ConsoleFetcher() {}

    @Override
    public void setRowCreator(String name, UTF8Row.Creator rowCreator) {
        utf8JsonRowCreator.setRowCreator(name, rowCreator);
    }

    @Override
    public boolean ensure(SegmentSchema schema) throws Exception {
        this.reader = new BufferedReader(new InputStreamReader(System.in));
        closed = false;
        return true;
    }

    @Override
    public boolean hasNext() throws Exception {
        return !closed;
    }

    @Override
    public List<UTF8Row> next() throws Exception {
        // Node that System.in cannot be reopened, so don't close it.
        while (true) {
            if (reader == null) {
                return Collections.emptyList();
            }
            if (System.in.available() > 0) {
                break;
            }
            Thread.sleep(100);
        }
        String txt = reader.readLine();
        byte[] data = txt.getBytes("utf-8");
        return utf8JsonRowCreator.create(data);
    }

    @Override
    public void commit() {}

    @Override
    public void close() throws IOException {
        // Don't close System.in.
        reader = null;
        closed = true;
    }

    @Override
    public boolean equals(Fetcher o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return true;
    }

    @Override
    public String toString() {
        return "ConsoleFetcher";
    }

    @Override
    public long statConsume() {
        return utf8JsonRowCreator.getConsumeCount();
    }

    @Override
    public long statProduce() {
        return utf8JsonRowCreator.getProduceCount();
    }

    @Override
    public long statIgnore() {
        return utf8JsonRowCreator.getIgnoreCount();
    }

    @Override
    public long statFail() {
        return utf8JsonRowCreator.getFailCount();
    }

    @Override
    public void statReset() {
        utf8JsonRowCreator.resetStat();
    }
}

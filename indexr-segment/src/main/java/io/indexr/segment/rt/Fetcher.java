package io.indexr.segment.rt;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import io.indexr.segment.SegmentSchema;

/**
 * A row datasource.
 * 
 * A fetcher can be closed and open({@link #ensure(SegmentSchema)}) over and over.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface Fetcher extends Closeable {

    public void setRowCreator(String name, UTF8Row.Creator rowCreator);

    /**
     * Make sure this fetcher is ready for fetching data.
     */
    public boolean ensure(SegmentSchema schema) throws Exception;

    /**
     * Has more rows or not.
     */
    public boolean hasNext() throws Exception;

    /**
     * Take next rows.
     */
    public List<UTF8Row> next() throws Exception;

    /**
     * Commit the taken operation.
     * Good chance to commit something like offsets here.
     */
    public void commit() throws Exception;

    /**
     * Whether those two fetchers are the same. Used in config reload.
     */
    public boolean equals(Fetcher fetcher);

    /**
     * Close this fetcher.
     */
    @Override
    void close() throws IOException;

    /**
     * The number of consumed events.
     */
    public long statConsume();

    /**
     * The number of produced rows from events.
     */
    public long statProduce();

    /**
     * The number of rows being ignored from produced.
     */
    public long statIgnore();

    /**
     * The number of failed events.
     */
    public long statFail();

    /**
     * Reset the statistics.
     */
    public void statReset();
}

package org.apache.spark.executor;


/**
 * :: DeveloperApi ::
 * Metrics about reading input data.
 */
public class InputMetrics {
    public DataReadMethod readMethod;

    // @formatter:off
    /**
     * Total bytes read.
     */
    private long _bytesRead;
    public long bytesRead(){return _bytesRead;}
    public void incBytesRead(long bytes){_bytesRead += bytes;}

    /**
     * Total records read.
     */
    private long _recordsRead;
    public long recordsRead(){return _recordsRead;}
    public void incRecordsRead(long records){_recordsRead += records;}
}

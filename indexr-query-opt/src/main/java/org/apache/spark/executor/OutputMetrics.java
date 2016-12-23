package org.apache.spark.executor;

public class OutputMetrics {
    public DataWriteMethod writeMethod;

    // @formatter:off
    /**
     * Total bytes written
     */
    private long _bytesWritten;
    public long bytesWritten(){return _bytesWritten;}
    public void setBytesWritten(long value){_bytesWritten = value;}

    /**
     * Total records written
     */
    private long _recordsWritten;
    public long recordsWritten(){return _recordsWritten;}
    public void setRecordsWritten(long value){_recordsWritten = value;}
    // @formatter:on
}
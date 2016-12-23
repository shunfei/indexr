package org.apache.spark.executor;

public class ShuffleWriteMetrics {
    // @formatter:off

    /**
    * Number of bytes written for the shuffle by this task
    */
    private volatile long _bytesWritten;
    public long bytesWritten(){return _bytesWritten;}
    public void inc_bytesWritten(long value){_bytesWritten += value;}
    public void dec_bytesWritten(long value){_bytesWritten -= value;}

    /**
    * Time the task spent blocking on writes to disk or buffer cache, in nanoseconds
    */
    private volatile long _writeTime;
    public long writeTime(){return _writeTime;}
    public void inc_writeTime(long value){_writeTime += value;}
    public void dec_writeTime(long value){_writeTime -= value;}


    /**
    * Total number of records written to the shuffle by this task
    */
    private volatile long _recordsWritten;
    public long recordsWritten(){return _recordsWritten;}
    public void incRecordsWritten(long value){_recordsWritten += value;}
    public void decRecordsWritten(long value){_recordsWritten -= value;}
    public void setRecordsWritten(long value){_recordsWritten = value;}

    // Legacy methods for backward compatibility.
    // TODO: remove these once we make this class private.
    @Deprecated
    public long shuffleBytesWritten(){return bytesWritten();}
    @Deprecated
    public long shuffleWriteTime(){return writeTime();}
    @Deprecated
    public long shuffleRecordsWritten(){return recordsWritten();}

    // @formatter:on
}

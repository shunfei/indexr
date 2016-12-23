package org.apache.spark.executor;

public class ShuffleReadMetrics {
    // @formatter:off
    /**
     * Number of remote blocks fetched in this shuffle by this task
     */
    private int _remoteBlocksFetched;
    public int remoteBlocksFetched(){return _remoteBlocksFetched;}
    public void incRemoteBlocksFetched(int value){_remoteBlocksFetched += value;}
    public void decRemoteBlocksFetched(int value){_remoteBlocksFetched -= value;}

    /**
     * Number of local blocks fetched in this shuffle by this task
     */
    private int _localBlocksFetched;
    public int localBlocksFetched(){return _localBlocksFetched;}
    public void incLocalBlocksFetched(int value){_localBlocksFetched += value;}
    public void decLocalBlocksFetched(int value){_localBlocksFetched -= value;}

    /**
     * Time the task spent waiting for remote shuffle blocks. This only includes the time
     * blocking on shuffle input data. For instance if block B is being fetched while the task is
     * still not finished processing block A, it is not considered to be blocking on block B.
     */
    private long _fetchWaitTime;
    public long fetchWaitTime(){return _fetchWaitTime;}
    public void incFetchWaitTime(long value){_fetchWaitTime += value;}
    public void decFetchWaitTime(long value){_fetchWaitTime -= value;}

    /**
     * Total number of remote bytes read from the shuffle by this task
     */
    private long _remoteBytesRead;
    public long remoteBytesRead(){return _remoteBytesRead;}
    public void incRemoteBytesRead(long value){_remoteBytesRead += value;}
    public void decRemoteBytesRead(long value){_remoteBytesRead -= value;}

    /**
     * Shuffle data that was read from the local disk (as opposed to from a remote executor).
     */
    private long _localBytesRead;
    public long localBytesRead(){return _localBytesRead;}
    public void incLocalBytesRead(long value){_localBytesRead += value;}
    public void decLocalBytesRead(long value){_localBytesRead -= value;}

    /**
     * Total bytes fetched in the shuffle by this task (both remote and local).
     */
    public long totalBytesRead(){return _remoteBytesRead + _localBytesRead;}

    /**
     * Number of blocks fetched in this shuffle by this task (remote or local)
     */
    public int totalBlocksFetched(){return _remoteBlocksFetched + _localBlocksFetched;}

    /**
     * Total number of records read from the shuffle by this task
     */
    private long _recordsRead;
    public void incRecordsRead(long value){_recordsRead += value;}
    public void decRecordsRead(long value){_recordsRead -= value;}
    // @formatter:on
}
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.executor;

import java.io.Serializable;


/**
 * :: DeveloperApi ::
 * Metrics tracked during the execution of a task.
 * 
 * This class is used to house metrics both for in-progress and completed tasks. In executors,
 * both the task thread and the heartbeat thread write to the TaskMetrics. The heartbeat thread
 * reads it to send in-progress metrics, and the task thread reads it to send metrics along with
 * the completed task.
 * 
 * So, when adding new fields, take into consideration that the whole object can be serialized for
 * shipping off at any time to consumers of the SparkListener interface.
 */
class TaskMetrics implements Serializable {
    // @formatter:off
    /**
     * Host's name the task runs on
     */
    private String _hostname;
    public String hostname(){return _hostname;}
    public void setHostname(String value){_hostname = value;}

    /**
     * Time taken on the executor to deserialize this task
     */
    private long _executorDeserializeTime;
    public long executorDeserializeTime(){return _executorDeserializeTime;}
    public void setExecutorDeserializeTime(long value){_executorDeserializeTime = value;}

    /**
     * Time the executor spends actually running the task (including fetching shuffle data)
     */
    private long _executorRunTime;
    public long executorRunTime(){return _executorRunTime;}
    public void setExecutorRunTime(long value){_executorRunTime = value;}

    /**
     * The number of bytes this task transmitted back to the driver as the TaskResult
     */
    private long _resultSize;
    public long resultSize(){return _resultSize;}
    public void setResultSize(long value){_resultSize = value;}

    /**
     * Amount of time the JVM spent in garbage collection while executing this task
     */
    private long _jvmGCTime;
    public long jvmGCTime(){return _jvmGCTime;}
    public void setJvmGCTime(long value){_jvmGCTime = value;}

    /**
     * Amount of time spent serializing the task result
     */
    private long _resultSerializationTime;
    public long resultSerializationTime(){return _resultSerializationTime;}
    public void setResultSerializationTime(long value){_resultSerializationTime = value;}

    /**
     * The number of in-memory bytes spilled by this task
     */
    private long _memoryBytesSpilled;
    public long memoryBytesSpilled(){return _memoryBytesSpilled;}
    public void incMemoryBytesSpilled(long value){_memoryBytesSpilled += value;}
    public void decMemoryBytesSpilled(long value){_memoryBytesSpilled -= value;}

    /**
     * The number of on-disk bytes spilled by this task
     */
    private long _diskBytesSpilled;
    public long diskBytesSpilled(){return _diskBytesSpilled;}
    public void incDiskBytesSpilled(long value){_diskBytesSpilled += value;}
    public void decDiskBytesSpilled(long value){_diskBytesSpilled -= value;}

    /**
     * If this task reads from a HadoopRDD or from persisted data, metrics on how much data was read
     * are stored here.
     */
    private InputMetrics _inputMetrics;
    public InputMetrics inputMetrics(){return _inputMetrics;}
    //
    ///**
    // * This should only be used when recreating TaskMetrics, not when updating input metrics in
    // * executors
    // */
    //private[spark] def setInputMetrics(inputMetrics: Option[InputMetrics]) {
    //    _inputMetrics = inputMetrics
    //}
    //
    ///**
    // * If this task writes data externally (e.g. to a distributed filesystem), metrics on how much
    // * data was written are stored here.
    // */
    //var outputMetrics: Option[OutputMetrics] = None
    //
    ///**
    // * If this task reads from shuffle output, metrics on getting shuffle data will be collected here.
    // * This includes read metrics aggregated over all the task's shuffle dependencies.
    // */
    //private var _shuffleReadMetrics: Option[ShuffleReadMetrics] = None
    //
    //def shuffleReadMetrics: Option[ShuffleReadMetrics] = _shuffleReadMetrics
    //
    ///**
    // * This should only be used when recreating TaskMetrics, not when updating read metrics in
    // * executors.
    // */
    //private[spark] def setShuffleReadMetrics(shuffleReadMetrics: Option[ShuffleReadMetrics]) {
    //    _shuffleReadMetrics = shuffleReadMetrics
    //}
    //
    ///**
    // * ShuffleReadMetrics per dependency for collecting independently while task is in progress.
    // */
    //@transient private lazy val depsShuffleReadMetrics: ArrayBuffer[ShuffleReadMetrics] =
    //        new ArrayBuffer[ShuffleReadMetrics]()
    //
    ///**
    // * If this task writes to shuffle output, metrics on the written shuffle data will be collected
    // * here
    // */
    //var shuffleWriteMetrics: Option[ShuffleWriteMetrics] = None
    //
    ///**
    // * Storage statuses of any blocks that have been updated as a result of this task.
    // */
    //var updatedBlocks: Option[Seq[(BlockId, BlockStatus)]] = None
    //
    ///**
    // * A task may have multiple shuffle readers for multiple dependencies. To avoid synchronization
    // * issues from readers in different threads, in-progress tasks use a ShuffleReadMetrics for each
    // * dependency, and merge these metrics before reporting them to the driver. This method returns
    // * a ShuffleReadMetrics for a dependency and registers it for merging later.
    // */
    //private [spark] def createShuffleReadMetricsForDependency(): ShuffleReadMetrics = synchronized {
    //    val readMetrics = new ShuffleReadMetrics()
    //    depsShuffleReadMetrics += readMetrics
    //    readMetrics
    //}
    //
    ///**
    // * Returns the input metrics object that the task should use. Currently, if
    // * there exists an input metric with the same readMethod, we return that one
    // * so the caller can accumulate bytes read. If the readMethod is different
    // * than previously seen by this task, we return a new InputMetric but don't
    // * record it.
    // *
    // * Once https://issues.apache.org/jira/browse/SPARK-5225 is addressed,
    // * we can store all the different inputMetrics (one per readMethod).
    // */
    //private[spark] def getInputMetricsForReadMethod(readMethod: DataReadMethod): InputMetrics = {
    //    synchronized {
    //        _inputMetrics match {
    //            case None =>
    //                val metrics = new InputMetrics(readMethod)
    //                _inputMetrics = Some(metrics)
    //                metrics
    //            case Some(metrics @ InputMetrics(method)) if method == readMethod =>
    //                metrics
    //            case Some(InputMetrics(method)) =>
    //                new InputMetrics(readMethod)
    //        }
    //    }
    //}
    //
    ///**
    // * Aggregates shuffle read metrics for all registered dependencies into shuffleReadMetrics.
    // */
    //private[spark] def updateShuffleReadMetrics(): Unit = synchronized {
    //    if (!depsShuffleReadMetrics.isEmpty) {
    //        val merged = new ShuffleReadMetrics()
    //        for (depMetrics <- depsShuffleReadMetrics) {
    //            merged.incFetchWaitTime(depMetrics.fetchWaitTime)
    //            merged.incLocalBlocksFetched(depMetrics.localBlocksFetched)
    //            merged.incRemoteBlocksFetched(depMetrics.remoteBlocksFetched)
    //            merged.incRemoteBytesRead(depMetrics.remoteBytesRead)
    //            merged.incLocalBytesRead(depMetrics.localBytesRead)
    //            merged.incRecordsRead(depMetrics.recordsRead)
    //        }
    //        _shuffleReadMetrics = Some(merged)
    //    }
    //}
    //
    //private[spark] def updateInputMetrics(): Unit = synchronized {
    //    inputMetrics.foreach(_.updateBytesRead())
    //}
    //
    //@throws(classOf[IOException])
    //private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    //    in.defaultReadObject()
    //    // Get the hostname from cached data, since hostname is the order of number of nodes in
    //    // cluster, so using cached hostname will decrease the object number and alleviate the GC
    //    // overhead.
    //    _hostname = TaskMetrics.getCachedHostName(_hostname)
    //}
    //
    //private var _accumulatorUpdates: Map[LONG, Any] = Map.empty
    //@transient private var _accumulatorsUpdater: () => Map[LONG, Any] = null
    //
    //private[spark] def updateAccumulators(): Unit = synchronized {
    //    _accumulatorUpdates = _accumulatorsUpdater()
    //}
    //
    ///**
    // * Return the latest updates of accumulators in this task.
    // */
    //def accumulatorUpdates(): Map[LONG, Any] = _accumulatorUpdates
    //
    //private[spark] def setAccumulatorsUpdater(accumulatorsUpdater: () => Map[LONG, Any]): Unit = {
    //    _accumulatorsUpdater = accumulatorsUpdater
    //}
    // @formatter:on
}


//private[spark]object TaskMetrics{
//private val hostNameCache=new ConcurrentHashMap[STRING,STRING]()
//
//        def empty:TaskMetrics=new TaskMetrics
//
//        def getCachedHostName(host:STRING):STRING={
//        val canonicalHost=hostNameCache.putIfAbsent(host,host)
//        if(canonicalHost!=null)canonicalHost else host
//        }
//        }

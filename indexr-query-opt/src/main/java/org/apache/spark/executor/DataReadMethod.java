package org.apache.spark.executor;

/**
 * :: DeveloperApi ::
 * Method by which input data was read.  Network means that the data was read over the network
 * from a remote block manager (which may have stored the data on-disk or in-memory).
 */
public enum DataReadMethod {
    Memory, Disk, Hadoop, Network;
}

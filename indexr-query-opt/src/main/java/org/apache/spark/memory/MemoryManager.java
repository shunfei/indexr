package org.apache.spark.memory;

import com.google.common.base.Preconditions;

import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.LongLongMap;

import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.apache.spark.util.JavaUtils;

public class MemoryManager {
    public static final long maxOffHeapMemory = JavaUtils.byteStringAsBytes(System.getProperty("indexr.memory.offHeap.size", "5g"));
    public static final long pageSize = JavaUtils.byteStringAsBytes(System.getProperty("indexr.memory.offHeap.size", "64m"));


    private LongLongMap memoryForTask = new LongLongHashMap();
    private long totalUsedMemory;

    /**
     * The default page size, in bytes.
     * 
     * If user didn't explicitly set "spark.buffer.pageSize", we figure out the default value
     * by looking at the number of cores available to the process, and the total amount of memory,
     * and then divide it by a factor of safety.
     */
    public long pageSizeBytes() {
        if (pageSize != 0) {
            return pageSize;
        }

        long minPageSize = (long) 1024 * 1024;   // 1MB
        long maxPageSize = 64L * minPageSize;  // 64MB
        int cores = Runtime.getRuntime().availableProcessors();
        // Because of rounding to next power of 2, we may have safetyFactor as 8 in worst case
        int safetyFactor = 16;
        long size = nextPowerOf2(maxOffHeapMemory / cores / safetyFactor);
        return Math.min(maxPageSize, Math.max(minPageSize, size));
    }

    /** Returns the next number greater or equal num that is power of 2. */
    public static long nextPowerOf2(long num) {
        final long highBit = Long.highestOneBit(num);
        return (highBit == num) ? num : highBit << 1;
    }

    /**
     * Allocates memory for use by Unsafe/Tungsten code.
     */
    public MemoryAllocator tungstenMemoryAllocator() {
        return MemoryAllocator.UNSAFE;
    }

    public synchronized long acquireMemory(long required, long taskAttemptId) {
        long cur = memoryForTask.get(taskAttemptId);
        long got;
        if (totalUsedMemory + required > maxOffHeapMemory) {
            got = maxOffHeapMemory - totalUsedMemory;
        } else {
            got = required;
        }
        totalUsedMemory += got;
        memoryForTask.put(taskAttemptId, cur + got);
        return got;
    }

    public synchronized void releaseMemory(long size, long taskAttemptId) {
        long cur = memoryForTask.get(taskAttemptId);
        cur -= size;
        totalUsedMemory -= size;
        Preconditions.checkState(cur >= 0);
        Preconditions.checkState(totalUsedMemory >= 0);
        if (cur <= 0) {
            memoryForTask.remove(taskAttemptId);
        } else {
            memoryForTask.put(taskAttemptId, cur);
        }
    }

    public synchronized long getMemoryUsageForTask(long taskAttemptId) {
        return memoryForTask.get(taskAttemptId);
    }

    /**
     * Release all memory for the given task and mark it as inactive (e.g. when a task ends).
     *
     * @return the number of bytes freed.
     */
    public synchronized long releaseAllMemoryForTask(long taskAttemptId) {
        long numBytesToFree = memoryForTask.get(taskAttemptId);
        releaseMemory(numBytesToFree, taskAttemptId);
        return numBytesToFree;
    }
}

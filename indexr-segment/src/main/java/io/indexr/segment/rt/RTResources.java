package io.indexr.segment.rt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class RTResources {
    private static final Logger logger = LoggerFactory.getLogger(RTResources.class);

    private final boolean enableMemoryLimit;
    private final long maxMemory;
    private final AtomicLong curMemoryUsage = new AtomicLong(0);

    public RTResources(boolean enableMemoryLimit, long maxMemory) {
        this.enableMemoryLimit = enableMemoryLimit;
        this.maxMemory = maxMemory;
        logger.info("Max realtime memory: {}, enable: {}", maxMemory, enableMemoryLimit);
    }

    public long memoryUsage() {
        return curMemoryUsage.get();
    }

    public long freeMemory() {
        return maxMemory - curMemoryUsage.get();
    }

    public void decMemoryUsage(long bytes) {
        assert bytes >= 0;
        if (!enableMemoryLimit) {
            return;
        }
        curMemoryUsage.addAndGet(-bytes);
    }

    public void addMemoryUsageEx(long bytes) {
        assert bytes >= 0;
        if (!enableMemoryLimit) {
            return;
        }
        if (curMemoryUsage.addAndGet(bytes) >= maxMemory) {
            throw new OutOfMemoryError(String.format("Memory used by realtime full. Hint: %s",
                    hint()));
        }
    }

    public boolean reserveMemory(long bytes) {
        if (!enableMemoryLimit) {
            return true;
        }
        boolean ok = curMemoryUsage.get() + bytes < maxMemory;
        if (!ok) {
            logger.warn("Fail to reserve {}M free memory. Hint: {}",
                    bytes >>> 20, hint());
        }
        return ok;
    }

    public void reserveMemoryEx(long bytes) {
        if (!enableMemoryLimit) {
            return;
        }
        if (curMemoryUsage.get() + bytes >= maxMemory) {
            throw new OutOfMemoryError(String.format("Fail to reserve %sM free heap memory. Hint: %s",
                    bytes >>> 20, hint()));
        }
    }

    private String hint() {
        return String.format("Realtime memory aproximately usage: %sM+ , limit: %sM. " +
                        "You can try" +
                        "\n1. increase the max realtime memory setting" +
                        "\n2. twist realtime setting, like smaller max row in memory, more frequency time to save to disk, etc" +
                        "\n3. add more nodes and make fewer realtime tables per node",
                curMemoryUsage.get() >>> 20,
                maxMemory >>> 20);
    }
}

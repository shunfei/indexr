package io.indexr.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CombinedBlockingIterator<T> implements Closeable, Iterator<T> {
    private static final Logger logger = LoggerFactory.getLogger(CombinedBlockingIterator.class);

    private final BlockingQueue<T> queue;
    private final ExecutorService executor;
    private final AtomicInteger validCount;

    private volatile boolean closed = false;
    private T next;

    public CombinedBlockingIterator(List<Iterator<T>> iterators) {
        this.queue = new ArrayBlockingQueue<T>(Math.max(iterators.size(), 16));
        this.executor = Executors.newFixedThreadPool(iterators.size());
        this.validCount = new AtomicInteger(iterators.size());

        for (Iterator<T> itr : iterators) {
            executor.execute(() -> {
                try {
                    while (!closed && itr.hasNext()) {
                        queue.put(itr.next());
                    }
                } catch (Exception e) {
                    if (e instanceof InterruptedException) {
                        // Ignore.
                    } else {
                        logger.error("", e);
                    }
                }
                validCount.decrementAndGet();
            });
        }
    }

    @Override
    public boolean hasNext() {
        if (next != null) {
            return true;
        }
        try {
            while (true) {
                next = queue.poll(200, TimeUnit.MILLISECONDS);
                if (next != null) {
                    return true;
                }
                if (closed || validCount.get() == 0) {
                    return false;
                }
            }
        } catch (InterruptedException e) {
            return false;
        }
    }

    @Override
    public T next() {
        if (next == null) {
            throw new NoSuchElementException();
        }
        T t = next;
        next = null;
        return t;
    }

    @Override
    public void close() {
        closed = true;
        executor.shutdownNow();
    }
}

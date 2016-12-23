package io.indexr.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DelayTask {
    private Task task;
    private long runTime;

    public DelayTask(Task task, long delayMS) {
        this.task = task;
        this.runTime = System.currentTimeMillis() + delayMS;
    }

    private boolean isReady() {
        return runTime <= System.currentTimeMillis();
    }

    public void submit() {
        tasks.add(this);
    }

    public static interface Task {
        void run() throws Exception;
    }

    private static final Logger logger = LoggerFactory.getLogger(DelayTask.class);
    private static final Thread worker;
    private static final Collection<DelayTask> tasks = new ConcurrentLinkedQueue<>();

    static {
        worker = new Thread(DelayTask::work, "DelayTask-Worker");
        worker.setDaemon(true);
        worker.start();
    }


    private static void work() {
        while (!Thread.interrupted()) {
            try {
                doWork();
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                // Do nothing.
            } catch (Throwable t) {
                logger.error("", t);
            }
        }
    }

    private static void doWork() {
        Iterator<DelayTask> it = tasks.iterator();
        while (it.hasNext()) {
            DelayTask task = it.next();
            if (!task.isReady()) {
                continue;
            }
            it.remove();
            try {
                task.task.run();
            } catch (Throwable t) {
                logger.error("Run delay task failed.", t);
            }
        }
    }
}

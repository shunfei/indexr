package io.indexr.util;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public interface DelayRepeatTask {
    long run();

    void onStart();

    void onComplete();

    static void runTask(DelayRepeatTask task, long delayMS, ScheduledExecutorService execSvr) {
        task.onStart();
        doRunTask(task, delayMS, execSvr);
    }

    static void doRunTask(DelayRepeatTask task, long delayMS, ScheduledExecutorService execSvr) {
        if (delayMS < 0) {
            task.onComplete();
        } else {
            execSvr.schedule(
                    () -> doRunTask(task, task.run(), execSvr),
                    delayMS,
                    TimeUnit.MILLISECONDS);
        }
    }

    static void runTaskInThread(DelayRepeatTask task, long delayMS) {
        task.onStart();
        while (delayMS >= 0) {
            if (delayMS > 0) {
                try {
                    Thread.sleep(delayMS);
                } catch (InterruptedException e) {
                    // Do nothing.
                }
            }
            delayMS = task.run();
        }
        task.onComplete();
    }
}

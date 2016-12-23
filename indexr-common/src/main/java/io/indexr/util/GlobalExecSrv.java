package io.indexr.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class GlobalExecSrv {
    public static final ScheduledExecutorService EXECUTOR_SERVICE = Executors.newScheduledThreadPool(1);
}

package io.indexr.util;

import org.slf4j.Logger;

import java.io.InterruptedIOException;
import java.nio.channels.ClosedByInterruptException;

public class Try {

    public static void on(F0 f, Logger logger) {
        try {
            f.f();
        } catch (Throwable t) {
            logError(logger, t, "");
        }
    }

    public static boolean on(F0 f, int times, Logger logger, String... msg) {
        int count = 0;
        while (true) {
            try {
                f.f();
                return true;
            } catch (Throwable t) {
                count++;
                if (count >= times) {
                    logError(logger, t, msg);
                    return false;
                }
            }
        }
    }

    public static <T> T on(F1<T> f, int times, Logger logger, String... msg) {
        int count = 0;
        while (true) {
            try {
                return f.f();
            } catch (Throwable t) {
                count++;
                if (count >= times) {
                    logError(logger, t, msg);
                    return null;
                }
            }
        }
    }

    private static void logError(Logger logger, Throwable t, String... msg) {
        if (logger == null) {
            return;
        }
        String logStr = msg.length > 0 ? msg[0] : "";
        if (isDebugLog(t)) {
            logger.debug(logStr, t);
        } else {
            logger.error(logStr, t);
        }
    }

    private static boolean isDebugLog(Throwable t) {
        // Those exceptions are normal case most of time.
        return t instanceof InterruptedIOException
                || t instanceof ClosedByInterruptException
                || t instanceof InterruptedException;
    }

    @FunctionalInterface
    public static interface F0 {
        void f() throws Throwable;
    }

    @FunctionalInterface
    public static interface F1<T> {
        T f() throws Throwable;
    }
}

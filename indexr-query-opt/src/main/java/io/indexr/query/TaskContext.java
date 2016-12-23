package io.indexr.query;

import org.apache.spark.memory.TaskMemoryManager;

import java.util.function.Consumer;

public abstract class TaskContext {
    /**
     * Returns true if the task has completed.
     */
    public abstract boolean isCompleted();

    /**
     * Returns true if the task has been killed.
     */
    public abstract boolean isInterrupted();

    /**
     * Adds a (Java friendly) listener to be executed on task completion.
     * This will be called in all situation - success, failure, or cancellation.
     * An example use is for HadoopRDD to register a callback to close the input stream.
     */
    public abstract TaskContext addTaskCompletionListener(Consumer<TaskContext> f);

    public abstract long taskId();

    /** ::DeveloperApi:: */
    public abstract TaskMetrics taskMetrics();

    /**
     * Returns the manager for this task's managed memory.
     */
    public abstract TaskMemoryManager taskMemoryManager();


    /**
     * Return the currently active TaskContext. This can be called inside of
     * user functions to access contextual information about running tasks.
     */
    public static TaskContext get() {
        return taskContext.get();
    }

    private static ThreadLocal<TaskContext> taskContext = new ThreadLocal<TaskContext>();

    // Note: protected[spark] instead of private[spark] to prevent the following two from
    // showing up in JavaDoc.

    /**
     * Set the thread local TaskContext. Internal to Spark.
     */
    public static void setTaskContext(TaskContext tc) {
        taskContext.set(tc);
    }

    /**
     * Unset the thread local TaskContext. Internal to Spark.
     */
    public static void unset() {
        taskContext.remove();
    }

    /**
     * An empty task context that does not represent an actual task.
     */
    private TaskContextImpl empty() {
        return new TaskContextImpl(0, null);
    }

}

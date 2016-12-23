package io.indexr.query;

import org.apache.spark.memory.TaskMemoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class TaskContextImpl extends TaskContext {
    private static final Logger log = LoggerFactory.getLogger(TaskContextImpl.class);

    private long taskId;
    private TaskMemoryManager taskMemoryManager;
    private TaskMetrics taskMetrics = new TaskMetrics();

    // List of callback functions to execute when the task completes.
    private List<Consumer<TaskContext>> onCompleteCallbacks = new ArrayList<>();
    // Whether the corresponding task has been killed.
    private boolean interrupted = false;
    // Whether the task has completed.
    private boolean completed = false;

    public TaskContextImpl(long taskId, TaskMemoryManager taskMemoryManager) {
        this.taskId = taskId;
        this.taskMemoryManager = taskMemoryManager;
    }

    @Override
    public boolean isCompleted() {return completed;}

    @Override
    public boolean isInterrupted() {return interrupted;}

    @Override
    public long taskId() {return taskId;}

    @Override
    public TaskMetrics taskMetrics() {return taskMetrics;}

    @Override
    public TaskMemoryManager taskMemoryManager() {return taskMemoryManager;}

    @Override
    public TaskContext addTaskCompletionListener(Consumer<TaskContext> f) {
        onCompleteCallbacks.add(f);
        return this;
    }

    /** Marks the task as completed and triggers the listeners. */
    public List<Exception> markTaskCompleted() {
        completed = true;
        List<Exception> errMsgs = new ArrayList<>();
        onCompleteCallbacks.forEach(e -> {
            try {
                e.accept(this);
            } catch (Exception ex) {
                errMsgs.add(ex);
                log.error("Error in task completion listener", ex);
            }
        });
        return errMsgs;
    }

    public void markInterrupted() {
        interrupted = true;
    }
}

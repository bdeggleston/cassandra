package org.apache.cassandra.service;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;

public class StorageServiceExecutors
{
    /**
     * This pool is used for periodic short (sub-second) tasks.
     */
    public final DebuggableScheduledThreadPoolExecutor scheduledTasks = new DebuggableScheduledThreadPoolExecutor("ScheduledTasks");

    /**
     * This pool is used by tasks that can have longer execution times, and usually are non periodic.
     */
    public final DebuggableScheduledThreadPoolExecutor tasks = new DebuggableScheduledThreadPoolExecutor("NonPeriodicTasks");
    /**
     * tasks that do not need to be waited for on shutdown/drain
     */
    public final DebuggableScheduledThreadPoolExecutor optionalTasks = new DebuggableScheduledThreadPoolExecutor("OptionalTasks");

    public StorageServiceExecutors()
    {
        tasks.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }
}

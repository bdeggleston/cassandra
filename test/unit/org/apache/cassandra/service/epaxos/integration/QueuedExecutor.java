package org.apache.cassandra.service.epaxos.integration;

import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedTransferQueue;

public class QueuedExecutor extends AbstractExecutorService
{
    private Queue<Runnable> queue = new LinkedTransferQueue<>();
    private volatile int executed = 0;

    private final ArrayList<Runnable> postRunCallbacks = new ArrayList<>(10);
    private final long threadId;

    public QueuedExecutor()
    {
        threadId = Thread.currentThread().getId();
    }

    private void assertThread()
    {
        assert Thread.currentThread().getId() == threadId;
    }

    private synchronized void maybeRun(Runnable runnable)
    {
        assertThread();
        boolean wasEmpty = queue.isEmpty();
        queue.add(runnable);
        if (wasEmpty)
        {
            while (!queue.isEmpty())
            {
                Runnable nextTask = queue.peek();  // prevents the next added task thinking it should run
                nextTask.run();
                queue.remove();
                executed++;
                for (Runnable r: postRunCallbacks)
                {
                    r.run();
                }
            }
        }
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result)
    {
        assertThread();
        maybeRun(task);
        return null;
    }

    @Override
    public Future<?> submit(Runnable task)
    {
        assertThread();
        maybeRun(task);
        return null;
    }

    public int getExecuted()
    {
        assertThread();
        return executed;
    }

    public int queueSize()
    {
        assertThread();
        return queue.size();
    }

    public void addPostRunCallback(Runnable r)
    {
        assertThread();
        postRunCallbacks.add(r);
    }
}

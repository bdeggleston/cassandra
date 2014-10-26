package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.utils.Pair;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

public class BallotUpdateTask implements Runnable
{
    private final EpaxosState state;
    private final UUID id;
    private final int ballot;
    private final List<Pair<Stage, Runnable>> nextTasks = new LinkedList<>();

    public BallotUpdateTask(EpaxosState state, UUID id, int ballot)
    {
        this.state = state;
        this.id = id;
        this.ballot = ballot;
    }

    public void addNextTask(Stage stage, Runnable runnable)
    {
        nextTasks.add(Pair.create(stage, runnable));
    }

    @Override
    public void run()
    {
        ReadWriteLock lock = state.getInstanceLock(id);
        lock.writeLock().lock();
        try
        {
            Instance instance = state.loadInstance(id);
            instance.updateBallot(ballot);
            instance.incrementBallot();
        }
        finally
        {
            lock.writeLock().unlock();
        }

        for (Pair<Stage, Runnable> next: nextTasks)
        {
            state.getStage(next.left).submit(next.right);
        }
    }
}

package org.apache.cassandra.service.epaxos;

import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

public class BallotUpdateTask implements Runnable
{
    private final EpaxosState state;
    private final UUID id;
    private final int ballot;
    private final Runnable callback;

    public BallotUpdateTask(EpaxosState state, UUID id, int ballot, Runnable callback)
    {
        this.state = state;
        this.id = id;
        this.ballot = ballot;
        this.callback = callback;
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

        if (callback != null)
            callback.run();
    }
}

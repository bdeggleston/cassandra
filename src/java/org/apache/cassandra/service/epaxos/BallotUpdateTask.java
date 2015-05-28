package org.apache.cassandra.service.epaxos;

import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

public class BallotUpdateTask implements Runnable
{
    private final EpaxosService service;
    private final UUID id;
    private final int ballot;
    private final Runnable callback;

    public BallotUpdateTask(EpaxosService service, UUID id, int ballot, Runnable callback)
    {
        this.service = service;
        this.id = id;
        this.ballot = ballot;
        this.callback = callback;
    }

    @Override
    public void run()
    {
        ReadWriteLock lock = service.getInstanceLock(id);
        lock.writeLock().lock();
        try
        {
            Instance instance = service.loadInstance(id);
            instance.updateBallot(ballot);
            instance.incrementBallot();
            service.saveInstance(instance);
        }
        finally
        {
            lock.writeLock().unlock();
        }

        if (callback != null)
            callback.run();
    }
}

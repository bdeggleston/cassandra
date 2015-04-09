package org.apache.cassandra.service.epaxos;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;

/**
 * Garbage collects instances for the given token range, instances executed in
 * epochs older than currentEpoch - 1 can be safely deleted
 */
public class GarbageCollectionTask implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(GarbageCollectionTask.class);

    private final EpaxosState epaxosState;
    private final TokenState tokenState;
    private final KeyStateManager keyStateManager;

    public GarbageCollectionTask(EpaxosState epaxosState, TokenState tokenState, KeyStateManager keyStateManager)
    {
        this.epaxosState = epaxosState;
        this.tokenState = tokenState;
        this.keyStateManager = keyStateManager;
    }

    @Override
    public void run()
    {
        if (!tokenState.canGc())
        {
            logger.debug("Skipping GC task for {}", tokenState);
            return;
        }
        logger.debug("Running GC task for {}", tokenState);

        Long oldestEpoch;
        tokenState.lock.readLock().lock();
        try
        {
            oldestEpoch = tokenState.getEpoch() - 1;
        }
        finally
        {
            tokenState.lock.readLock().unlock();
        }

        logger.debug("Running GC task for {} with oldest epoch", tokenState, oldestEpoch);

        Iterator<CfKey> cfKeyIterator = keyStateManager.getCfKeyIterator(tokenState);
        while (cfKeyIterator.hasNext())
        {
            CfKey cfKey = cfKeyIterator.next();
            gcForKey(cfKey, oldestEpoch - 1);
        }
    }

    private void gcForKey(CfKey cfKey, Long oldestEpoch)
    {
        Map<Long, Set<UUID>> expiredEpochs;
        Lock lock = keyStateManager.getCfKeyLock(cfKey);
        lock.lock();
        try
        {
            KeyState keyState = keyStateManager.loadKeyState(cfKey.key, cfKey.cfId);
            if (keyState == null) return;
            expiredEpochs = keyState.getEpochsOlderThan(oldestEpoch);
        }
        finally
        {
            lock.unlock();
        }

        // this is done outside of the KeyState lock because:
        //   a) it would mean aquiring instance and key state locks in the incorrect order, leading to deadlocks
        //   b) since these are older epochs, they can't be modified
        for (Map.Entry<Long, Set<UUID>> entry: expiredEpochs.entrySet())
        {
            assert entry.getKey() < oldestEpoch;
            for (UUID id: entry.getValue())
            {
                logger.debug("deleting instance {}", id);
                epaxosState.deleteInstance(id);
            }
        }

        lock.lock();
        try
        {
            KeyState keyState = keyStateManager.loadKeyState(cfKey.key, cfKey.cfId);
            if (keyState == null) return;
            for (Long expiredEpoch: expiredEpochs.keySet())
            {
                keyState.removeEpoch(expiredEpoch);
            }
            keyStateManager.saveKeyState(cfKey.key, cfKey.cfId, keyState);
        }
        finally
        {
            lock.unlock();
        }
    }
}

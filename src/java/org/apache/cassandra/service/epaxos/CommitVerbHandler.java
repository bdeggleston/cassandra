package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.epaxos.exceptions.InvalidInstanceStateChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReadWriteLock;

public class CommitVerbHandler extends AbstractEpochVerbHandler<MessageEnvelope<Instance>>
{
    private static final Logger logger = LoggerFactory.getLogger(CommitVerbHandler.class);

    public CommitVerbHandler(EpaxosState state)
    {
        super(state);
    }

    @Override
    public void doEpochVerb(MessageIn<MessageEnvelope<Instance>> message, int id)
    {
        Instance remoteInstance = message.payload.contents;

        logger.debug("Commit request received from {} for {}", message.from, remoteInstance.getId());
        ReadWriteLock lock = state.getInstanceLock(remoteInstance.getId());
        lock.writeLock().lock();
        Instance instance;
        try
        {
            instance = state.loadInstance(remoteInstance.getId());
            boolean recordDeps;
            if (instance == null)
            {
                instance = remoteInstance.copyRemote();
                state.recordMissingInstance(instance);
                recordDeps = true;
            }
            else
            {
                recordDeps = instance.isPlaceholder();
                instance.applyRemote(remoteInstance);
            }
            instance.commit(remoteInstance.getDependencies());
            state.saveInstance(instance);
            if (recordDeps)
            {
                state.getCurrentDependencies(instance);
            }
            state.recordAcknowledgedDeps(instance);
            state.notifyCommit(remoteInstance.getId());
        }
        catch (InvalidInstanceStateChange e)
        {
            // got a duplicate commit message, no big deal
            logger.debug("Duplicate commit message received", e.getMessage());
            return;
        }
        finally
        {
            lock.writeLock().unlock();
        }

        state.execute(remoteInstance.getId());
    }

    @Override
    public boolean canPassiveRecord()
    {
        return true;
    }
}

package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.epaxos.exceptions.InvalidInstanceStateChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReadWriteLock;

class CommitVerbHandler implements IVerbHandler<Instance>
{
    private static final Logger logger = LoggerFactory.getLogger(CommitVerbHandler.class);

    private EpaxosState state;

    public CommitVerbHandler(EpaxosState state)
    {
        this.state = state;
    }

    @Override
    public void doVerb(MessageIn<Instance> message, int id)
    {
        logger.debug("Commit request received from {} for {}", message.from, message.payload.getId());
        ReadWriteLock lock = state.getInstanceLock(message.payload.getId());
        lock.writeLock().lock();
        Instance instance;
        try
        {
            Instance remoteInstance = message.payload;
            instance = state.loadInstance(remoteInstance.getId());
            if (instance == null)
            {
                instance = remoteInstance.copyRemote();
                state.recordMissingInstance(instance);
            } else
            {
                instance.applyRemote(remoteInstance);
            }
            instance.commit(remoteInstance.getDependencies());
            state.saveInstance(instance);
            state.recordAcknowledgedDeps(instance);
            state.notifyCommit(message.payload.getId());
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

        state.execute(message.payload.getId());
    }
}

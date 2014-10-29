package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.epaxos.exceptions.InvalidInstanceStateChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

public abstract class PreacceptTask implements Runnable
{
    protected static final Logger logger = LoggerFactory.getLogger(PreacceptTask.class);

    protected final EpaxosState state;
    protected final UUID id;
    private final Runnable failureCallback;

    protected PreacceptTask(EpaxosState state, UUID id, Runnable failureCallback)
    {
        this.state = state;
        this.id = id;
        this.failureCallback = failureCallback;
    }

    protected abstract Instance getInstance();
    protected abstract boolean forceAccept();

    @Override
    public void run()
    {
        logger.debug("preaccepting instance {}, {}", id, this.getClass().getSimpleName());
        Instance instanceCopy;
        EpaxosState.ParticipantInfo participantInfo;
        ReadWriteLock lock = state.getInstanceLock(id);
        lock.writeLock().lock();
        try
        {

            Instance instance = getInstance();

            if (instance.getState().atLeast(Instance.State.ACCEPTED))
            {
                if (failureCallback != null)
                {
                    // not technically a failure, but the task didn't
                    // complete the way it was expected to
                    failureCallback.run();
                }
                return;
            }

            participantInfo = state.getParticipants(instance);
            instance.preaccept(state.getCurrentDependencies(instance));
            if (instance.getSuccessors() == null)
                instance.setSuccessors(participantInfo.getSuccessors());
            instance.setFastPathImpossible(true);
            instance.incrementBallot();
            state.saveInstance(instance);

            instanceCopy = instance.copy();
        }
        catch (UnavailableException | InvalidInstanceStateChange e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            lock.writeLock().unlock();
        }
        sendMessage(instanceCopy, participantInfo);
    }

    protected void sendMessage(Instance instance, EpaxosState.ParticipantInfo participantInfo)
    {
        MessageOut<Instance> message = instance.getMessage(MessagingService.Verb.EPAXOS_PREACCEPT);
        PreacceptCallback callback = state.getPreacceptCallback(instance, participantInfo, failureCallback, forceAccept());

        for (InetAddress endpoint : participantInfo.liveEndpoints)
        {
            if (!endpoint.equals(state.getEndpoint()))
            {
                logger.debug("sending preaccept request to {} for instance {}", endpoint, instance.getId());
                state.sendRR(message, endpoint, callback);
            }
            else
            {
                logger.debug("counting self in preaccept quorum for instance {}", instance.getId());
                callback.countLocal();
            }
        }
    }

    public static class Leader extends PreacceptTask
    {

        private final Instance target;

        public Leader(EpaxosState state, Instance target)
        {
            super(state, target.getId(), null);
            this.target = target;
        }

        @Override
        protected Instance getInstance()
        {
            assert target.getState() == Instance.State.INITIALIZED;
            return target;
        }

        @Override
        protected boolean forceAccept()
        {
            return false;
        }
    }

    public static class Prepare extends PreacceptTask
    {

        private final boolean noop;

        public Prepare(EpaxosState state, UUID id, boolean noop, Runnable failureCallback)
        {
            super(state, id, failureCallback);
            this.noop = noop;
        }

        @Override
        protected Instance getInstance()
        {
            Instance instance = state.loadInstance(id);
            assert instance != null;
            instance.setNoop(noop);
            return instance;
        }

        @Override
        protected boolean forceAccept()
        {
            return true;
        }
    }
}

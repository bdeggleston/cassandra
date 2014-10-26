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
    protected static final Logger logger = LoggerFactory.getLogger(EpaxosState.class);

    protected final EpaxosState state;
    protected final UUID id;

    protected PreacceptTask(EpaxosState state, UUID id)
    {
        this.state = state;
        this.id = id;
    }

    protected abstract void sendMessage(Instance instance, EpaxosState.ParticipantInfo participantInfo);
    protected abstract Instance getInstance();

    @Override
    public void run()
    {
        logger.debug("preaccepting instance {}", id);
        PreacceptCallback callback;
        ReadWriteLock lock = state.getInstanceLock(id);
        lock.writeLock().lock();
        try
        {

            Instance instance = getInstance();

            EpaxosState.ParticipantInfo participantInfo = state.getParticipants(instance);
            instance.preaccept(state.getCurrentDependencies(instance));
            instance.setSuccessors(participantInfo.getSuccessors());
            instance.setFastPathImpossible(true);
            instance.incrementBallot();
            state.saveInstance(instance);

            sendMessage(instance, participantInfo);
        }
        catch (UnavailableException | InvalidInstanceStateChange e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }


    public static class Leader extends PreacceptTask
    {

        private final Instance target;

        public Leader(EpaxosState state, Instance target)
        {
            super(state, target.getId());
            this.target = target;
        }

        @Override
        protected Instance getInstance()
        {
            assert target.getState() == Instance.State.INITIALIZED;
            return target;
        }

        @Override
        protected void sendMessage(Instance instance, EpaxosState.ParticipantInfo participantInfo)
        {
            MessageOut<Instance> message = instance.getMessage(MessagingService.Verb.EPAXOS_PREACCEPT);
            PreacceptCallback callback = state.getPreacceptCallback(instance, participantInfo);

            if (state.getEndpoint().equals(instance.getLeader()))
            {
                logger.debug("counting self in preaccept quorum for instance {}", instance.getId());
                callback.countLocal();
            }
            for (InetAddress endpoint : participantInfo.liveEndpoints)
            {
                if (!endpoint.equals(state.getEndpoint()))
                {
                    logger.debug("sending preaccept request to {} for instance {}", endpoint, instance.getId());
                    state.sendRR(message, endpoint, callback);
                }
            }
        }
    }

    public static class Prepare extends PreacceptTask
    {

        private final boolean noop;

        public Prepare(EpaxosState state, UUID id, boolean noop)
        {
            super(state, id);
            this.noop = noop;
        }

        @Override
        protected void sendMessage(Instance instance, EpaxosState.ParticipantInfo participantInfo)
        {
            MessageOut<Instance> message = instance.getMessage(MessagingService.Verb.EPAXOS_PREACCEPT);
            PreacceptCallback callback = state.getPreacceptCallback(instance, participantInfo);
            for (InetAddress endpoint : participantInfo.liveEndpoints)
            {
                logger.debug("sending preaccept request to {} for instance {}", endpoint, instance.getId());
                state.sendRR(message, endpoint, callback);
            }
        }

        @Override
        protected Instance getInstance()
        {
            Instance instance = state.getInstanceCopy(id);
            assert instance != null;
            return instance;
        }
    }
}

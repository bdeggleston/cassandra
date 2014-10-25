package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.epaxos.exceptions.InvalidInstanceStateChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.concurrent.locks.ReadWriteLock;

public class PreacceptTask implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(EpaxosState.class);

    private final EpaxosState state;
    private final Instance instance;

    public PreacceptTask(EpaxosState state, Instance instance)
    {
        this.state = state;
        this.instance = instance;
    }

    @Override
    public void run()
    {
        logger.debug("preaccepting instance {}", instance.getId());
        PreacceptCallback callback;
        ReadWriteLock lock = state.getInstanceLock(instance.getId());
        lock.writeLock().lock();

        try
        {
            EpaxosState.ParticipantInfo participantInfo = state.getParticipants(instance);
            instance.preaccept(state.getCurrentDependencies(instance));
            instance.setSuccessors(participantInfo.getSuccessors());
            instance.setFastPathImpossible(true);
            instance.incrementBallot();
            state.saveInstance(instance);

            MessageOut<Instance> message = new MessageOut<>(MessagingService.Verb.EPAXOS_PREACCEPT,
                                                            instance,
                                                            Instance.serializer);
            callback = state.getPreacceptCallback(instance, participantInfo);
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
        catch (UnavailableException | InvalidInstanceStateChange e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }
}

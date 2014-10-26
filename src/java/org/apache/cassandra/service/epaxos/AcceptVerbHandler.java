package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.epaxos.exceptions.BallotException;
import org.apache.cassandra.service.epaxos.exceptions.InvalidInstanceStateChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReadWriteLock;

class AcceptVerbHandler implements IVerbHandler<AcceptRequest>
{
    private static final Logger logger = LoggerFactory.getLogger(AcceptVerbHandler.class);

    private final EpaxosState state;

    public AcceptVerbHandler(EpaxosState state)
    {
        this.state = state;
    }

    @Override
    public void doVerb(MessageIn<AcceptRequest> message, int id)
    {
        Instance remoteInstance = message.payload.instance;
        logger.debug("Accept request received from {} for {}", message.from, remoteInstance.getId());

        if (message.payload.missingInstances.size() > 0)
        {
            logger.debug("Adding {} missing instances from {}", message.payload.missingInstances.size(), message.from);
            for (Instance missing: message.payload.missingInstances)
            {
                if (!missing.getId().equals(message.payload.instance.getId()))
                {
                    state.addMissingInstance(missing);
                }
            }
        }

        ReadWriteLock lock = state.getInstanceLock(remoteInstance.getId());
        lock.writeLock().lock();
        Instance instance = null;
        try
        {
            instance = state.loadInstance(remoteInstance.getId());
            if (instance == null)
            {
                instance = remoteInstance.copyRemote();
                state.recordMissingInstance(instance);
            } else
            {
                instance.checkBallot(remoteInstance.getBallot());
                instance.applyRemote(remoteInstance);
            }
            instance.accept(remoteInstance.getDependencies());
            state.saveInstance(instance);

            logger.debug("Accept request from {} successful for {}", message.from, remoteInstance.getId());
            AcceptResponse response = new AcceptResponse(true, 0);
            state.sendReply(response.getMessage(), id, message.from);

            state.recordAcknowledgedDeps(instance);
        }
        catch (BallotException e)
        {
            logger.debug("Accept request from {} for {}, rejected. Old ballot", message.from, remoteInstance.getId());
            AcceptResponse response = new AcceptResponse(false, e.localBallot);
            state.sendReply(response.getMessage(), id, message.from);
        }
        catch (InvalidInstanceStateChange e)
        {
            // another node is working on a prepare phase that this node wasn't involved in.
            // as long as the dependencies are the same, reply with an ok, otherwise, something
            // has gone wrong
            assert instance.getDependencies().equals(remoteInstance.getDependencies());

            logger.debug("Accept request from {} for {}, rejected. State demotion", message.from, remoteInstance.getId());
            AcceptResponse response = new AcceptResponse(true, 0);
            state.sendReply(response.getMessage(), id, message.from);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }
}

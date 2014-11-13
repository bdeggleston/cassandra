package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

public class TryPreacceptVerbHandler implements IVerbHandler<TryPreacceptRequest>
{
    private static final Logger logger = LoggerFactory.getLogger(AcceptCallback.class);

    private EpaxosState state;

    public TryPreacceptVerbHandler(EpaxosState state)
    {
        this.state = state;
    }

    @Override
    public void doVerb(MessageIn<TryPreacceptRequest> message, int id)
    {
        logger.debug("TryPreaccept message received from {} for {}", message.from, message.payload.iid);
        // TODO: check ballot
        ReadWriteLock lock = state.getInstanceLock(message.payload.iid);
        lock.writeLock().lock();
        try
        {
            Instance instance = state.loadInstance(message.payload.iid);
            TryPreacceptDecision decision = handleTryPreaccept(instance, message.payload.dependencies);
            TryPreacceptResponse response = new TryPreacceptResponse(instance.getId(), decision);
            MessageOut<TryPreacceptResponse> reply = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE,
                                                                      response,
                                                                      TryPreacceptResponse.serializer);
            state.sendReply(reply, id, message.from);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    protected TryPreacceptDecision handleTryPreaccept(Instance instance, Set<UUID> dependencies)
    {
        // TODO: reread the try preaccept stuff, dependency management may break it if we're not careful
        logger.debug("Attempting TryPreaccept for {} with deps {}", instance.getId(), dependencies);

        // get the ids of instances the the message instance doesn't have in it's dependencies
        // TODO: use a different method to get deps, this one mutates the dependency manager
        Set<UUID> conflictIds = Sets.newHashSet(state.getCurrentDependencies(instance));
        conflictIds.removeAll(dependencies);
        conflictIds.remove(instance.getId());

        // if this node hasn't seen some of the proposed dependencies, don't preaccept them
        for (UUID dep: dependencies)
        {
            if (state.loadInstance(dep) == null)
            {
                logger.debug("Missing dep for TryPreaccept for {}, rejecting ", instance.getId());
                return TryPreacceptDecision.REJECTED;
            }
        }

        for (UUID id: conflictIds)
        {
            if (id.equals(instance.getId()))
                continue;

            Instance conflict = state.loadInstance(id);

            if (!conflict.getState().isCommitted())
            {
                // requiring the potential conflict to be committed can cause
                // an infinite prepare loop, so we just abort this prepare phase.
                // This instance will get picked up again for explicit prepare after
                // the other instances being prepared are successfully committed. Hopefully
                // this conflicting instance will be committed as well.
                logger.debug("TryPreaccept contended for {}, {} is not committed", instance.getId(), conflict.getId());
                return TryPreacceptDecision.CONTENDED;
            }

            // if the instance in question isn't a dependency of the potential
            // conflict, then it couldn't have been committed on the fast path
            if (!conflict.getDependencies().contains(instance.getId()))
            {
                logger.debug("TryPreaccept rejected for {}, not a dependency of conflicting instance", instance.getId());
                return TryPreacceptDecision.REJECTED;
            }
        }

        logger.debug("TryPreaccept accepted for {}, with deps", instance.getId(), dependencies);
        // set dependencies on this instance
        assert instance.getState() == Instance.State.PREACCEPTED;
        instance.setDependencies(dependencies);
        state.saveInstance(instance);

        return TryPreacceptDecision.ACCEPTED;
    }

}

package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

public class TryPreacceptVerbHandler extends AbstractEpochVerbHandler<TryPreacceptRequest>
{
    private static final Logger logger = LoggerFactory.getLogger(AcceptCallback.class);

    public TryPreacceptVerbHandler(EpaxosState state)
    {
        super(state);
    }

    @Override
    public void doEpochVerb(MessageIn<TryPreacceptRequest> message, int id)
    {
        logger.debug("TryPreaccept message received from {} for {}", message.from, message.payload.iid);
        // TODO: check ballot
        ReadWriteLock lock = state.getInstanceLock(message.payload.iid);
        lock.writeLock().lock();
        try
        {
            Instance instance = state.loadInstance(message.payload.iid);
            Pair<TryPreacceptDecision, Boolean> decision = handleTryPreaccept(instance, message.payload.dependencies);
            TryPreacceptResponse response = new TryPreacceptResponse(instance.getToken(),
                                                                     state.getCurrentEpoch(instance),
                                                                     instance.getId(),
                                                                     decision.left,
                                                                     decision.right);
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

    private boolean maybeVetoEpoch(Instance inst)
    {
        if (!(inst instanceof TokenInstance))
        {
            return false;
        }

        TokenInstance instance = (TokenInstance) inst;
        long currentEpoch = state.tokenStateManager.getEpoch(instance.getToken());

        if (instance.getEpoch() > currentEpoch + 1)
        {
            instance.setVetoed(true);
        }
        return instance.isVetoed();
    }

    protected Pair<TryPreacceptDecision, Boolean> handleTryPreaccept(Instance instance, Set<UUID> dependencies)
    {
        // TODO: reread the try preaccept stuff, dependency management may break it if we're not careful
        logger.debug("Attempting TryPreaccept for {} with deps {}", instance.getId(), dependencies);

        // get the ids of instances the the message instance doesn't have in it's dependencies
        // TODO: use a different method to get deps, this one mutates the dependency manager
        Set<UUID> conflictIds = Sets.newHashSet(state.getCurrentDependencies(instance));
        conflictIds.removeAll(dependencies);
        conflictIds.remove(instance.getId());

        boolean vetoed = maybeVetoEpoch(instance);


        // if this node hasn't seen some of the proposed dependencies, don't preaccept them
        for (UUID dep: dependencies)
        {
            if (state.loadInstance(dep) == null)
            {
                logger.debug("Missing dep for TryPreaccept for {}, rejecting ", instance.getId());
                return Pair.create(TryPreacceptDecision.REJECTED, vetoed);
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
                return Pair.create(TryPreacceptDecision.CONTENDED, vetoed);
            }

            // if the instance in question isn't a dependency of the potential
            // conflict, then it couldn't have been committed on the fast path
            if (!conflict.getDependencies().contains(instance.getId()))
            {
                logger.debug("TryPreaccept rejected for {}, not a dependency of conflicting instance", instance.getId());
                return Pair.create(TryPreacceptDecision.REJECTED, vetoed);
            }
        }

        logger.debug("TryPreaccept accepted for {}, with deps", instance.getId(), dependencies);
        // set dependencies on this instance
        assert instance.getState() == Instance.State.PREACCEPTED;
        instance.setDependencies(dependencies);
        state.saveInstance(instance);

        return Pair.create(TryPreacceptDecision.ACCEPTED, vetoed);
    }

}

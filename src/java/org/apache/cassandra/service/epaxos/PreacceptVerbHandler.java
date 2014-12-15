package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.epaxos.exceptions.BallotException;
import org.apache.cassandra.service.epaxos.exceptions.InvalidInstanceStateChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Handles preaccept requests.
 *
 * This should *not* be handling initial leader requests. The leader
 * should run a preaccept phase before sending preaccept messages to the other
 * participants, so the other participances will know if they agree with the
 * leader or not.
 */
public class PreacceptVerbHandler implements IVerbHandler<Instance>
{
    private static final Logger logger = LoggerFactory.getLogger(PreacceptVerbHandler.class);

    private final EpaxosState state;

    public PreacceptVerbHandler(EpaxosState state)
    {
        this.state = state;
    }

    private void maybeVetoEpoch(Instance inst)
    {
        if (!(inst instanceof TokenInstance))
            return;

        TokenInstance instance = (TokenInstance) inst;
        long currentEpoch = state.tokenStateManager.getEpoch(instance.getToken());

        if (instance.getEpoch() > currentEpoch + 1)
        {
            instance.setVetoed(true);
        }
    }

    @Override
    public void doVerb(MessageIn<Instance> message, int id)
    {
        logger.debug("Preaccept request received from {} for {}", message.from, message.payload.getId());

        PreacceptResponse response;
        Set<UUID> missingInstanceIds = null;

        ReadWriteLock lock = state.getInstanceLock(message.payload.getId());
        lock.writeLock().lock();
        try
        {
            Instance remoteInstance = message.payload;
            Instance instance = state.loadInstance(remoteInstance.getId());
            try
            {
                if (instance == null)
                {
                    // TODO: add to deps
                    instance = remoteInstance.copyRemote();
                }
                else
                {
                    instance.checkBallot(remoteInstance.getBallot());
                    instance.applyRemote(remoteInstance);
                }
                instance.preaccept(state.getCurrentDependencies(instance), remoteInstance.getDependencies());
                maybeVetoEpoch(instance);
                state.saveInstance(instance);

                if (instance.getLeaderAttrsMatch())
                {
                    logger.debug("Preaccept dependencies agree for {}", instance.getId());
                    response = PreacceptResponse.success(instance);
                }
                else
                {
                    logger.debug("Preaccept dependencies disagree for {}", instance.getId());
                    missingInstanceIds = Sets.difference(instance.getDependencies(), remoteInstance.getDependencies());
                    missingInstanceIds.remove(instance.getId());
                    response = PreacceptResponse.failure(instance);
                }
            }
            catch (BallotException e)
            {
                response = PreacceptResponse.ballotFailure(e.localBallot);
            }
            catch (InvalidInstanceStateChange e)
            {
                // another node is working on a prepare phase that this node wasn't involved in.
                // as long as the dependencies are the same, reply with an ok, otherwise, something
                // has gone wrong
                assert instance.getDependencies().equals(message.payload.getDependencies());

                response = PreacceptResponse.success(instance);
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
        response.missingInstances = Lists.newArrayList(Iterables.filter(state.getInstanceCopies(missingInstanceIds),
                                                                        Instance.skipPlaceholderPredicate));
        MessageOut<PreacceptResponse> reply = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE,
                                                               response,
                                                               PreacceptResponse.serializer);
        state.sendReply(reply, id, message.from);
    }
}

package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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
public class PreacceptVerbHandler extends AbstractEpochVerbHandler<MessageEnvelope<Instance>>
{
    private static final Logger logger = LoggerFactory.getLogger(PreacceptVerbHandler.class);

    protected PreacceptVerbHandler(EpaxosState state)
    {
        super(state);
    }

    private void maybeVetoEpoch(Instance inst)
    {
        if (!(inst instanceof TokenInstance))
            return;

        TokenInstance instance = (TokenInstance) inst;
        TokenState tokenState = state.tokenStateManager.get(instance.getToken());
        long currentEpoch = state.tokenStateManager.getEpoch(instance.getToken());

        if (instance.getEpoch() > currentEpoch + 1)
        {
            logger.debug("Epoch {} is greater than {} + 1", instance.getEpoch(), currentEpoch);
            instance.setVetoed(true);
        }
        else if (!state.keyStateManager.canIncrementToEpoch(tokenState, instance.getEpoch()))
        {
            logger.debug("KeyStateManager can't increment epoch to {}", instance.getEpoch());
            instance.setVetoed(true);
        }
    }

    @Override
    public void doEpochVerb(MessageIn<MessageEnvelope<Instance>> message, int id)
    {
        Instance remoteInstance = message.payload.contents;
        logger.debug("Preaccept request received from {} for {}", message.from, remoteInstance.getId());

        PreacceptResponse response;
        Set<UUID> missingInstanceIds = null;

        ReadWriteLock lock = state.getInstanceLock(remoteInstance.getId());
        lock.writeLock().lock();
        try
        {
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
                    response = PreacceptResponse.success(instance.getToken(),
                                                         state.getCurrentEpoch(instance.getToken()),
                                                         instance);
                }
                else
                {
                    logger.debug("Preaccept dependencies disagree for {}", instance.getId());
                    missingInstanceIds = Sets.difference(instance.getDependencies(), remoteInstance.getDependencies());
                    missingInstanceIds.remove(instance.getId());
                    response = PreacceptResponse.failure(instance.getToken(),
                                                         state.getCurrentEpoch(instance.getToken()),
                                                         instance);
                }
            }
            catch (BallotException e)
            {
                response = PreacceptResponse.ballotFailure(instance.getToken(),
                                                           state.getCurrentEpoch(instance.getToken()),
                                                           e.localBallot);
            }
            catch (InvalidInstanceStateChange e)
            {
                // another node is working on a prepare phase that this node wasn't involved in.
                // as long as the dependencies are the same, reply with an ok, otherwise, something
                // has gone wrong
                assert instance.getDependencies().equals(remoteInstance.getDependencies());

                response = PreacceptResponse.success(instance.getToken(),
                                                     state.getCurrentEpoch(instance.getToken()),
                                                     instance);
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

package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.epaxos.exceptions.BallotException;
import org.apache.cassandra.service.epaxos.exceptions.InvalidInstanceStateChange;
import org.apache.cassandra.utils.Pair;

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
        if (!(inst instanceof EpochInstance))
            return;

        EpochInstance instance = (EpochInstance) inst;
        TokenState tokenState = state.tokenStateManager.get(instance);
        long currentEpoch = state.tokenStateManager.getEpoch(instance);

        if (instance.getEpoch() > currentEpoch + 1)
        {
            logger.debug("Epoch {} is greater than {} + 1", instance.getEpoch(), currentEpoch);
            instance.setVetoed(true);
        }
        else if (!state.keyStateManager.canIncrementToEpoch(tokenState, instance.getEpoch()))
        {
            state.keyStateManager.canIncrementToEpoch(tokenState, instance.getEpoch());
            logger.debug("KeyStateManager can't increment epoch to {}", instance.getEpoch());
            instance.setVetoed(true);
        }
    }

    private void maybeMergeTokenRange(Instance inst, Range<Token> range)
    {
        if (!(inst instanceof TokenInstance))
            return;

        assert range != null;
        TokenInstance instance = (TokenInstance) inst;
        instance.mergeLocalSplitRange(range);
    }

    @Override
    public void doEpochVerb(MessageIn<MessageEnvelope<Instance>> message, final int id)
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
                    instance = remoteInstance.copyRemote();
                }
                else
                {
                    instance.checkBallot(remoteInstance.getBallot());
                    instance.applyRemote(remoteInstance);
                }
                Pair<Set<UUID>, Range<Token>> attrs = state.getCurrentDependencies(instance);
                instance.preaccept(attrs.left, remoteInstance.getDependencies());
                maybeVetoEpoch(instance);
                maybeMergeTokenRange(instance, attrs.right);
                state.saveInstance(instance);

                if (instance.getLeaderAttrsMatch())
                {
                    logger.debug("Preaccept dependencies agree for {}", instance.getId());
                    response = PreacceptResponse.success(instance.getToken(),
                                                         state.getCurrentEpoch(instance),
                                                         instance);
                }
                else
                {
                    logger.debug("Preaccept dependencies disagree for {}", instance.getId());
                    missingInstanceIds = Sets.difference(instance.getDependencies(), remoteInstance.getDependencies());
                    missingInstanceIds.remove(instance.getId());
                    response = PreacceptResponse.failure(instance.getToken(),
                                                         state.getCurrentEpoch(instance),
                                                         instance);
                }
            }
            catch (BallotException e)
            {
                response = PreacceptResponse.ballotFailure(instance.getToken(),
                                                           instance.getCfId(),
                                                           state.getCurrentEpoch(instance),
                                                           e.localBallot);
            }
            catch (InvalidInstanceStateChange e)
            {
                // another node is working on a prepare phase that this node wasn't involved in.
                // as long as the dependencies are the same, reply with an ok, otherwise, something
                // has gone wrong
                assert instance.getDependencies().equals(remoteInstance.getDependencies());

                response = PreacceptResponse.success(instance.getToken(),
                                                     state.getCurrentEpoch(instance),
                                                     instance);
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
        response.missingInstances = state.getInstanceCopies(missingInstanceIds);
        MessageOut<PreacceptResponse> reply = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE,
                                                               response,
                                                               PreacceptResponse.serializer);
        state.sendReply(reply, id, message.from);
    }
}

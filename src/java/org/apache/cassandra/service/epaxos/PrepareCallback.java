package org.apache.cassandra.service.epaxos;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.net.MessageIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.util.*;

public class PrepareCallback extends AbstractEpochCallback<MessageEnvelope<Instance>>
{
    private static final Logger logger = LoggerFactory.getLogger(PreacceptCallback.class);

    private final UUID id;
    private final int ballot;
    private final EpaxosState.ParticipantInfo participantInfo;
    private final PrepareGroup group;
    private final boolean instanceUnknown;
    private final Map<InetAddress, Instance> responses = Maps.newHashMap();

    private boolean completed = false;

    public PrepareCallback(EpaxosState state, UUID id, int ballot, EpaxosState.ParticipantInfo participantInfo, PrepareGroup group)
    {
        super(state);
        this.id = id;
        this.ballot = ballot;
        this.participantInfo = participantInfo;
        this.group = group;
        instanceUnknown = ballot == 0;

        if (instanceUnknown)
        {
            assert state.loadInstance(id) == null;
        }
    }

    @Override
    public synchronized void epochResponse(MessageIn<MessageEnvelope<Instance>> msg)
    {
        logger.debug("prepare response received from {} for instance {}", msg.from, id);


        if (completed)
        {
            logger.debug("ignoring prepare response from {} for instance {}. prepare messaging completed", msg.from, id);
            return;
        }

        if (responses.containsKey(msg.from))
        {
            logger.debug("ignoring duplicate prepare response from {} for instance {}.", msg.from, id);
            return;
        }

        Instance msgInstance = msg.payload.contents;

        if (msgInstance != null)
        {
            if (instanceUnknown)
            {
                completed = true;
                state.addMissingInstance(msgInstance);
                return;
            }
            else if (msgInstance.getBallot() > ballot)
            {
                // TODO: should we only try n times? if so start sending attempt # along
                completed = true;
                state.updateBallot(id, msgInstance.getBallot(), new PrepareTask(state, id, group));
                return;
            }
        }

        responses.put(msg.from, msgInstance);

        if (responses.size() >= participantInfo.quorumSize)
        {
            completed = true;
            PrepareDecision decision = getDecision();
            logger.debug("prepare decision for {}: {}", id, decision);

            if (decision.commitNoop && instanceUnknown)
            {
                throw new AssertionError("noop required for unknown instance");
            }

            // if any of the next steps fail, they should report
            // the prepare phase as complete for this instance
            // so the prepare is tried again
            Runnable failureCallback = new Runnable()
            {
                public void run()
                {
                    group.prepareComplete(id);
                }
            };

            switch (decision.state)
            {
                case PREACCEPTED:
                    if (!decision.tryPreacceptAttempts.isEmpty())
                    {
                        List<TryPreacceptAttempt> attempts = decision.tryPreacceptAttempts;
                        state.tryPreaccept(id, attempts, participantInfo, failureCallback);
                    }
                    else
                    {
                        state.preacceptPrepare(id, decision.commitNoop, failureCallback);
                    }
                    break;
                case ACCEPTED:
                    state.accept(id, decision.deps, decision.vetoed, failureCallback);
                    break;
                case COMMITTED:
                    state.commit(id, decision.deps);
                    break;
                default:
                    throw new AssertionError();
            }
        }
    }

    private final Predicate<Instance> committedPredicate = new Predicate<Instance>()
    {
        @Override
        public boolean apply(@Nullable Instance instance)
        {
            if (instance == null)
                return false;
            Instance.State state = instance.getState();
            return state == Instance.State.COMMITTED || state == Instance.State.EXECUTED;
        }
    };

    private final Predicate<Instance> acceptedPredicate = new Predicate<Instance>()
    {
        @Override
        public boolean apply(@Nullable Instance instance)
        {
            if (instance == null)
                return false;
            Instance.State state = instance.getState();
            return state == Instance.State.ACCEPTED;
        }
    };

    private final Predicate<Instance> notNullPredicate = new Predicate<Instance>()
    {
        @Override
        public boolean apply(@Nullable Instance instance)
        {
            return instance != null;
        }
    };

    public synchronized PrepareDecision getDecision()
    {
        int ballot = 0;
        boolean vetoed = false;
        for (Instance inst: responses.values())
        {
            if (inst != null)
            {
                ballot = Math.max(ballot, inst.getBallot());
                if (inst instanceof EpochInstance)
                {
                    vetoed |= ((EpochInstance) inst).isVetoed();
                }
            }
        }

        List<Instance> committed = Lists.newArrayList(Iterables.filter(responses.values(), committedPredicate));
        if (!committed.isEmpty())
            return new PrepareDecision(Instance.State.COMMITTED, committed.get(0).getDependencies(), vetoed, ballot);

        List<Instance> accepted = Lists.newArrayList(Iterables.filter(responses.values(), acceptedPredicate));
        if (!accepted.isEmpty())
            return new PrepareDecision(Instance.State.ACCEPTED, accepted.get(0).getDependencies(), vetoed, ballot);

        // no other node knows about this instance, commit a noop
        if (Lists.newArrayList(Iterables.filter(responses.values(), notNullPredicate)).isEmpty())
            return new PrepareDecision(Instance.State.PREACCEPTED, null, vetoed, ballot, Collections.<TryPreacceptAttempt>emptyList(), true);

        return new PrepareDecision(Instance.State.PREACCEPTED, null, vetoed, ballot, getTryPreacceptAttempts(), false);
    }

    /**
     * Attempts to work out if there are any dependency sets that a preaccept should be attempted
     * for, and returns them in the order that they should be tried. Dependency groups that have
     * instances that agree with the original leader take precedence over ones that do not.
     *
     * If the command leader is one of the replicas that responded, and it hasn't committed this
     * instance, then no replica would have committed it, and we fall back to a normal preaccept
     * phase, and committing on the slow path (accept phase required).
     */
    private List<TryPreacceptAttempt> getTryPreacceptAttempts()
    {
        // Check for the leader of the instance. If it didn't commit on the
        // fast path, no on did, and there's no use running a TryPreaccept
        for (Map.Entry<InetAddress, Instance> entry: responses.entrySet())
        {
            if (entry.getValue() != null && entry.getKey().equals(entry.getValue().getLeader()))
            {
                return Collections.emptyList();
            }
        }

        // group common responses
        Set<InetAddress> replyingReplicas = Sets.newHashSet();
        Map<Set<UUID>, Set<InetAddress>> depGroups = Maps.newHashMap();
        final Map<Set<UUID>, Integer> scores = Maps.newHashMap();
        for (Map.Entry<InetAddress, Instance> entry: responses.entrySet())
        {
            if (entry.getValue() == null)
            {
                continue;
            }

            Set<UUID> deps = entry.getValue().getDependencies();
            if (!depGroups.containsKey(deps))
            {
                depGroups.put(deps, Sets.<InetAddress>newHashSet());
                scores.put(deps, 0);
            }
            depGroups.get(deps).add(entry.getKey());
            replyingReplicas.add(entry.getKey());

            scores.put(deps, (scores.get(deps) + (entry.getValue().getLeaderAttrsMatch() ? 2 : 1)));
        }

        // min # of identical preaccepts
        int minIdentical = (participantInfo.F + 1) / 2;
        List<TryPreacceptAttempt> attempts = Lists.newArrayListWithCapacity(depGroups.size());
        for (Map.Entry<Set<UUID>, Set<InetAddress>> entry: depGroups.entrySet())
        {
            Set<UUID> deps = entry.getKey();
            Set<InetAddress> nodes = entry.getValue();

            if (nodes.size() < minIdentical)
                continue;

            Set<InetAddress> toConvince = Sets.difference(replyingReplicas, nodes);
            int requiredConvinced = participantInfo.F + 1 - nodes.size();
            TryPreacceptAttempt attempt = new TryPreacceptAttempt(deps, toConvince, requiredConvinced, nodes);
            attempts.add(attempt);
        }

        // sort the attempts, attempts with instances that agreed
        // with the leader should be tried first
        Comparator<TryPreacceptAttempt> attemptComparator = new Comparator<TryPreacceptAttempt>()
        {
            @Override
            public int compare(TryPreacceptAttempt o1, TryPreacceptAttempt o2)
            {
                return scores.get(o2.dependencies) - scores.get(o1.dependencies);
            }
        };
        Collections.sort(attempts, attemptComparator);

        return attempts;
    }

    @VisibleForTesting
    boolean isCompleted()
    {
        return completed;
    }

    @VisibleForTesting
    int getNumResponses()
    {
        return responses.size();
    }

    @VisibleForTesting
    boolean isInstanceUnknown()
    {
        return instanceUnknown;
    }

    @Override
    public boolean isLatencyForSnitch()
    {
        return false;
    }
}

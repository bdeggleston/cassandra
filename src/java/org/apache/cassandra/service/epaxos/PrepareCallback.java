package org.apache.cassandra.service.epaxos;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.epaxos.exceptions.BallotException;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.util.*;

public class PrepareCallback extends AbstractEpaxosCallback<Instance>
{

    private final Instance instance;
    private final int ballot;
    private final Map<InetAddress, Instance> responses = Maps.newHashMap();

    public PrepareCallback(Instance instance, EpaxosManager.ParticipantInfo participantInfo)
    {
        super(participantInfo);
        this.instance = instance;
        ballot = instance.getBallot();
    }

    @Override
    public synchronized void response(MessageIn<Instance> msg)
    {
        responses.put(msg.from, msg.payload);
        latch.countDown();
    }

    public synchronized void countLocal(InetAddress endpoint, Instance instance)
    {
        responses.put(endpoint, instance);
        countLocal();
    }

    private Predicate<Instance> committedPredicate = new Predicate<Instance>()
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

    private Predicate<Instance> acceptedPredicate = new Predicate<Instance>()
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

    private Predicate<Instance> notNullPredicate = new Predicate<Instance>()
    {
        @Override
        public boolean apply(@Nullable Instance instance)
        {
            return instance != null;
        }
    };

    public synchronized PrepareDecision getDecision(Instance instance) throws BallotException
    {
        int maxBallot = 0;
        for (Instance inst: responses.values())
            maxBallot = Math.max(maxBallot, inst.getBallot());

        if (maxBallot > ballot)
            throw new BallotException(instance, maxBallot);

        List<Instance> committed = Lists.newArrayList(Iterables.filter(responses.values(), committedPredicate));
        if (committed.size() > 0)
            return new PrepareDecision(Instance.State.COMMITTED, committed.get(0).getDependencies());

        List<Instance> accepted = Lists.newArrayList(Iterables.filter(responses.values(), acceptedPredicate));
        if (accepted.size() > 0)
            return new PrepareDecision(Instance.State.ACCEPTED, accepted.get(0).getDependencies());

        // no other node knows about this instance, commit a noop
        if (Lists.newArrayList(Iterables.filter(responses.values(), notNullPredicate)).size() == 0)
            return new PrepareDecision(Instance.State.PREACCEPTED, null, null, true);

        return new PrepareDecision(Instance.State.PREACCEPTED, null, getTryPreacceptAttempts(), false);
    }

    private List<TryPreacceptAttempt> getTryPreacceptAttempts()
    {
        // check for fast path being impossible
        for (Instance instance: responses.values())
            if (instance.isFastPathImpossible())
                return Collections.EMPTY_LIST;

        //
        Set<InetAddress> replyingReplicas = Sets.newHashSet();
        Map<Set<UUID>, Set<InetAddress>> depGroups = Maps.newHashMap();
        final Map<Set<UUID>, Integer> scores = Maps.newHashMap();
        for (Map.Entry<InetAddress, Instance> entry: responses.entrySet())
        {
            Set<UUID> deps = entry.getValue().getDependencies();
            if (!depGroups.containsKey(deps))
            {
                depGroups.put(deps, Sets.<InetAddress>newHashSet());
                scores.put(deps, 0);
            }
            depGroups.get(deps).add(entry.getKey());
            replyingReplicas.add(entry.getKey());

            scores.put(deps, (scores.get(deps) + (instance.getLeaderDepsMatch() ? 2 : 1)));
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
            TryPreacceptAttempt attempt = new TryPreacceptAttempt(deps, toConvince, nodes);
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

}

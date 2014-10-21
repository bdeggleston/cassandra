package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.epaxos.exceptions.BallotException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class PreacceptCallback extends AbstractEpaxosCallback<PreacceptResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(PreacceptCallback.class);

    private final Instance instance;
    private final Set<UUID> dependencies;
    private final Set<UUID> remoteDependencies = Sets.newHashSet();
    private final Map<InetAddress, PreacceptResponse> responses = Maps.newHashMap();
    private final Map<UUID, Instance> missingInstances = Maps.newHashMap();

    private int ballotFailure = 0;
    private int localResponse = 0;

    public PreacceptCallback(Instance instance, EpaxosService.ParticipantInfo participantInfo)
    {
        super(participantInfo);
        this.instance = instance;
        this.dependencies = instance.getDependencies();
    }

    @Override
    public void response(MessageIn<PreacceptResponse> msg)
    {
        PreacceptResponse response = msg.payload;

        logger.debug("preaccept response received from {} for instance {}", msg.from, instance.getId());
        // another replica has taken control of this instance
        if (response.ballotFailure > 0)
        {

            logger.debug("preaccept ballot failure from {} for instance {}", msg.from, instance.getId());
            ballotFailure = Math.max(ballotFailure, response.ballotFailure);
            while (latch.getCount() > 0)
                latch.countDown();
            return;
        }
        responses.put(msg.from, response);

        remoteDependencies.addAll(response.dependencies);

        for (Instance missingInstance: response.missingInstances)
        {
            Instance previous = missingInstances.get(missingInstance.getId());
            if (previous != null && previous.getBallot() > missingInstance.getBallot())
                continue;
            missingInstances.put(missingInstance.getId(), missingInstance);
        }

        latch.countDown();
    }

    @Override
    public void countLocal()
    {
        super.countLocal();
        localResponse = 1;
    }

    public synchronized void checkBallotFailure() throws BallotException
    {
        if (ballotFailure > 0)
            throw new BallotException(instance, ballotFailure);
    }

    // returns deps for an accept phase if all responses didn't agree with the leader,
    // or a fast quorum didn't respond. Otherwise, null is returned
    public synchronized AcceptDecision getAcceptDecision() throws BallotException
    {
        checkBallotFailure();
        boolean depsMatch = dependencies.equals(remoteDependencies);

        // the fast path quorum may be larger than the simple quorum, so getResponseCount can't be used
        boolean fpQuorum = (responses.size() + localResponse) >= participantInfo.fastQuorumSize;

        Set<UUID> unifiedDeps = Sets.union(dependencies, remoteDependencies);

        Map<InetAddress, Set<UUID>> missingIds = Maps.newHashMap();
        for (Map.Entry<InetAddress, PreacceptResponse> entry: responses.entrySet())
        {
            Set<UUID> diff = Sets.difference(unifiedDeps, entry.getValue().dependencies);
            if (diff.size() > 0)
            {
                diff.remove(instance.getId());
                missingIds.put(entry.getKey(), diff);
            }
        }

        AcceptDecision decision = new AcceptDecision((!depsMatch || !fpQuorum), unifiedDeps, missingIds);
        logger.debug("preaccept accept decision for {}: {}", instance.getId(), decision);
        return decision;
    }

    Iterable<Instance> getMissingInstances()
    {
        return missingInstances.values();
    }
}

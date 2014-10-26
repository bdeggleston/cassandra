package org.apache.cassandra.service.epaxos;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class PreacceptCallback implements IAsyncCallback<PreacceptResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(PreacceptCallback.class);

    private final EpaxosState state;
    private final UUID iid;
    private final Set<UUID> dependencies;
    private final EpaxosState.ParticipantInfo participantInfo;
    private final Set<UUID> remoteDependencies = Sets.newHashSet();
    private final Map<InetAddress, PreacceptResponse> responses = Maps.newHashMap();
    private final Map<UUID, Instance> missingInstances = Maps.newHashMap();

    private boolean completed = false;
    private int numResponses = 0;
    private int ballotFailure = 0;
    private int localResponse = 0;

    public PreacceptCallback(EpaxosState state, Instance instance, EpaxosState.ParticipantInfo participantInfo)
    {
        this.state = state;
        this.iid = instance.getId();
        this.dependencies = instance.getDependencies();
        this.participantInfo = participantInfo;
    }

    @Override
    public synchronized void response(MessageIn<PreacceptResponse> msg)
    {
        if (completed)
            return;

        logger.debug("preaccept response received from {} for instance {}", msg.from, iid);
        PreacceptResponse response = msg.payload;

        // another replica has taken control of this instance
        if (response.ballotFailure > 0)
        {

            logger.debug("preaccept ballot failure from {} for instance {}", msg.from, iid);
            ballotFailure = Math.max(ballotFailure, response.ballotFailure);

            completed = true;
            return;
        }
        responses.put(msg.from, response);

        remoteDependencies.addAll(response.dependencies);

        if (response.missingInstances.size() > 0)
        {
            state.getStage(Stage.MUTATION).submit(new AddMissingInstances(state, response.missingInstances));
        }

        numResponses++;
        if (numResponses >= participantInfo.quorumSize)
        {
            AcceptDecision decision = getAcceptDecision();
            processDecision(decision);
        }
    }

    protected void processDecision(AcceptDecision decision)
    {
        if (decision.acceptNeeded)
        {
            state.accept(iid, decision);
        }
        else
        {
            state.commit(iid, decision.acceptDeps, true);
        }
    }

    public synchronized void countLocal()
    {
        numResponses++;
        localResponse = 1;
    }

    // returns deps for an accept phase if all responses didn't agree with the leader,
    // or a fast quorum didn't respond. Otherwise, null is returned
    public synchronized AcceptDecision getAcceptDecision()
    {
        boolean depsMatch = dependencies.equals(remoteDependencies);

        // the fast path quorum may be larger than the simple quorum, so getResponseCount can't be used
        boolean fpQuorum = (responses.size() + localResponse) >= participantInfo.fastQuorumSize;

        Set<UUID> unifiedDeps = ImmutableSet.copyOf(Iterables.concat(dependencies, remoteDependencies));

        Map<InetAddress, Set<UUID>> missingIds = Maps.newHashMap();
        for (Map.Entry<InetAddress, PreacceptResponse> entry: responses.entrySet())
        {
            Set<UUID> diff = Sets.difference(unifiedDeps, entry.getValue().dependencies);
            if (diff.size() > 0)
            {
                diff.remove(iid);
                missingIds.put(entry.getKey(), diff);
            }
        }

        AcceptDecision decision = new AcceptDecision((!depsMatch || !fpQuorum), unifiedDeps, missingIds);
        logger.debug("preaccept accept decision for {}: {}", iid, decision);
        return decision;
    }

    @Override
    public boolean isLatencyForSnitch()
    {
        return false;
    }
}

package org.apache.cassandra.service.epaxos;

import com.google.common.collect.*;
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
    private final UUID id;
    private final Set<UUID> dependencies;
    private final EpaxosState.ParticipantInfo participantInfo;
    private final Runnable failureCallback;
    private final Set<UUID> remoteDependencies = Sets.newHashSet();
    private final Map<InetAddress, PreacceptResponse> responses = Maps.newHashMap();
    private final boolean forceAccept;

    private boolean completed = false;
    private int numResponses = 0;
    private int ballot = 0;

    public PreacceptCallback(EpaxosState state, Instance instance, EpaxosState.ParticipantInfo participantInfo, Runnable failureCallback, boolean forceAccept)
    {
        this.state = state;
        this.id = instance.getId();
        this.dependencies = instance.getDependencies();
        this.participantInfo = participantInfo;
        this.failureCallback = failureCallback;
        this.forceAccept = forceAccept;
    }

    @Override
    public synchronized void response(MessageIn<PreacceptResponse> msg)
    {
        if (completed)
        {
            logger.debug("ignoring preaccept response from {} for instance {}. preaccept messaging completed", msg.from, id);
            return;
        }

        logger.debug("preaccept response received from {} for instance {}", msg.from, id);
        PreacceptResponse response = msg.payload;

        // another replica has taken control of this instance
        if (response.ballotFailure > 0)
        {
            logger.debug("preaccept ballot failure from {} for instance {}", msg.from, id);
            ballot = Math.max(ballot, response.ballotFailure);
            completed = true;

            BallotUpdateTask ballotTask = new BallotUpdateTask(state, id, ballot);
            if (failureCallback != null)
                ballotTask.addNextTask(null, failureCallback);
            state.getStage(Stage.MUTATION).submit(ballotTask);
            return;
        }
        responses.put(msg.from, response);

        remoteDependencies.addAll(response.dependencies);

        if (response.missingInstances.size() > 0)
        {
            state.getStage(Stage.MUTATION).submit(new AddMissingInstances(state, response.missingInstances));
        }

        numResponses++;
        maybeDecideResult();
    }

    protected void maybeDecideResult()
    {
        if (numResponses >= participantInfo.quorumSize)
        {
            completed = true;
            AcceptDecision decision = getAcceptDecision();
            processDecision(decision);
        }
    }

    protected void processDecision(AcceptDecision decision)
    {
        if (decision.acceptNeeded || forceAccept)
        {
            logger.debug("preaccept messaging completed for {}, running accept phase", id);
            state.accept(id, decision, failureCallback);
        }
        else
        {
            logger.debug("preaccept messaging completed for {}, committing on fast path", id);
            state.commit(id, decision.acceptDeps);
        }
    }

    public synchronized void countLocal()
    {
        numResponses++;
        maybeDecideResult();
    }

    // returns deps for an accept phase if all responses didn't agree with the leader,
    // or a fast quorum didn't respond. Otherwise, null is returned
    private AcceptDecision getAcceptDecision()
    {
        boolean depsMatch = dependencies.equals(remoteDependencies);

        // the fast path quorum may be larger than the simple quorum, so getResponseCount can't be used
        boolean fpQuorum = numResponses >= participantInfo.fastQuorumSize;

        Set<UUID> unifiedDeps = ImmutableSet.copyOf(Iterables.concat(dependencies, remoteDependencies));

        Map<InetAddress, Set<UUID>> missingIds = Maps.newHashMap();
        for (Map.Entry<InetAddress, PreacceptResponse> entry: responses.entrySet())
        {
            Set<UUID> diff = Sets.difference(unifiedDeps, entry.getValue().dependencies);
            if (diff.size() > 0)
            {
                diff.remove(id);
                missingIds.put(entry.getKey(), diff);
            }
        }

        AcceptDecision decision = new AcceptDecision((!depsMatch || !fpQuorum), unifiedDeps, missingIds);
        logger.debug("preaccept accept decision for {}: {}", id, decision);
        return decision;
    }

    @Override
    public boolean isLatencyForSnitch()
    {
        return false;
    }
}

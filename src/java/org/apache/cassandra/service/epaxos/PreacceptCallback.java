package org.apache.cassandra.service.epaxos;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.net.MessageIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class PreacceptCallback extends AbstractEpochCallback<PreacceptResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(PreacceptCallback.class);

    private final UUID id;
    private final Set<UUID> dependencies;
    private final EpaxosState.ParticipantInfo participantInfo;
    private final Runnable failureCallback;
    private final Set<UUID> remoteDependencies = Sets.newHashSet();
    private boolean vetoed = false;  // only used for token instances
    private final Map<InetAddress, PreacceptResponse> responses = Maps.newHashMap();
    private final boolean forceAccept;
    private final Set<InetAddress> endpointsReplied = Sets.newHashSet();
    private final int expectedResponses;

    private boolean completed = false;
    private boolean localResponse = false;
    private int numResponses = 0;

    public PreacceptCallback(EpaxosState state, Instance instance, EpaxosState.ParticipantInfo participantInfo, Runnable failureCallback, boolean forceAccept)
    {
        super(state);
        this.id = instance.getId();
        this.dependencies = instance.getDependencies();
        this.participantInfo = participantInfo;
        this.failureCallback = failureCallback;
        this.forceAccept = forceAccept;
        expectedResponses = participantInfo.fastQuorumExists() ? participantInfo.fastQuorumSize : participantInfo.quorumSize;
    }

    @Override
    public synchronized void epochResponse(MessageIn<PreacceptResponse> msg)
    {
        logger.debug("preaccept response received from {} for instance {}. {}", msg.from, id, msg.payload);
        if (completed)
        {
            logger.debug("ignoring preaccept response from {} for instance {}. preaccept messaging completed", msg.from, id);
            return;
        }

        if (endpointsReplied.contains(msg.from))
        {
            logger.debug("ignoring duplicate preaccept response from {} for instance {}.", msg.from, id);
            return;
        }
        endpointsReplied.add(msg.from);

        PreacceptResponse response = msg.payload;

        // another replica has taken control of this instance
        if (response.ballotFailure > 0)
        {
            logger.debug("preaccept ballot failure from {} for instance {}", msg.from, id);
            completed = true;
            state.updateBallot(id, response.ballotFailure, failureCallback);
            return;
        }
        responses.put(msg.from, response);

        remoteDependencies.addAll(response.dependencies);
        vetoed |= response.vetoed;

        if (response.missingInstances.size() > 0)
        {
            state.getStage(Stage.MUTATION).submit(new AddMissingInstances(state, response.missingInstances));
        }

        numResponses++;
        maybeDecideResult();
    }

    protected void maybeDecideResult()
    {
        if (numResponses >= expectedResponses && localResponse)
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
        localResponse = true;
        maybeDecideResult();
    }

    @VisibleForTesting
    AcceptDecision getAcceptDecision()
    {
        boolean depsMatch = dependencies.equals(remoteDependencies);

        // the fast path quorum may be larger than the simple quorum, so getResponseCount can't be used
        boolean fpQuorum = numResponses >= participantInfo.fastQuorumSize;

        Set<UUID> unifiedDeps = ImmutableSet.copyOf(Iterables.concat(dependencies, remoteDependencies));

        boolean acceptRequired = !depsMatch || !fpQuorum || vetoed;

        Map<InetAddress, Set<UUID>> missingIds = Maps.newHashMap();
        if (acceptRequired)
        {
            for (Map.Entry<InetAddress, PreacceptResponse> entry: responses.entrySet())
            {
                Set<UUID> diff = Sets.difference(unifiedDeps, entry.getValue().dependencies);
                if (diff.size() > 0)
                {
                    diff.remove(id);
                    missingIds.put(entry.getKey(), diff);
                }
            }
        }

        AcceptDecision decision = new AcceptDecision(acceptRequired, unifiedDeps, vetoed, missingIds);
        logger.debug("preaccept accept decision for {}: {}", id, decision);
        return decision;
    }

    public boolean isCompleted()
    {
        return completed;
    }

    public int getNumResponses()
    {
        return numResponses;
    }

    @Override
    public boolean isLatencyForSnitch()
    {
        return false;
    }
}

package org.apache.cassandra.service.epaxos;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.epaxos.exceptions.BallotException;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class PreacceptCallback extends AbstractEpaxosCallback<PreacceptResponse>
{
    private final Instance instance;
    private final Set<UUID> dependencies;
    private final Set<UUID> remoteDependencies = Sets.newHashSet();
    private final Map<InetAddress, PreacceptResponse> responses = Maps.newHashMap();
    private final Map<UUID, Instance> missingInstances = Maps.newHashMap();

    private int ballotFailure = 0;
    private int localResponse = 0;

    public PreacceptCallback(Instance instance, EpaxosManager.ParticipantInfo participantInfo)
    {
        super(participantInfo);
        this.instance = instance;
        this.dependencies = instance.getDependencies();
    }

    @Override
    public synchronized void response(MessageIn<PreacceptResponse> msg)
    {
        PreacceptResponse response = msg.payload;

        // another replica has taken control of this instance
        if (response.ballotFailure > 0)
        {
            ballotFailure = Math.max(ballotFailure, response.ballotFailure);
            while (latch.getCount() > 0)
                latch.countDown();
            return;
        }
        responses.put(msg.from, response);

        remoteDependencies.addAll(response.dependencies);


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

        return new AcceptDecision((!depsMatch || !fpQuorum),
                                  ImmutableSet.copyOf(Iterables.concat(dependencies, remoteDependencies)));
    }
}

package org.apache.cassandra.service.epaxos;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.epaxos.exceptions.BallotException;
import org.apache.cassandra.service.paxos.AbstractPaxosCallback;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class PreacceptCallback extends AbstractPaxosCallback<PreacceptResponse>
{
    private final Instance instance;
    private final Set<UUID> dependencies;
    private final Set<UUID> remoteDependencies = Sets.newHashSet();
    private final EpaxosManager.ParticipantInfo participantInfo;
    private final Map<InetAddress, PreacceptResponse> responses = Maps.newHashMap();
    private final Map<UUID, Instance> missingInstances = Maps.newHashMap();

    private int ballotFailure = 0;

    public PreacceptCallback(Instance instance, EpaxosManager.ParticipantInfo participantInfo)
    {
        super(participantInfo.quorumSize, participantInfo.consistencyLevel);
        this.instance = instance;
        this.dependencies = instance.getDependencies();
        this.participantInfo = participantInfo;
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

    public synchronized void checkBallotFailure() throws BallotException
    {
        if (ballotFailure > 0)
            throw new BallotException(instance, ballotFailure);
    }

    // returns deps for an accept phase if all responses didn't agree with the leader,
    // or a fast quorum didn't respond. Otherwise, null is returned
    public synchronized Set<UUID> getAcceptDeps() throws BallotException
    {
        checkBallotFailure();
        if (!dependencies.equals(remoteDependencies) || responses.size() < participantInfo.fastQuorumSize)
            return ImmutableSet.copyOf(Iterables.concat(dependencies, remoteDependencies));

        return null;
    }
}

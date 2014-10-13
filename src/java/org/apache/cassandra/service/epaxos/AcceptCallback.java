package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.epaxos.exceptions.BallotException;
import org.apache.cassandra.service.paxos.AbstractPaxosCallback;

public class AcceptCallback extends AbstractPaxosCallback<AcceptResponse>
{
    private final Instance instance;
    private final EpaxosManager.ParticipantInfo participantInfo;
    private boolean success;
    private int ballot;

    public AcceptCallback(Instance instance, EpaxosManager.ParticipantInfo participantInfo)
    {
        super(participantInfo.quorumSize, participantInfo.consistencyLevel);
        this.instance = instance;
        this.participantInfo = participantInfo;
    }

    @Override
    public synchronized void response(MessageIn<AcceptResponse> msg)
    {
        AcceptResponse response = msg.payload;

        if (!response.success)
        {
            ballot = Math.max(ballot, response.ballot);
            success = false;
            while (latch.getCount() > 0)
                latch.countDown();
            return;
        }

        latch.countDown();
    }

    public void checkSuccess() throws BallotException
    {
        if (!success)
            throw new BallotException(instance, ballot);
    }
}

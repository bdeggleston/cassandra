package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.epaxos.exceptions.BallotException;

public class AcceptCallback extends AbstractEpaxosCallback<AcceptResponse>
{
    private final Instance instance;
    private boolean success = true;
    private int ballot = 0;

    public AcceptCallback(Instance instance, EpaxosService.ParticipantInfo participantInfo)
    {
        super(participantInfo);
        this.instance = instance;
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

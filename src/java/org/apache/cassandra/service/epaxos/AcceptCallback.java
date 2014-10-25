package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.epaxos.exceptions.BallotException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AcceptCallback extends AbstractEpaxosCallback<AcceptResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(AcceptCallback.class);

    private final Instance instance;
    private boolean success = true;
    private int ballot = 0;

    public AcceptCallback(Instance instance, EpaxosState.ParticipantInfo participantInfo)
    {
        super(participantInfo);
        this.instance = instance;
    }

    @Override
    public synchronized void response(MessageIn<AcceptResponse> msg)
    {
        logger.debug("preaccept response received from {} for instance {}", msg.from, instance.getId());
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

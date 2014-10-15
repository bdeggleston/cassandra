package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.paxos.AbstractPaxosCallback;

public class TryPreacceptCallback extends AbstractPaxosCallback<TryPreacceptResponse>
{
    private int convinced = 0;

    public TryPreacceptCallback(TryPreacceptAttempt attempt, EpaxosManager.ParticipantInfo participantInfo)
    {
        super(attempt.requiredConvinced, participantInfo.consistencyLevel);
    }

    @Override
    public void response(MessageIn<TryPreacceptResponse> msg)
    {
        // TODO: should wait for more than `targets`? Or should a single negative response abort the attempt?
        if (msg.payload.success)
            convinced++;
        latch.countDown();
    }

    public void localResponse(boolean success)
    {
        if (success)
            convinced++;
        latch.countDown();
    }

    public boolean successful()
    {
        return convinced >= targets;
    }
}

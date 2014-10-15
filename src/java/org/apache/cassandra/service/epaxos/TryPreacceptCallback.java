package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.epaxos.exceptions.PrepareAbortException;
import org.apache.cassandra.service.paxos.AbstractPaxosCallback;

public class TryPreacceptCallback extends AbstractPaxosCallback<TryPreacceptResponse>
{
    private final Instance instance;
    private int convinced = 0;
    private boolean contended = false;

    public TryPreacceptCallback(Instance instance, TryPreacceptAttempt attempt, EpaxosManager.ParticipantInfo participantInfo)
    {
        super(attempt.requiredConvinced, participantInfo.consistencyLevel);
        this.instance = instance;
    }

    @Override
    public synchronized void response(MessageIn<TryPreacceptResponse> msg)
    {
        // TODO: should wait for more than `targets`? Or should a single negative response abort the attempt?
        recordDecision(msg.payload.decision);
    }

    public synchronized void recordDecision(TryPreacceptDecision decision)
    {
        if (decision == TryPreacceptDecision.ACCEPTED)
        {
            convinced++;
        }
        else if (decision == TryPreacceptDecision.CONTENDED)
        {
            contended = true;
        }
        latch.countDown();
    }

    public synchronized boolean successful() throws PrepareAbortException
    {
        if (convinced >= targets)
            return true;

        if (contended)
            throw new PrepareAbortException(instance, "Contended try preaccept");

        return false;
    }
}

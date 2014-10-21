package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.epaxos.exceptions.PrepareAbortException;
import org.apache.cassandra.service.paxos.AbstractPaxosCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class TryPreacceptCallback extends AbstractPaxosCallback<TryPreacceptResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(PreacceptCallback.class);

    private final UUID instanceId;
    private int convinced = 0;
    private boolean contended = false;

    public TryPreacceptCallback(UUID instanceId, TryPreacceptAttempt attempt, EpaxosService.ParticipantInfo participantInfo)
    {
        super(attempt.requiredConvinced, participantInfo.consistencyLevel);
        this.instanceId = instanceId;
    }

    @Override
    public synchronized void response(MessageIn<TryPreacceptResponse> msg)
    {
        logger.debug("preaccept response received from {} for instance {}", msg.from, instanceId);
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
            throw new PrepareAbortException(instanceId, "Contended try preaccept");

        return false;
    }
}

package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.net.MessageIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class TryPreacceptCallback extends AbstractEpochCallback<TryPreacceptResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(PreacceptCallback.class);

    private final UUID id;
    private final TryPreacceptAttempt attempt;
    private final List<TryPreacceptAttempt> nextAttempts;
    private final EpaxosState.ParticipantInfo participantInfo;
    private final Runnable failureCallback;

    private int responses = 0;
    private int convinced = 0;
    private boolean vetoed = false;
    private boolean contended = false;
    private boolean completed = false;

    public TryPreacceptCallback(EpaxosState state, UUID id, TryPreacceptAttempt attempt, List<TryPreacceptAttempt> nextAttempts, EpaxosState.ParticipantInfo participantInfo, Runnable failureCallback)
    {
        super(state);
        this.id = id;
        this.attempt = attempt;
        this.nextAttempts = nextAttempts;
        this.participantInfo = participantInfo;
        this.failureCallback = failureCallback;
    }

    @Override
    public synchronized void epochResponse(MessageIn<TryPreacceptResponse> msg)
    {
        if (completed)
            return;

        logger.debug("preaccept response received from {} for instance {}", msg.from, id);
        TryPreacceptResponse response = msg.payload;

        responses++;
        vetoed |= response.vetoed;

        if (response.decision == TryPreacceptDecision.ACCEPTED)
        {
            convinced++;
        }
        else if (response.decision == TryPreacceptDecision.CONTENDED)
        {
            // stop prepare phase for this instance
            contended = true;
        }

        if (responses >= attempt.requiredConvinced)
        {
            completed = true;

            if (convinced >= attempt.requiredConvinced)
            {
                // try-preaccept successful
                state.accept(id, attempt.dependencies, vetoed, failureCallback);
            }
            else if (contended)
            {
                // need to wait for other instances to be committed,  tell the
                // prepare group prepare was completed for this id. It will get
                // picked up while preparing the other instances, and another
                // prepare task will be started if it's not committed
                if (failureCallback != null)
                    failureCallback.run();
            }
            else
            {
                // try-preaccept unsuccessful
                if (!nextAttempts.isEmpty())
                {
                    // start the next trypreaccept
                    state.tryPreaccept(id, nextAttempts, participantInfo, failureCallback);
                }
                else
                {
                    // fall back to regular preaccept
                    state.preacceptPrepare(id, false, failureCallback);
                }
            }
        }
    }

    @Override
    public boolean isLatencyForSnitch()
    {
        return false;
    }
}

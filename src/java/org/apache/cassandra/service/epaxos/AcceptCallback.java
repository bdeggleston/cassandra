package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.UUID;

public class AcceptCallback implements IAsyncCallback<AcceptResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(AcceptCallback.class);

    private final EpaxosState state;
    private final UUID id;
    private final EpaxosState.ParticipantInfo participantInfo;
    private final Runnable failureCallback;
    private final int proposedBallot;
    private final Set<UUID> proposedDependencies;

    private boolean completed = false;
    private int numResponses = 0;

    public AcceptCallback(EpaxosState state, Instance instance, EpaxosState.ParticipantInfo participantInfo, Runnable failureCallback)
    {
        this.state = state;
        this.id = instance.getId();
        this.proposedBallot = instance.getBallot();
        this.proposedDependencies = instance.getDependencies();
        this.participantInfo = participantInfo;
        this.failureCallback = failureCallback;
    }

    @Override
    public synchronized void response(MessageIn<AcceptResponse> msg)
    {
        if (completed)
            return;

        logger.debug("accept response received from {} for instance {}", msg.from, id);
        AcceptResponse response = msg.payload;

        if (!response.success)
        {
            logger.debug("proposed ballot rejected for accept response {} <= {}", proposedBallot, response.ballot);
            completed = true;

            BallotUpdateTask ballotTask = new BallotUpdateTask(state, id, response.ballot);
            if (failureCallback != null)
                ballotTask.addNextTask(null, failureCallback);
            state.getStage(Stage.MUTATION).submit(ballotTask);
            return;
        }

        numResponses++;
        if (numResponses >= participantInfo.quorumSize)
        {
            state.commit(id, proposedDependencies);
        }
    }

    @Override
    public boolean isLatencyForSnitch()
    {
        return false;
    }
}

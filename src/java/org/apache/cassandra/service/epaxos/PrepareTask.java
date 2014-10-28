package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class PrepareTask implements Runnable, ICommitCallback
{
    private static final Logger logger = LoggerFactory.getLogger(EpaxosState.class);

    // the number of times a prepare phase will try to gain control of an instance before giving up
    protected static int PREPARE_BALLOT_FAILURE_RETRIES = 5;

    // the amount of time the prepare phase will wait for the leader to commit an instance before
    // attempting a prepare phase. This is multiplied by a replica's position in the successor list
    protected static int PREPARE_GRACE_MILLIS = 2000;

    private final EpaxosState state;
    private final UUID id;
    private final PrepareGroup group;

    private volatile boolean committed;

    private PrepareTask(EpaxosState state, UUID id, PrepareGroup group)
    {
        this.state = state;
        this.id = id;
        this.group = group;
    }

    public static PrepareTask create(EpaxosState state, UUID id, PrepareGroup group)
    {
        PrepareTask prepareTask = new PrepareTask(state, id, group);
        state.registerCommitCallback(id, prepareTask);
        return prepareTask;
    }

    private boolean shouldPrepare(Instance instance)
    {
        return !instance.getState().atLeast(Instance.State.COMMITTED);
    }

    @Override
    public void run()
    {
        // TODO: how to prevent multiple pending tasks? Prepare lock isn't the best option here
        if (committed)
        {
            group.instanceCommitted(id);
            logger.debug("Instance {} was committed", id);
            return;
        }


        // if there's another prepare in progress for this instance, tell
        // it to rerun this one when it finishes. This prevents a single
        // node from running multiple concurrent prepare phases for the
        // same instance.
        // the api however, kinda sucks
        // TODO: make not suck
        while (true)
        {
            PrepareGroup previous = state.registerPrepareGroup(id, group);
            if (previous == null)
            {
                break;
            }
            else if (previous.addCompleteRunnable(Stage.READ, this))
            {
                return;
            }
        }

//        PrepareGroup previous = state.registerPrepareGroup(id, group);
//        if (previous != null)
//        {
//            group.prepareComplete(id);
//            return;
//        }

        Instance instance = state.getInstanceCopy(id);

        // if we don't have a copy of the instance yet, tell the prepare
        // group prepare was completed for this id. It should get picked
        // up while preparing the other instances, and another prepare
        // task will be started if it's not committed
        if (instance == null)
        {
            if (group.size() == 1)
            {
                logger.error("Single missing instance for prepare: ", id);
            }
            group.instanceCommitted(id);
            return;
        }

        if (!shouldPrepare(instance))
        {
            group.instanceCommitted(id);
            state.notifyCommit(id);
            return;
        }

        // maybe wait for grace period to end
        long wait = state.getPrepareWaitTime(instance.getLastUpdated());

        if (wait > 0)
        {
            logger.debug("Delaying {} prepare task for {} ms", id, wait);
            StorageService.optionalTasks.schedule(new DelayedPrepare(this), wait, TimeUnit.MILLISECONDS);
            return;
        }

        // TODO: maybe defer to successor

        instance.incrementBallot();
        EpaxosState.ParticipantInfo participantInfo;
        try
        {
            participantInfo = state.getParticipants(instance);
        }
        catch (UnavailableException e)
        {
            throw new RuntimeException(e);
        }

        PrepareRequest request = new PrepareRequest(instance);
        PrepareCallback callback = state.getPrepareCallback(instance, participantInfo, group);
        MessageOut<PrepareRequest> message = request.getMessage();
        for (InetAddress endpoint: participantInfo.liveEndpoints)
        {
            state.sendRR(message, endpoint, callback);
        }
    }

    @Override
    public void instanceCommitted(UUID id)
    {
        if (this.id.equals(id))
        {
            committed = true;
        }
    }

    private static class DelayedPrepare implements Runnable
    {

        private final PrepareTask task;

        private DelayedPrepare(PrepareTask task)
        {
            this.task = task;
        }

        @Override
        public void run()
        {
            if (task.committed)
            {
                logger.debug("Instance {} was committed", task.id);
                return;
            }
            task.state.getStage(Stage.MUTATION).submit(task);
        }
    }
}


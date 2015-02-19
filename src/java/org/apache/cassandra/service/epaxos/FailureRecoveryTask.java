package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.FutureTask;

/**
 * Performs a recovery of epaxos data for a given token range.
 *
 * 1. Pre-recovery.
 *      * token state is set to PRE_RECOVERY. This prevents it from participating in any
 *          epaxos instances, or executing any instances.
 *      * token state is set to the remote epoch value we're trying to recover towards
 *      * all of the key states owned by the recovering token state, and the instances owned
 *          by those key states are deleted.
 * 2. Instance recovery.
 *      * token state is set to RECOVERING_INSTANCES. While it will not participate in epaxos
 *          instances, it will accept and record accept and commit messages.
 *      * key states for the given token range are read in from the other nodes.
 *      * instances from remote keystates are retrieved from other nodes
 * 3. Repair
 *      * token state is set to RECOVERING_DATA. It will now participate in epaxos instances, although
 *          it will not execute committed instances.
 *      * a repair session is started for the given token range and cfid
 *      * repair streams include epaxos header data that tells the local key states where the
 *          local dataset has been executed to. Instance executions are a no-op until the key
 *          state has caught up to the streamed data.
 * 4. Normal
 *      * token state is now repaired and is participating in, and executing instances.
 */
public class FailureRecoveryTask implements Runnable
{
    // TODO: convert each method to a Runnable/StreamEventListener/RepairEventListener, etc
    private static final Logger logger = LoggerFactory.getLogger(EpaxosState.class);

    private final EpaxosState state;
    private final Token token;
    private final UUID cfId;

    // the remote epoch that caused the failure recovery
    private final long epoch;

    public FailureRecoveryTask(EpaxosState state, Token token, UUID cfId, long epoch)
    {
        this.state = state;
        this.token = token;
        this.cfId = cfId;
        this.epoch = epoch;
    }

    protected TokenState getTokenState()
    {
        return state.tokenStateManager.get(token, cfId);
    }

    protected String getKeyspace()
    {
        return Schema.instance.getCF(cfId).left;
    }

    protected String getColumnFamily()
    {
        return Schema.instance.getCF(cfId).right;
    }

    protected Collection<InetAddress> getEndpoints(Range<Token> range)
    {
        // TODO: check that left token will return the correct endpoints
        // TODO: perform some other checks to make sure the requested token state and token metadata are on the same page
        return StorageService.instance.getNaturalEndpoints(getKeyspace(), range.left);
    }

    /**
     * Set the relevant token state to PRE_RECOVERY, which will prevent any
     * participation then delete all data owned by the recovering token manager.
     */
    void preRecover()
    {
        // set token state status to recovering
        // stop participating in any epaxos instances
        TokenState tokenState = getTokenState();

        // bail out if we're not actually behind
        if (tokenState.getState() == TokenState.State.NORMAL && tokenState.getEpoch() >= epoch)
        {
            return;
        }

        tokenState.rwLock.writeLock().lock();
        try
        {
            tokenState.setState(TokenState.State.PRE_RECOVERY);
            tokenState.setEpoch(epoch);
            state.tokenStateManager.save(tokenState);
        }
        finally
        {
            tokenState.rwLock.writeLock().unlock();
        }

        // erase data for all keys owned by recovering token manager
        KeyStateManager ksm = state.keyStateManager;
        Iterator<CfKey> cfKeys = ksm.getCfKeyIterator(tokenState);
        while (cfKeys.hasNext())
        {
            Set<UUID> toDelete = new HashSet<>();
            CfKey cfKey = cfKeys.next();
            ksm.getCfKeyLock(cfKey).lock();
            try
            {
                KeyState ks = ksm.loadKeyState(cfKey);

                toDelete.addAll(ks.getActiveInstanceIds());
                for (Set<UUID> ids: ks.getEpochExecutions().values())
                {
                    toDelete.addAll(ids);
                }
                ksm.deleteKeyState(cfKey);
            }
            finally
            {
                ksm.getCfKeyLock(cfKey).unlock();
            }

            // aquiring the instance lock after the key state lock can create
            // a deadlock, so we get all the instance ids we want to delete,
            // then delete them after we're done deleting the key state
            for (UUID id: toDelete)
            {
                state.deleteInstance(id);
            }
        }
    }

    /**
     * Recovers current instances by streaming them from other replicas.
     * At this point, the recovering node will start receiving accepts and commits, but will not participation or execute instances
     */
    void recoverInstances()
    {
        // TODO: only try one replica at a time
        TokenState tokenState = getTokenState();
        Range<Token> range;
        tokenState.rwLock.writeLock().lock();
        try
        {
            if (tokenState.getState() != TokenState.State.PRE_RECOVERY)
            {

                logger.info("Aborting instance recovery for {}. Status is {}, expected {}",
                            tokenState, tokenState.getState(), TokenState.State.PRE_RECOVERY);
                return;
            }

            tokenState.setState(TokenState.State.RECOVERING_INSTANCES);
            state.tokenStateManager.save(tokenState);
            range = tokenState.getRange();
        }
        finally
        {
            tokenState.rwLock.writeLock().unlock();
        }

        final StreamPlan streamPlan = new StreamPlan(tokenState.toString() + "-Instance-Recovery");

        // TODO: don't request data from ALL
        for (InetAddress endpoint: getEndpoints(range))
        {
            streamPlan.requestEpaxosRange(endpoint, cfId, range);
        }

        streamPlan.listeners(new StreamEventHandler()
        {
            private boolean submitted = false;

            public void handleStreamEvent(StreamEvent event)
            {
                if (event.eventType == StreamEvent.Type.STREAM_COMPLETE && !submitted)
                {
                    logger.debug("Instance stream complete. Submitting data recovery task");
                    state.getStage(Stage.MISC).submit(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            recoverData();
                        }
                    });
                    submitted = true;
                }
            }

            public void onSuccess(@Nullable StreamState streamState) {}

            public void onFailure(Throwable throwable) {}
        });
        streamPlan.execute();
    }

    /**
     * Start a repair task that repairs the affected range.
     * Replica can now participate in instances, but won't execute the instances
     */
    void recoverData()
    {
        TokenState tokenState = getTokenState();
        Range<Token> range;
        boolean localOnly;
        tokenState.rwLock.writeLock().lock();
        try
        {
            if (tokenState.getState() != TokenState.State.RECOVERING_INSTANCES)
            {

                logger.info("Aborting instance recovery for {}. Status is {}, expected {}",
                            tokenState, tokenState.getState(), TokenState.State.PRE_RECOVERY);
                return;
            }

            tokenState.setState(TokenState.State.RECOVERING_DATA);
            state.tokenStateManager.save(tokenState);
            range = tokenState.getRange();
            localOnly = tokenState.localOnly();
        }
        finally
        {
            tokenState.rwLock.writeLock().unlock();
        }

        Pair<String, String> cfName = Schema.instance.getCF(cfId);
        FutureTask<Object> future = StorageService.instance.createRepairTask(cfName.left,
                                                                             Collections.singleton(range),
                                                                             false,
                                                                             localOnly,
                                                                             true,
                                                                             cfName.right);

        new Thread(new FutureTask<Object>(future, null) {
            @Override
            protected void done()
            {
                super.done();
                complete();
            }
        }).start();
    }

    /**
     * return the token state to a normal state
     */
    void complete()
    {
        TokenState tokenState = getTokenState();
        tokenState.rwLock.writeLock().lock();
        try
        {
            tokenState.setState(TokenState.State.NORMAL);
            state.tokenStateManager.save(tokenState);
        }
        finally
        {
            tokenState.rwLock.writeLock().unlock();
        }
    }

    @Override
    public void run()
    {
        preRecover();
        recoverInstances();
    }
}

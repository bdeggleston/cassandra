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

    public final EpaxosState state;
    public final Token token;
    public final UUID cfId;

    // the remote epoch that caused the failure recovery
    private final long epoch;

    public FailureRecoveryTask(EpaxosState state, Token token, UUID cfId, long epoch)
    {
        this.state = state;
        this.token = token;
        this.cfId = cfId;
        this.epoch = epoch;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FailureRecoveryTask that = (FailureRecoveryTask) o;

        if (epoch != that.epoch) return false;
        if (!cfId.equals(that.cfId)) return false;
        if (!token.equals(that.token)) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = token.hashCode();
        result = 31 * result + cfId.hashCode();
        result = 31 * result + (int) (epoch ^ (epoch >>> 32));
        return result;
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

        tokenState.lock.writeLock().lock();
        try
        {
            tokenState.setState(TokenState.State.PRE_RECOVERY);
            state.tokenStateManager.save(tokenState);
        }
        finally
        {
            tokenState.lock.writeLock().unlock();
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
        TokenState tokenState = getTokenState();
        Range<Token> range;
        tokenState.lock.writeLock().lock();
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
            range = state.tokenStateManager.rangeFor(tokenState);
        }
        finally
        {
            tokenState.lock.writeLock().unlock();
        }

        final StreamPlan streamPlan = new StreamPlan(tokenState.toString() + "-Instance-Recovery");

        for (InetAddress endpoint: getEndpoints(range))
        {
            if (endpoint.equals(state.getEndpoint()))
                continue;
            streamPlan.requestEpaxosRange(endpoint, cfId, range);
        }

        streamPlan.listeners(new StreamEventHandler()
        {
            private boolean submitted = false;

            public synchronized void handleStreamEvent(StreamEvent event)
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
//
//    /**
//     * Start a repair task that repairs the affected range.
//     * Replica can now participate in instances, but won't execute the instances
//     */
//    void recoverData()
//    {
//        TokenState tokenState = getTokenState();
//        Range<Token> range;
//        tokenState.lock.writeLock().lock();
//        try
//        {
//            if (tokenState.getState() != TokenState.State.RECOVERING_INSTANCES)
//            {
//
//                logger.info("Aborting instance recovery for {}. Status is {}, expected {}",
//                            tokenState, tokenState.getState(), TokenState.State.PRE_RECOVERY);
//                return;
//            }
//
//            tokenState.setState(TokenState.State.RECOVERING_DATA);
//            state.tokenStateManager.save(tokenState);
//            range = state.tokenStateManager.rangeFor(tokenState);
//        }
//        finally
//        {
//            tokenState.lock.writeLock().unlock();
//        }
//
//        final StreamPlan streamPlan = new StreamPlan(tokenState.toString() + "-Instance-Recovery");
//
//        // TODO: don't request data from ALL
//        for (InetAddress endpoint: getEndpoints(range))
//        {
//            if (endpoint.equals(state.getEndpoint()))
//                continue;
//            Pair<String, String> table = Schema.instance.getCF(cfId);
//            streamPlan.requestRanges(endpoint, table.left, Collections.singleton(range), table.right);
//        }
//
//        streamPlan.listeners(new StreamEventHandler()
//        {
//            private boolean submitted = false;
//
//            public synchronized void handleStreamEvent(StreamEvent event)
//            {
//                // TODO: die on error
//                if (event.eventType == StreamEvent.Type.STREAM_COMPLETE && !submitted)
//                {
//                    logger.debug("Instance stream complete. Submitting data recovery task");
//                    state.getStage(Stage.MISC).submit(new Runnable()
//                    {
//                        @Override
//                        public void run()
//                        {
//                            complete();
//                        }
//                    });
//                    submitted = true;
//                }
//            }
//
//            public void onSuccess(@Nullable StreamState streamState) {}
//
//            public void onFailure(Throwable throwable) {}
//        });
//        streamPlan.execute();
//    }

    /**
     * Start a repair task that repairs the affected range.
     * Replica can now participate in instances, but won't execute the instances
     */
    void recoverData()
    {
        TokenState tokenState = getTokenState();
        Range<Token> range;
        boolean localOnly;
        tokenState.lock.writeLock().lock();
        try
        {
            if (tokenState.getState() != TokenState.State.RECOVERING_INSTANCES)
            {

                // should be set by stream receiver
                logger.info("Aborting instance recovery for {}. Status is {}, expected {}",
                            tokenState, tokenState.getState(), TokenState.State.RECOVERING_INSTANCES);
                return;
            }

            tokenState.setState(TokenState.State.RECOVERING_DATA);
            state.tokenStateManager.save(tokenState);
            range = state.tokenStateManager.rangeFor(tokenState);
            localOnly = tokenState.localOnly();
        }
        finally
        {
            tokenState.lock.writeLock().unlock();
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
        tokenState.lock.writeLock().lock();
        try
        {
            tokenState.setState(TokenState.State.NORMAL);
            state.tokenStateManager.save(tokenState);
        }
        finally
        {
            state.failureRecoveryTaskCompleted(this);
            tokenState.lock.writeLock().unlock();
        }
        logger.info("Epaxos failure recovery task for {} on {} to {} completed", token, cfId, epoch);
        // TODO: execute all committed instances
    }

    @Override
    public void run()
    {
        logger.info("Beginning epaxos failure recovery task for {} on {} to {}", token, cfId, epoch);
        preRecover();
        recoverInstances();
    }
}

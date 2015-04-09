package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Set;
import java.util.UUID;

/**
 * Periodically examines the token states, persisting execution
 * metrics or incrementing epochs when appropriate.
 */
public class TokenStateMaintenanceTask implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(EpaxosState.class);

    private final EpaxosState state;
    private final TokenStateManager tokenStateManager;

    public TokenStateMaintenanceTask(EpaxosState state, TokenStateManager tokenStateManager)
    {
        this.state = state;
        this.tokenStateManager = tokenStateManager;
    }

    protected boolean replicatesTokenForKeyspace(Token token, UUID cfId)
    {
        Pair<String, String> cfName = Schema.instance.getCF(cfId);
        if (cfName == null)
            return false;

        Keyspace keyspace = Keyspace.open(cfName.left);
        AbstractReplicationStrategy replicationStrategy = keyspace.getReplicationStrategy();
        Set<InetAddress> replicas = Sets.newHashSet(replicationStrategy.getNaturalEndpoints(token));
        replicas.addAll(StorageService.instance.getTokenMetadata().pendingEndpointsFor(token, cfName.left));

        return replicas.contains(FBUtilities.getLocalAddress());
    }

    @Override
    public void run()
    {
        logger.debug("TokenStateManager running");
        for (UUID cfId: tokenStateManager.getAllManagedCfIds())
        {
            for (Token token: tokenStateManager.allTokenStatesForCf(cfId))
            {
                if (!replicatesTokenForKeyspace(token, cfId))
                    // TODO: is it safe to remove then?
                    continue;

                TokenState ts = tokenStateManager.getExact(token, cfId);

                ts.lock.readLock().lock();
                long currentEpoch;
                try
                {
                    currentEpoch = ts.getEpoch();
                    // if this token state is expecting recovery to resume, schedule
                    // a recovery and continue
                    if (ts.getState() == TokenState.State.RECOVERY_REQUIRED)
                    {
                        state.startLocalFailureRecovery(ts.getToken(), ts.getCfId(), 0);
                        continue;
                    }
                }
                finally
                {
                    ts.lock.readLock().unlock();
                }

                if (ts.getExecutions() >= state.getEpochIncrementThreshold(cfId))
                {
                    if (ts.getExecutions() < state.getEpochIncrementThreshold(cfId))
                    {
                        continue;
                    }

                    logger.debug("Incrementing epoch for {}", ts);

                    EpochInstance instance = state.createEpochInstance(ts.getToken(), ts.getCfId(), currentEpoch + 1);
                    state.preaccept(instance);
                }
                else if (ts.getNumUnrecordedExecutions() > 0)
                {
                    ts.lock.writeLock().lock();
                    try
                    {
                        logger.debug("Persisting execution data for {}", ts);
                        tokenStateManager.save(ts);
                    }
                    finally
                    {
                        ts.lock.writeLock().unlock();
                    }
                }
                else
                {
                    logger.debug("No activity to update for {}", ts);
                }
            }
        }
    }
}

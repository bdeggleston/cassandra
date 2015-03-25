package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

    protected void updateEpochs()
    {
        for (UUID cfId: tokenStateManager.getAllManagedCfIds())
        {
            for (Token token: tokenStateManager.allTokenStatesForCf(cfId))
            {
                if (!replicatesTokenForKeyspace(token, cfId))
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

    protected Set<Token> getReplicatedTokens(String ksName)
    {
        Set<Token> tokens = Sets.newHashSet();
        for (Map.Entry<Range<Token>, List<InetAddress>> entry: StorageService.instance.getRangeToAddressMap(ksName).entrySet())
        {
            if (entry.getValue().contains(state.getEndpoint()))
            {
                tokens.add(entry.getKey().right);
            }
        }
        return tokens;
    }

    protected Set<Token> getPendingReplicatedTokens(String ksName)
    {
        Set<Token> tokens = Sets.newHashSet();
        Map<Range<Token>, Collection<InetAddress>> rangeMap = StorageService.instance.getTokenMetadata().getPendingRanges(ksName);
        for (Map.Entry<Range<Token>, Collection<InetAddress>> entry: rangeMap.entrySet())
        {
            if (entry.getValue().contains(state.getEndpoint()))
            {
                tokens.add(entry.getKey().right);
            }
        }
        return tokens;
    }

    protected String getKsName(UUID cfId)
    {
        return Schema.instance.getCF(cfId).left;
    }

    /**
     * check that we have token states for all of the tokens we replicate
     */
    protected void checkTokenCoverage()
    {
        for (UUID cfId: tokenStateManager.getAllManagedCfIds())
        {
            String ksName = getKsName(cfId);
            Set<Token> replicatedTokens = getReplicatedTokens(ksName);
            replicatedTokens.addAll(getPendingReplicatedTokens(ksName));

            List<Token> tokens = Lists.newArrayList(replicatedTokens);
            Collections.sort(tokens);

            for (Token token: tokens)
            {
                if (state.tokenStateManager.getExact(token, cfId) == null)
                {
                    logger.info("Running instance for missing token state for token {} on {}", token, cfId);
                    TokenState parentState = state.tokenStateManager.get(token, cfId);
                    TokenInstance instance = state.createTokenInstance(token, cfId);
                    ConsistencyLevel cl = parentState.localOnly() ? ConsistencyLevel.LOCAL_SERIAL : ConsistencyLevel.SERIAL;
                    try
                    {
                        state.process(instance, cl);
                    }
                    catch (WriteTimeoutException e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    protected boolean shouldRun()
    {
        return StorageService.instance.inNormalMode();
    }

    @Override
    public void run()
    {
        logger.debug("TokenStateMaintenanceTask running");
        if (!shouldRun())
        {
            logger.debug("Skipping TokenStateMaintenanceTask, node is not in normal mode");
            return;
        }
        checkTokenCoverage();
        updateEpochs();
    }
}

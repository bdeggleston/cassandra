package org.apache.cassandra.service.epaxos;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    @Override
    public void run()
    {
        logger.debug("TokenStateManager running");
        for (TokenState ts: tokenStateManager.all())
        {
            if (ts.getExecutions() >= state.getEpochIncrementThreshold())
            {
                // TODO: start new epoch increment instance
                // TODO: check that another epoch increment instance hasn't been started yet
                // TODO: check that a previously seen epoch instance isn't local serial from another dc
                ts.rwLock.writeLock().lock();
                try
                {
                    if (ts.getExecutions() < state.getEpochIncrementThreshold())
                    {
                        continue;
                    }

                    // if the high epoch is greater than the current epoch, then an
                    // epoch increment is already in the works
                    if (ts.getHighEpoch() > ts.getEpoch())
                    {
                        continue;
                    }

                    logger.debug("Incrementing epoch for {}", ts);

                    TokenInstance instance = state.createTokenInstance(ts.getToken(), ts.getCfId(), ts.getEpoch() + 1);
                    state.preaccept(instance);
                    ts.recordHighEpoch(instance.getEpoch());
                    tokenStateManager.save(ts);
                }
                finally
                {
                    ts.rwLock.writeLock().unlock();
                }
            }
            else if (ts.getNumUnrecordedExecutions() > 0)
            {
                ts.rwLock.writeLock().lock();
                try
                {
                    logger.debug("Persisting execution data for {}", ts);
                    tokenStateManager.save(ts);
                }
                finally
                {
                    ts.rwLock.writeLock().unlock();
                }
            }
            else
            {
                logger.debug("No activity to update for {}", ts);
            }
        }
    }
}

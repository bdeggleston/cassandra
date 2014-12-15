package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;

import java.nio.ByteBuffer;
import java.util.Collection;

/**
 * TokenStates are always in memory
 *
 * Operations that are only performed during the
 * execution of instances don't have to be synchronized
 * because the epaxos execution algorithm handles that.
 */
public class TokenStateManager
{
    private volatile TokenState __tokenState__ = new TokenState(StorageService.getPartitioner().getMinimumToken(), 0, 0, 0);

    private final String keyspace;
    private final String table;

    public TokenStateManager()
    {
        this(Keyspace.SYSTEM_KS, SystemKeyspace.EPAXOS_TOKEN_STATE);
    }

    public TokenStateManager(String keyspace, String table)
    {
        this.keyspace = keyspace;
        this.table = table;
        // TODO: read token data off disk
        // TODO: maybe 'start' Epaxos?
    }

    // FIXME: only using a single state for now
    public TokenState get(ByteBuffer key)
    {
        return __tokenState__;
    }

    /**
     * Returns
     * @param key
     * @return
     */
    public long getEpoch(ByteBuffer key)
    {
        TokenState ts = get(key);
        ts.rwLock.readLock().lock();
        try
        {
            return ts.getEpoch();
        }
        finally
        {
            ts.rwLock.readLock().unlock();
        }
    }

    public void recordHighEpoch(TokenInstance instance)
    {
        TokenState ts = get(instance.getToken());
        ts.rwLock.writeLock().lock();
        try
        {
            if (ts.recordHighEpoch(instance.getEpoch()))
            {
                save(ts);
            }
        }
        finally
        {
            ts.rwLock.writeLock().unlock();
        }
    }

    public Collection<TokenState> all()
    {
        return Lists.newArrayList(__tokenState__);
    }

    /**
     * Returns the token state that matches the given token
     */
    public TokenState get(Token token)
    {
        // TODO: return the token state that corresponds to the given token
        // TODO: how to deal with unknown tokens?
        return __tokenState__;
    }

    public void save(TokenState state)
    {
        state.onSave();
        // TODO: persist
    }

    // TODO: handle changes to the token ring
    // TODO: manage epochs by cfId?
    // TODO: should there be something like prepare successors to prevent multiple nodes doing redundant increments?

    /**
     * Called when query instances are executed.
     * This method periodically persists it's counts
     * and starts epoch increment tasks when thresholds
     * are reached
     */
    public void reportExecution(ByteBuffer key)
    {
        // TODO: we shouldn't need to synchronize anything here because the execution algorithm does that, right?
        TokenState ts = get(key);
        ts.recordExecution();

    }

    public void reportExecution(Token token)
    {

    }
}

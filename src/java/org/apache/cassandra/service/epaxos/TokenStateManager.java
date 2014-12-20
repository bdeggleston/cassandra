package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;

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

    // TODO: all token operations should quantize tokens to the proper token state, and token states should be inclusive of their own tokens
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
        return getEpoch(StorageService.getPartitioner().getToken(key));
    }

    // ambiguous: is this token for the key, or for the token state
    public long getEpoch(Token token)
    {
        TokenState ts = get(token);
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

    public Set<UUID> getCurrentDependencies(TokenInstance instance)
    {
        TokenState ts = get(instance.getToken());
        ts.rwLock.writeLock().lock();
        try
        {
            Set<UUID> deps = ts.getCurrentTokenInstances();
            ts.recordTokenInstance(instance);
            save(ts);
            return deps;
        }
        finally
        {
            ts.rwLock.writeLock().unlock();
        }
    }

    public Set<UUID> getCurrentTokenDependencies(ByteBuffer key)
    {
        TokenState ts = get(key);
        ts.rwLock.writeLock().lock();
        try
        {
            return ts.getCurrentTokenInstances();
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
    public void reportExecution(Token token)
    {
        // TODO: we shouldn't need to synchronize anything here because the execution algorithm does that, right?
        TokenState ts = get(token);
        ts.recordExecution();
    }
}

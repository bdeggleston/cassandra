package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Maps;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * TokenStates are always in memory
 *
 * Operations that are only performed during the
 * execution of instances don't have to be synchronized
 * because the epaxos execution algorithm handles that.
 */
public class TokenStateManager
{
    private final ConcurrentMap<UUID, TokenState> tokenStates = Maps.newConcurrentMap();

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

    public TokenState get(CfKey cfKey)
    {
        return get(cfKey.key, cfKey.cfId);
    }

    public TokenState get(Instance instance)
    {
        return get(instance.getToken(), instance.getCfId());
    }

    // TODO: all token operations should quantize tokens to the proper token state, and token states should be inclusive of their own tokens
    // FIXME: only using a single state for now
    public TokenState get(ByteBuffer key, UUID cfId)
    {
        return getClosestTokenState(StorageService.getPartitioner().getToken(key), cfId);
    }

    /**
     * Returns the token state that matches the given token
     */
    public TokenState get(Token token, UUID cfId)
    {
        // TODO: return the token state that corresponds to the given token
        // TODO: how to deal with unknown tokens?
        return getClosestTokenState(token, cfId);
    }

    /**
     * Returns
     * @param key
     * @return
     */
    public long getEpoch(ByteBuffer key, UUID cfId)
    {
        return getEpoch(StorageService.getPartitioner().getToken(key), cfId);
    }

    public long getEpoch(Instance instance)
    {
        return getEpoch(instance.getToken(), instance.getCfId());
    }

    // ambiguous: is this token for the key, or for the token state
    public long getEpoch(Token token, UUID cfId)
    {
        TokenState ts = get(token, cfId);
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
        TokenState ts = get(instance.getToken(), instance.getCfId());
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
        TokenState ts = get(instance.getToken(), instance.getCfId());
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

    public Set<UUID> getCurrentTokenDependencies(CfKey cfKey)
    {
        TokenState ts = get(cfKey);
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
        return tokenStates.values();
    }

    private Token getClosestToken(Token token)
    {
        return StorageService.getPartitioner().getMinimumToken();
    }

    private TokenState getClosestTokenState(Token keyToken, UUID cfId)
    {
        Token token = getClosestToken(keyToken);
        TokenState tokenState = tokenStates.get(cfId);
        if (tokenState == null)
        {
            tokenStates.putIfAbsent(cfId, new TokenState(token, cfId, 0, 0, 0));
            tokenState = tokenStates.get(cfId);
            assert tokenState != null;
        }
        return tokenState;
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
    public void reportExecution(Token token, UUID cfId)
    {
        // TODO: we shouldn't need to synchronize anything here because the execution algorithm does that, right?
        TokenState ts = get(token, cfId);
        ts.recordExecution();
    }
}

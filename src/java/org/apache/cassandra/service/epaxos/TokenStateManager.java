package org.apache.cassandra.service.epaxos;

import com.google.common.util.concurrent.Striped;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Token;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;

/**
 * TokenStates are always in memory
 *
 * Operations that are only performed during the
 * execution of instances don't have to be synchronized
 * because the epaxos execution algorithm handles that.
 */
public class TokenStateManager
{
    private volatile TokenState __tokenState__ = new TokenState(0);

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
        // TODO: persist
    }

    // TODO: handle changes to the token ring
    // TODO: maybe manage epochs by cfId?

    /**
     * Called when query instances are executed.
     * This method periodically persists it's counts
     * and starts epoch increment tasks when thresholds
     * are reached
     */
    public void reportExecution(Token token)
    {
        // TODO: should the counts be persisted?
        // TODO: should there be something like prepare successors to prevent multiple nodes doing redundant increments?
        // TODO: we shouldn't need to synchronize anything here because the execution algorithm does that, right?
    }
}

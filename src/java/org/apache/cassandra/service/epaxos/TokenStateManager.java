package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.dht.Token;

import java.nio.ByteBuffer;

public class TokenStateManager
{
    private volatile TokenState __tokenState__ = new TokenState(0);

    // FIXME: only using a single state for now
    public TokenState get(ByteBuffer key)
    {
        // TODO: return the token state that corresponds to the given key
//        String token = "";
//        if (tokenState != null)
//        {
//            return tokenState;
//        }
//
//        String select = String.format("SELECT * FROM %s.%s WHERE token=?", keyspace(), stateTable());
//        UntypedResultSet results = QueryProcessor.executeInternal(select, token);
//        if (results.isEmpty())
//        {
//            tokenState = new TokenState(0);
//            saveTokenState(token, tokenState);
//        }
        return __tokenState__;
    }

    public long getEpoch(ByteBuffer key)
    {
        TokenState ts = get(key);
        ts.rwLock.readLock().lock();
        try
        {
            return ts.epoch;
        }
        finally
        {
            ts.rwLock.readLock().unlock();
        }
    }

    private TokenState getTokenState(Token token)
    {
        // TODO: return the token state that corresponds to the given token
        return __tokenState__;
    }

    public void save(String token, TokenState state)
    {
//        token = "";
//        String insert = String.format("INSERT INTO %s.%s (token, epoch) VALUES (?, ?)", keyspace(), stateTable());
//        QueryProcessor.executeInternal(insert, token, state.epoch);

    }

}

package org.apache.cassandra.service.epaxos;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * TokenStates are always in memory
 *
 * Operations that are only performed during the
 * execution of instances don't have to be synchronized
 * because the epaxos execution algorithm handles that.
 *
 *
 *
 */
public class TokenStateManager
{
    private final ConcurrentMap<UUID, TokenState> tokenStates = Maps.newConcurrentMap();

    private final String keyspace;
    private final String table;
    private volatile boolean started = false;

    private static class ManagedCf
    {
        ReadWriteLock lock = new ReentrantReadWriteLock();
        Map<Token, TokenState> states = new HashMap<>();
        ArrayList<Token> tokens = new ArrayList<>();

        TokenState putIfAbsent(TokenState state)
        {
            lock.writeLock().lock();
            try
            {
                if (states.containsKey(state.getToken()))
                {
                    return states.get(state.getToken());
                }

                tokens = new ArrayList<>(states.keySet());
                Collections.sort(tokens);
            }
            finally
            {
                lock.writeLock().unlock();
            }

            return state;
        }

        Token firstToken(Token searchToken)
        {
            lock.readLock().lock();
            try
            {
                return TokenMetadata.firstToken(tokens, searchToken);
            }
            finally
            {
                lock.readLock().unlock();
            }
        }

        TokenState get(Token token)
        {
            lock.readLock().lock();
            try
            {
                return states.get(token);
            }
            finally
            {
                lock.readLock().unlock();
            }
        }

        List<Token> allTokens()
        {
            lock.readLock().lock();
            try
            {
                return ImmutableList.copyOf(tokens);
            }
            finally
            {
                lock.readLock().unlock();
            }
        }
    }

    private final ConcurrentMap<UUID, ManagedCf> states = Maps.newConcurrentMap();

    public TokenStateManager()
    {
        this(Keyspace.SYSTEM_KS, SystemKeyspace.EPAXOS_TOKEN_STATE);
    }

    public TokenStateManager(String keyspace, String table)
    {
        this.keyspace = keyspace;
        this.table = table;
    }

    private List<TokenState> loadCf(UUID cfId)
    {
        String query = String.format("SELECT * FROM %s.%s WHERE cf_id=?", keyspace, table);
        UntypedResultSet rows = QueryProcessor.executeInternal(query, cfId);
        List<TokenState> states = new ArrayList<>(rows.size());
        for (UntypedResultSet.Row row: rows)
        {
            ByteBuffer data = row.getBlob("data");
            assert data.hasArray();  // FIXME
            DataInput in = ByteStreams.newDataInput(data.array(), data.position());
            try
            {
                states.add(TokenState.serializer.deserialize(in, 0));

            }
            catch (IOException e)
            {
                throw new AssertionError(e);
            }
        }
        return states;
    }

    public synchronized void start()
    {
        assert !started;
        // TODO: load persisted token states
        UntypedResultSet rows = QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s", keyspace, table));
        for (UntypedResultSet.Row row: rows)
        {
            ByteBuffer data = row.getBlob("data");
            assert data.hasArray();  // FIXME
            DataInput in = ByteStreams.newDataInput(data.array(), data.position());
            try
            {
                putState(TokenState.serializer.deserialize(in, 0));
            }
            catch (IOException e)
            {
                throw new AssertionError(e);
            }
        }
        started = true;
    }

    private ManagedCf getManagedCf(UUID cfId)
    {
        ManagedCf cf = states.get(cfId);
        if (cf == null)
        {
            states.putIfAbsent(cfId, new ManagedCf());
            cf = states.get(cfId);
            assert cf != null;
        }
        return cf;
    }

    private synchronized TokenState putState(TokenState state)
    {
        return getManagedCf(state.getCfId()).putIfAbsent(state);
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
        return get(StorageService.getPartitioner().getToken(key), cfId);
    }

    /**
     * Returns the token state that corresponds to the given token/cfId combo
     */
    public TokenState get(Token token, UUID cfId)
    {
        // TODO: return the token state that corresponds to the given token
        // TODO: how to deal with unknown tokens?
        assert started;

        Token ringToken = getClosestToken(token);
        ManagedCf cf = getManagedCf(cfId);
        Token managedToken = cf.firstToken(token);

        // if the ringToken is less than the
        // managedToken, we need to add a new token, since it means a new token has appeared in the ring

        if (managedToken == null)
        {
            TokenState state = new TokenState(ringToken, cfId, 0, 0, 0);
            TokenState prev = cf.putIfAbsent(state);
            if (prev != null)
            {
                return prev;
            }

            save(state);
            return state;
        }

        throw new AssertionError("Not Implemented");
    }

    Token getClosestManagedToken(Token token, UUID cfId)
    {
        return getManagedCf(cfId).firstToken(token);
    }

    protected Token getClosestToken(Token token)
    {
        // TODO: is this right?
        return TokenMetadata.firstToken(StorageService.instance.getTokenMetadata().cachedOnlyTokenMap().sortedTokens(), token);
    }

    private TokenState getClosestTokenState(Token keyToken, UUID cfId)
    {
        Token token = getClosestToken(keyToken);
        TokenState tokenState = tokenStates.get(cfId);

        // TODO: what to do if the token doesn't exist?

        if (tokenState == null)
        {
            List<TokenState> states = loadCf(cfId);
            assert states.size() == 0 || states.size() == 1;
            tokenState = states.size() > 0 ? states.get(0) : new TokenState(token, cfId, 0, 0, 0);

            TokenState previous = tokenStates.putIfAbsent(cfId, tokenState);
            if (previous == null && states.size() == 0)
            {
                save(tokenState);
            }
            else if (previous != null)
            {
                tokenState = previous;
            }
            assert tokenState != null;
        }
        return tokenState;
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

    public void recordHighEpoch(EpochInstance instance)
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

    public Set<UUID> getCurrentDependencies(EpochInstance instance)
    {
        TokenState ts = get(instance.getToken(), instance.getCfId());
        ts.rwLock.writeLock().lock();
        try
        {
            Set<UUID> deps = ts.getCurrentEpochInstances();
            ts.recordEpochInstance(instance);
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
            return ts.getCurrentEpochInstances();
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

    public void save(TokenState state)
    {
        DataOutputBuffer tokenOut = new DataOutputBuffer((int) Token.serializer.serializedSize(state.getToken(), TypeSizes.NATIVE));
        DataOutputBuffer stateOut = new DataOutputBuffer((int) TokenState.serializer.serializedSize(state, 0));
        try
        {
            Token.serializer.serialize(state.getToken(), tokenOut);
            TokenState.serializer.serialize(state, stateOut, 0);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
        String depsReq = "INSERT INTO %s.%s (cf_id, token_bytes, data) VALUES (?, ?, ?)";
        QueryProcessor.executeInternal(String.format(depsReq, keyspace, table),
                                       state.getCfId(),
                                       ByteBuffer.wrap(tokenOut.getData()),
                                       ByteBuffer.wrap(stateOut.getData()));

        state.onSave();
    }

    // TODO: handle changes to the token ring
    // TODO: should there be something like prepare successors to prevent multiple nodes doing redundant increments?

    /**
     * Called when query instances are executed.
     * This method periodically persists it's counts
     * and starts epoch increment tasks when thresholds
     * are reached
     */
    public void reportExecution(Token token, UUID cfId)
    {
        TokenState ts = get(token, cfId);
        ts.rwLock.writeLock().lock();
        try
        {
            ts.recordExecution();
            save(ts);
        }
        finally
        {
            ts.rwLock.writeLock().unlock();
        }
    }

    public List<Token> getManagedTokensForCf(UUID cfId)
    {
        ManagedCf cf = states.get(cfId);
        return cf != null ? cf.allTokens() : ImmutableList.<Token>of();
    }

    public Set<UUID> getAllManagedCfIds()
    {
        return states.keySet();
    }

    public boolean managesCfId(UUID cfId)
    {
        return states.containsKey(cfId);
    }
}

package org.apache.cassandra.service.epaxos;

import com.google.common.collect.*;
import com.google.common.io.ByteStreams;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * TokenStates are always in memory
 *
 */
public class TokenStateManager
{
    private final String keyspace;
    private final String table;
    private volatile boolean started = false;

    class ManagedCf
    {
        final UUID cfid;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        Map<Token, TokenState> states = new HashMap<>();
        ArrayList<Token> tokens = new ArrayList<>();

        private ManagedCf(UUID cfid)
        {
            this.cfid = cfid;
        }

        TokenState putIfAbsent(TokenState state)
        {
            lock.writeLock().lock();
            try
            {
                if (states.containsKey(state.getToken()))
                {
                    return states.get(state.getToken());
                }
                states.put(state.getToken(), state);

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

        /**
         * Adds the given token if a token state does not already exist for it.
         * If it's a new token, it's neighbor is returned. null otherwise
         */
        Token addToken(Token token)
        {
            lock.writeLock().lock();
            try
            {
                if (!states.containsKey(token))
                {
                    TokenState ts;
                    if (states.size() == 0)
                    {
                        ts = new TokenState(token, cfid, 0, 0, 0);
                        putIfAbsent(ts);
                        save(ts);
                        return token;
                    }
                    else
                    {
                        TokenState neighbor = states.get(firstToken(token));
                        ts = new TokenState(token, cfid, neighbor.getEpoch(), neighbor.getEpoch(), 0);

                        putIfAbsent(ts);
                        save(ts);
                        return neighbor.getToken();
                    }
                }
            }
            finally
            {
                lock.writeLock().unlock();
            }
            return null;
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

    public synchronized void start()
    {
        assert !started;
        UntypedResultSet rows = QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s", keyspace, table));
        for (UntypedResultSet.Row row: rows)
        {
            ByteBuffer data = row.getBlob("data");
            assert data.hasArray();  // FIXME
            DataInput in = ByteStreams.newDataInput(data.array(), data.position());
            try
            {
                TokenState ts = TokenState.serializer.deserialize(in, 0);
                // not using getOrInitManagedCf
                ManagedCf cf = states.get(ts.getCfId());
                if (cf == null)
                {
                    cf = new ManagedCf(ts.getCfId());
                    states.put(ts.getCfId(), cf);
                }
                TokenState prev = cf.putIfAbsent(ts);
                assert prev == ts;
            }
            catch (IOException e)
            {
                throw new AssertionError(e);
            }
        }

        // TODO: check that there aren't any token instances in the INITIALIZING phase
        // TODO: check that
        started = true;
    }

    protected Set<Token> getReplicatedTokensForCf(UUID cfId)
    {
        ArrayList<Token> tokens = StorageService.instance.getTokenMetadata().sortedTokens();
        InetAddress localEndpoint = FBUtilities.getLocalAddress();

        Keyspace keyspace = Keyspace.open(Schema.instance.getCF(cfId).left);
        AbstractReplicationStrategy rs = keyspace.getReplicationStrategy();

        Set<Token> replicated = new HashSet<>();
        for (Token token: tokens)
        {
            if (rs.getNaturalEndpoints(token).contains(localEndpoint))
            {
                replicated.add(token);
            }
        }
        return replicated;
    }

    /**
     * Returns the ManagedCf instance for the given cfId, if it exists. If it
     * doesn't exist, it will initialize the ManagedCf with token states at epoch
     * 0 for each token replicated by this node, for that token state.
     */
    ManagedCf getOrInitManagedCf(UUID cfId)
    {
        ManagedCf cf = states.get(cfId);
        if (cf == null)
        {
            synchronized (this)
            {
                cf = states.get(cfId);
                if (cf != null) return cf;

                cf = new ManagedCf(cfId);
                ManagedCf prev = states.putIfAbsent(cfId, cf);
                assert prev == null;

                for (Token token: getReplicatedTokensForCf(cfId))
                {
                    TokenState ts = new TokenState(token, cfId, 0, 0, 0);
                    TokenState prevTs = cf.putIfAbsent(ts);
                    assert prevTs == ts;
                    save(ts);
                }
            }
        }
        return cf;
    }

    public synchronized TokenState putState(TokenState state)
    {
        return getOrInitManagedCf(state.getCfId()).putIfAbsent(state);
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

//        Token ringToken = getClosestToken(token);
        ManagedCf cf = getOrInitManagedCf(cfId);
        Token managedToken = cf.firstToken(token);
        // TODO: shit the bed if the ring token < the managedToken?
        return cf.get(managedToken);
    }

    public TokenState getExact(Token token, UUID cfId)
    {
        ManagedCf managedCf = states.get(cfId);
        if (managedCf != null)
        {
            return managedCf.get(token);
        }
        return null;
    }

    protected Token getClosestToken(Token token)
    {
        // TODO: is this right?
        return TokenMetadata.firstToken(StorageService.instance.getTokenMetadata().cachedOnlyTokenMap().sortedTokens(), token);
    }

    public long getEpoch(ByteBuffer key, UUID cfId)
    {
        return getEpoch(StorageService.getPartitioner().getToken(key), cfId);
    }

    public long getEpoch(Instance instance)
    {
        return getEpoch(instance.getToken(), instance.getCfId());
    }

    /**
     * @param token the key's token (not the managed token)
     * @param cfId
     * @return
     */
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

    public Set<UUID> getCurrentDependencies(AbstractTokenInstance instance)
    {
        TokenState ts = get(instance.getToken(), instance.getCfId());
        ts.rwLock.writeLock().lock();
        try
        {
            Set<UUID> deps = ImmutableSet.copyOf(Iterables.concat(ts.getCurrentEpochInstances(),
                                                                  ts.getCurrentTokenInstances(instance.getToken())));

            if (instance instanceof EpochInstance)
            {
                ts.recordEpochInstance((EpochInstance) instance);
            }
            else if (instance instanceof TokenInstance)
            {
                ts.recordTokenInstance((TokenInstance) instance);
            }
            else
            {
                throw new AssertionError("Unsupported instance type " + instance.getClass().getName());
            }
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

    /**
     * Moves token instance dependency into an epoch dependency for the current epoch.
     * This saves us from having to commit a token instance to an epoch on instantiation, while
     * still ensuring that the correct dependency chain is used for it.
     */
    public void recordExecutedTokenInstance(TokenInstance instance)
    {
        TokenState ts = get(instance);
        ts.rwLock.writeLock().lock();
        try
        {
            ts.recordTokenInstanceExecution(instance);
            save(ts);
        }
        finally
        {
            ts.rwLock.writeLock().unlock();
        }
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

    public List<Token> allTokenStatesForCf(UUID cfId)
    {
        return managesCfId(cfId) ? states.get(cfId).allTokens() : Lists.<Token>newArrayList();
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

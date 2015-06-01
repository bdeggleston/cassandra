package org.apache.cassandra.service.epaxos;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import java.io.DataInput;
import java.io.DataInputStream;
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
    // how many instances should be executed under an epoch before the epoch is incremented
    protected static final int MIN_EPOCH_INCREMENT_THRESHOLD = 5;

    // we don't save the token state every time an instance is executed.
    // This sets the percentage of the increment threshold that can be executed
    // before we must persist the execution count
    protected static final float EXECUTION_PERSISTENCE_PERCENT = Float.parseFloat(System.getProperty("cassandra.epaxos.execution_persistence_percent", "0.1"));

    private final String keyspace;
    private final String table;
    private final Scope scope;
    private volatile boolean started = false;

    class ManagedCf
    {
        final UUID cfid;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        Map<Token, TokenState> states = new HashMap<>();
        ArrayList<Token> tokens = new ArrayList<>();
        volatile int epochThreshold = -1;
        volatile int unsavedExecutionThreshold = -1;

        private ManagedCf(UUID cfid)
        {
            this.cfid = cfid;
        }

        private void updateInternalRing()
        {
            tokens = new ArrayList<>(states.keySet());
            Collections.sort(tokens);

            // recalculate the epoch increment threshold
            epochThreshold = Math.max(DatabaseDescriptor.getEpaxosEpochIncrementThreshold() / tokens.size(),
                                      MIN_EPOCH_INCREMENT_THRESHOLD);
            unsavedExecutionThreshold = (int) (((float) epochThreshold) * EXECUTION_PERSISTENCE_PERCENT);
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
                updateInternalRing();
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

        Range<Token> rangeFor(Token token)
        {
            lock.readLock().lock();
            try
            {
                return states.get(firstToken(token)).getRange();
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

        private void prepareForIncomingStream(Range<Token> range)
        {
            lock.writeLock().lock();
            try
            {
                Set<Token> currentTokens = Sets.newHashSet(states.keySet());
                for (Token token: currentTokens)
                {
                    if (range.contains(token))
                    {
                        TokenState state = states.remove(token);
                        delete(state);
                    }
                }

                putIfAbsent(new TokenState(range, cfid, 0, 0, TokenState.State.PRE_RECOVERY));
                updateInternalRing();
            }
            finally
            {
                lock.writeLock().unlock();
            }
        }
    }

    private final ConcurrentMap<UUID, ManagedCf> states = Maps.newConcurrentMap();

    public TokenStateManager(Scope scope)
    {
        this(Keyspace.SYSTEM_KS, SystemKeyspace.EPAXOS_TOKEN_STATE, scope);
    }

    public TokenStateManager(String keyspace, String table, Scope scope)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.scope = scope;
    }

    public synchronized void start()
    {
        assert !started;
        UntypedResultSet rows = QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s", keyspace, table));
        for (UntypedResultSet.Row row: rows)
        {
            if (row.getInt("scope") != scope.ordinal())
                continue;

            ByteBuffer data = row.getBlob("data");
            DataInput in = new DataInputStream(ByteBufferUtil.inputStream(data));
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

                // we haven't joined the ring yet, so it doesn't make sense to start failure recovery. The token
                // state will start failure recovery the next time it's encountered by a maintenance task, or the
                // verb handler/callbacks
                if (ts.getState() != TokenState.State.NORMAL)
                {
                    ts.setState(TokenState.State.RECOVERY_REQUIRED);
                    save(ts);
                }
            }
            catch (IOException e)
            {
                throw new AssertionError(e);
            }
        }

        setStarted();
    }

    @VisibleForTesting
    void setStarted()
    {
        started = true;
    }

    @VisibleForTesting
    int numManagedTokensFor(UUID cfId)
    {
        ManagedCf cf = states.get(cfId);
        return cf != null ? cf.tokens.size() : 0;
    }

    protected Set<Range<Token>> getReplicatedRangesForCf(UUID cfId)
    {
        TokenMetadata tkm = StorageService.instance.getTokenMetadata();
        ArrayList<Token> tokens = tkm.sortedTokens();
        InetAddress localEndpoint = FBUtilities.getLocalAddress();

        Keyspace keyspace = Keyspace.open(Schema.instance.getCF(cfId).left);
        AbstractReplicationStrategy rs = keyspace.getReplicationStrategy();

        Set<Range<Token>> replicated = new HashSet<>();
        for (Token token: tokens)
        {
            if (rs.getNaturalEndpoints(token).contains(localEndpoint))
            {
                replicated.add(new Range<Token>(tkm.getPredecessor(token), token));
            }
        }
        return replicated;
    }

    ManagedCf getOrInitManagedCf(UUID cfId)
    {
        return getOrInitManagedCf(cfId, TokenState.State.NORMAL);
    }

    /**
     * Returns the ManagedCf instance for the given cfId, if it exists. If it
     * doesn't exist, it will initialize the ManagedCf with token states at epoch
     * 0 for each token replicated by this node, for that token state.
     */
    ManagedCf getOrInitManagedCf(UUID cfId, TokenState.State defaultState)
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

                for (Range<Token> range: getReplicatedRangesForCf(cfId))
                {
                    TokenState ts = new TokenState(range, cfId, 0, 0, defaultState);
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

    public Scope getScope()
    {
        return scope;
    }

    public TokenState getWithDefaultState(ByteBuffer key, UUID cfId, TokenState.State state)
    {
        return getWithDefaultState(DatabaseDescriptor.getPartitioner().getToken(key), cfId, state);
    }

    public TokenState getWithDefaultState(Token token, UUID cfId, TokenState.State state)
    {
        getOrInitManagedCf(cfId, state);
        return get(token, cfId);
    }

    public TokenState get(CfKey cfKey)
    {
        return get(cfKey.key, cfKey.cfId);
    }

    public TokenState get(Instance instance)
    {
        return get(instance.getToken(), instance.getCfId());
    }

    public TokenState get(ByteBuffer key, UUID cfId)
    {
        return get(StorageService.getPartitioner().getToken(key), cfId);
    }

    /**
     * Returns the token state that corresponds to the given token/cfId combo
     */
    public TokenState get(Token token, UUID cfId)
    {
        assert started;

        ManagedCf cf = getOrInitManagedCf(cfId);
        Token managedToken = cf.firstToken(token);
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

    public Range<Token> rangeFor(TokenState tokenState)
    {
        return rangeFor(tokenState.getToken(), tokenState.getCfId());
    }

    public Range<Token> rangeFor(Token token, UUID cfId)
    {
        ManagedCf cf = states.get(cfId);
        return cf != null ? cf.rangeFor(token) : null;
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
        ts.lock.readLock().lock();
        try
        {
            return ts.getEpoch();
        }
        finally
        {
            ts.lock.readLock().unlock();
        }
    }

    public Set<UUID> getCurrentDependencies(AbstractTokenInstance instance)
    {
        TokenState ts = get(instance.getToken(), instance.getCfId());
        ts.lock.writeLock().lock();
        try
        {
            Range<Token> range = new Range<>(rangeFor(ts).left, instance.getToken());
            Set<UUID> deps = ImmutableSet.copyOf(Iterables.concat(ts.getCurrentEpochInstances(),
                                                                  ts.getCurrentTokenInstances(range)));

            switch (instance.getType())
            {
                case EPOCH:
                    ts.recordEpochInstance((EpochInstance) instance);
                    break;
                case TOKEN:
                    ts.recordTokenInstance((TokenInstance) instance);
                    break;
                default:
                    throw new AssertionError("Unsupported instance type " + instance.getClass().getName());
            }
            save(ts);
            return deps;
        }
        finally
        {
            ts.lock.writeLock().unlock();
        }
    }

    public Set<UUID> getCurrentTokenDependencies(CfKey cfKey)
    {
        TokenState ts = get(cfKey);
        ts.lock.writeLock().lock();
        try
        {
            return ts.getCurrentEpochInstances();
        }
        finally
        {
            ts.lock.writeLock().unlock();
        }
    }

    /**
     * Moves token instance dependency into an epoch dependency for the current epoch.
     * This saves us from having to commit a token instance to an epoch on instantiation, while
     * still ensuring that the correct dependency chain is used for it.
     */
    public void bindTokenInstanceToEpoch(TokenInstance instance)
    {
        for (Token token: states.get(instance.getCfId()).allTokens())
        {
            TokenState ts = getExact(token, instance.getCfId());
            ts.lock.writeLock().lock();
            try
            {
                if (ts.bindTokenInstanceToEpoch(instance))
                    save(ts);
            }
            finally
            {
                ts.lock.writeLock().unlock();
            }
        }
    }

    public TokenState recordMissingInstance(AbstractTokenInstance instance)
    {
        TokenState tokenState = get(instance);
        tokenState.lock.writeLock().lock();
        try
        {
            if (instance instanceof EpochInstance)
            {
                tokenState.recordEpochInstance((EpochInstance) instance);
            }
            else if (instance instanceof TokenInstance)
            {
                tokenState.recordTokenInstance((TokenInstance) instance);
            }
            else
            {
                throw new AssertionError("Unsupported instance type " + instance.getClass().getName());
            }
            return tokenState;
        }
        finally
        {
            tokenState.lock.writeLock().unlock();
        }
    }

    public void prepareForIncomingStream(Range<Token> range, UUID cfId)
    {
        getOrInitManagedCf(cfId).prepareForIncomingStream(range);
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
        String depsReq = "INSERT INTO %s.%s (cf_id, token_bytes, scope, data) VALUES (?, ?, ?, ?)";
        QueryProcessor.executeInternal(String.format(depsReq, keyspace, table),
                                       state.getCfId(),
                                       ByteBuffer.wrap(tokenOut.getData()),
                                       scope.ordinal(),
                                       ByteBuffer.wrap(stateOut.getData()));

        state.onSave();
    }

    private void delete(TokenState state)
    {
        DataOutputBuffer tokenOut = new DataOutputBuffer((int) Token.serializer.serializedSize(state.getToken(), TypeSizes.NATIVE));
        try
        {
            Token.serializer.serialize(state.getToken(), tokenOut);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
        String depsReq = "DELETE FROM %s.%s WHERE cf_id=? AND token_bytes=? AND scope=?";
        QueryProcessor.executeInternal(String.format(depsReq, keyspace, table),
                                       state.getCfId(),
                                       ByteBuffer.wrap(tokenOut.getData()),
                                       scope.ordinal());
    }

    /**
     * Called when query instances are executed.
     * This method periodically persists it's counts
     * and starts epoch increment tasks when thresholds
     * are reached
     */
    public void reportExecution(Token token, UUID cfId)
    {
        TokenState ts = get(token, cfId);
        ts.recordExecution();
        int unsavedThreshold = getUnsavedExecutionThreshold(cfId);
        if (ts.getNumUnrecordedExecutions() > unsavedThreshold)
        {
            ts.lock.writeLock().lock();
            try
            {
                if (ts.getNumUnrecordedExecutions() > unsavedThreshold)
                    save(ts);
            }
            finally
            {
                ts.lock.writeLock().unlock();
            }
        }
    }

    public int getEpochIncrementThreshold(UUID cfId)
    {
        return  states.get(cfId).epochThreshold;
    }

    protected int getUnsavedExecutionThreshold(UUID cfId)
    {
        return  states.get(cfId).unsavedExecutionThreshold;
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

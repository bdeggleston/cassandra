package org.apache.cassandra.service.epaxos;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.Striped;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.Pair;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public class KeyStateManager
{
    // prevents multiple threads modifying the dependency manager for a given key. Aquire this after locking an instance
    private final Striped<Lock> locks = Striped.lock(DatabaseDescriptor.getConcurrentWriters() * 1024);
    private final Cache<CfKey, KeyState> cache;
    private final String keyspace;
    private final String table;
    protected final TokenStateManager tokenStateManager;

    public KeyStateManager(TokenStateManager tokenStateManager)
    {
        this(Keyspace.SYSTEM_KS, SystemKeyspace.EPAXOS_KEY_STATE, tokenStateManager);
    }
    public KeyStateManager(String keyspace, String table, TokenStateManager tokenStateManager)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.tokenStateManager = tokenStateManager;
        cache = CacheBuilder.newBuilder().expireAfterAccess(5, TimeUnit.MINUTES).maximumSize(1000).build();
        // TODO: on startup, check that all key states have the same epoch as their token state... or at least during read.
    }

    public Lock getCfKeyLock(CfKey cfKey)
    {
        return locks.get(cfKey);
    }

    protected Iterator<CfKey> getCfKeyIterator(TokenState tokenState)
    {
        return getCfKeyIterator(tokenState, 10000);
    }

    /**
     * Returns an iterator of CfKeys of all keys owned by the given tokenState
     */
    public Iterator<CfKey> getCfKeyIterator(final TokenState tokenState, final int limit)
    {
        return getCfKeyIterator(tokenStateManager.rangeFor(tokenState), tokenState.getCfId(), limit);
    }

    /**
     * Returns an iterator of CfKeys of all keys owned by the given token range
     */
    public Iterator<CfKey> getCfKeyIterator(Range<Token> range, UUID cfId, int limit)
    {
        Function<UntypedResultSet.Row, CfKey> f = new Function<UntypedResultSet.Row, CfKey>()
        {
            public CfKey apply(UntypedResultSet.Row row)
            {
                return new CfKey(row.getBlob("row_key"), row.getUUID("cf_id"));
            }
        };
        TableIterable i = new TableIterable(cfId, range, limit, false);
        return Iterables.transform(i, f).iterator();
    }

    public Set<UUID> getCurrentDependencies(Instance instance)
    {
        if (instance.getType() == Instance.Type.QUERY)
        {
            SerializedRequest request = ((QueryInstance) instance).getQuery();
            CfKey cfKey = request.getCfKey();
            Set<UUID> deps = getCurrentDependencies(instance, cfKey);
            deps.remove(instance.getId());
            return deps;
        }
        else if (instance.getType() == Instance.Type.EPOCH || instance.getType() == Instance.Type.TOKEN)
        {
            TokenState tokenState = tokenStateManager.get(instance); // FIXME: get from instance
            Set<UUID> deps = new HashSet<>(tokenStateManager.getCurrentDependencies((AbstractTokenInstance) instance));

            // create a range using the left token as dictated by the current managed token ring for this cf, and the
            // instance token as the right token. This prevents token instances from having dependencies on instances
            // that some of it's  replicas don't replicate. If a node misses this split, it will find out about it the
            // next time it touches one of the keys owned by the new token
            Range<Token> tsRange = tokenStateManager.rangeFor(tokenState);
            Iterator<CfKey> cfKeyIterator = getCfKeyIterator(tsRange, tokenState.getCfId(), 10000);
            while (cfKeyIterator.hasNext())
            {
                CfKey cfKey = cfKeyIterator.next();
                deps.addAll(getCurrentDependencies(instance, cfKey));
            }

            deps.remove(instance.getId());

            return deps;
        }
        else
        {
            throw new IllegalArgumentException("Unsupported instance type: " + instance.getClass().getName());
        }
    }

    private Set<UUID> getCurrentDependencies(Instance instance, CfKey cfKey)
    {
        Lock lock = getCfKeyLock(cfKey);
        lock.lock();
        try
        {
            KeyState dm = loadKeyState(cfKey.key, cfKey.cfId);
            Set<UUID> deps = dm.getDepsAndAdd(instance.getId());
            saveKeyState(cfKey, dm);

            return deps;
        }
        finally
        {
            lock.unlock();
        }
    }

    public void recordMissingInstance(Instance instance)
    {
        CfKey cfKey;
        switch (instance.getType())
        {
            case QUERY:
                SerializedRequest request = ((QueryInstance) instance).getQuery();
                cfKey = request.getCfKey();
                recordMissingInstance(instance, cfKey);
                break;
            case TOKEN:
            case EPOCH:
                TokenState tokenState = tokenStateManager.get(instance);
                Iterator<CfKey> cfKeyIterator = getCfKeyIterator(tokenState, 10000);
                while (cfKeyIterator.hasNext())
                {
                    cfKey = cfKeyIterator.next();
                    recordMissingInstance(instance, cfKey);
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported instance type: " + instance.getClass().getName());
        }
    }

    private void recordMissingInstance(Instance instance, CfKey cfKey)
    {
        Lock lock = getCfKeyLock(cfKey);
        lock.lock();
        try
        {
            // FIXME: token state requests
            KeyState dm = loadKeyState(cfKey.key, cfKey.cfId);
            dm.recordInstance(instance.getId());

            if (instance.getState().atLeast(Instance.State.ACCEPTED))
            {
                dm.markAcknowledged(instance.getDependencies(), instance.getId());
            }

            saveKeyState(cfKey, dm);
        }
        finally
        {
            lock.unlock();
        }
    }

    public void recordAcknowledgedDeps(Instance instance)
    {
        CfKey cfKey;
        TokenState tokenState = tokenStateManager.get(instance);
        Range<Token> tokenRange = tokenStateManager.rangeFor(tokenState);
        switch (instance.getType())
        {
            case QUERY:
                SerializedRequest request = ((QueryInstance) instance).getQuery();
                cfKey = request.getCfKey();
                recordAcknowledgedDeps(instance, cfKey);
                break;
            case TOKEN:
                // TODO: test
                tokenStateManager.recordExecutedTokenInstance((TokenInstance) instance);
                if (tokenState.getCreatorToken() != null)
                {
                    tokenRange = new Range<>(tokenRange.left, tokenState.getCreatorToken());
                }
            case EPOCH:
                Iterator<CfKey> cfKeyIterator = getCfKeyIterator(tokenRange, instance.getCfId(), 10000);
                while (cfKeyIterator.hasNext())
                {
                    cfKey = cfKeyIterator.next();
                    recordAcknowledgedDeps(instance, cfKey);
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported instance type: " + instance.getClass().getName());
        }
    }

    private void recordAcknowledgedDeps(Instance instance, CfKey cfKey)
    {
        Lock lock = getCfKeyLock(cfKey);
        lock.lock();
        try
        {
            KeyState dm = loadKeyState(cfKey.key, cfKey.cfId);
            dm.markAcknowledged(instance.getDependencies(), instance.getId());
            saveKeyState(cfKey, dm);
        }
        finally
        {
            lock.unlock();
        }
    }

    public void recordExecuted(Instance instance, ReplayPosition position)
    {
        CfKey cfKey;
        TokenState tokenState = tokenStateManager.get(instance);
        Range<Token> tokenRange = tokenStateManager.rangeFor(tokenState);
        switch (instance.getType())
        {
            case QUERY:
                SerializedRequest request = ((QueryInstance) instance).getQuery();
                cfKey = request.getCfKey();
                recordExecuted(instance, cfKey, position);
                break;
            case TOKEN:
                // TODO: test
                tokenStateManager.recordExecutedTokenInstance((TokenInstance) instance);
                if (tokenState.getCreatorToken() != null)
                {
                    tokenRange = new Range<>(tokenRange.left, tokenState.getCreatorToken());
                }
            case EPOCH:
                Iterator<CfKey> cfKeyIterator = getCfKeyIterator(tokenRange, instance.getCfId(), 10000);
                while (cfKeyIterator.hasNext())
                {
                    cfKey = cfKeyIterator.next();
                    recordExecuted(instance, cfKey, position);
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported instance type: " + instance.getClass().getName());
        }
    }

    private void recordExecuted(Instance instance, CfKey cfKey, ReplayPosition position)
    {

        Lock lock = getCfKeyLock(cfKey);
        lock.lock();
        try
        {
            KeyState dm = loadKeyState(cfKey.key, cfKey.cfId);
            dm.markExecuted(instance.getId(), instance.getStronglyConnected(), position);
            saveKeyState(cfKey, dm);
        }
        finally
        {
            lock.unlock();
        }
    }

    private void addTokenDeps(CfKey cfKey, KeyState keyState)
    {
        for (UUID id: tokenStateManager.getCurrentTokenDependencies(cfKey))
        {
            keyState.recordInstance(id);
        }
    }

    private KeyState deserialize(ByteBuffer data)
    {
        assert data.hasArray();  // FIXME
        DataInput in = ByteStreams.newDataInput(data.array(), data.position());
        try
        {
            return KeyState.serializer.deserialize(in, 0);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    public KeyState loadKeyState(CfKey cfKey)
    {
        return loadKeyState(cfKey.key, cfKey.cfId);
    }

    public KeyState loadKeyStateIfExists(CfKey cfKey)
    {
        KeyState dm = cache.getIfPresent(cfKey);
        if (dm != null)
            return dm;

        String query = "SELECT * FROM %s.%s WHERE row_key=? AND cf_id=?";
        UntypedResultSet results = QueryProcessor.executeInternal(String.format(query, keyspace, table), cfKey.key, cfKey.cfId);

        if (results.isEmpty())
        {
            return null;
        }

        UntypedResultSet.Row row = results.one();

        ByteBuffer data = row.getBlob("data");
        dm = deserialize(data);
        cache.put(cfKey, dm);
        return dm;
    }

    public boolean exists(CfKey cfKey)
    {
        String query = "SELECT * FROM %s.%s WHERE row_key=? AND cf_id=?";
        return cache.getIfPresent(cfKey) != null || !QueryProcessor.executeInternal(String.format(query, keyspace, table), cfKey.key, cfKey.cfId).isEmpty();
    }

    /**
     * loads a dependency manager. Must be called within a lock held for this key & cfid pair
     */
    @VisibleForTesting
    public KeyState loadKeyState(ByteBuffer key, UUID cfId)
    {
        CfKey cfKey = new CfKey(key, cfId);
        KeyState dm = cache.getIfPresent(cfKey);
        if (dm != null)
            return dm;

        String query = "SELECT * FROM %s.%s WHERE row_key=? AND cf_id=?";
        UntypedResultSet results = QueryProcessor.executeInternal(String.format(query, keyspace, table), key, cfId);

        if (results.isEmpty())
        {
            dm = new KeyState(tokenStateManager.getEpoch(key, cfId));

            // add the current epoch dependencies if this is a new key
            addTokenDeps(cfKey, dm);
            saveKeyState(cfKey, dm);

            cache.put(cfKey, dm);
            return dm;
        }

        UntypedResultSet.Row row = results.one();

        ByteBuffer data = row.getBlob("data");
        dm = deserialize(data);
        cache.put(cfKey, dm);
        // TODO: check keyState epoch against expected tokenState epoch
        return dm;
    }

    @VisibleForTesting
    boolean managesKey(ByteBuffer key, UUID cfId)
    {
        if (cache.getIfPresent(new CfKey(key, cfId)) != null)
        {
            return true;
        }
        else
        {
            String query = "SELECT * FROM %s.%s WHERE row_key=? AND cf_id=?";
            UntypedResultSet results = QueryProcessor.executeInternal(String.format(query, keyspace, table), key, cfId);
            return !results.isEmpty();
        }
    }

    void saveKeyState(CfKey cfKey, KeyState dm)
    {
        saveKeyState(cfKey.key, cfKey.cfId, dm, true);
    }

    void saveKeyState(ByteBuffer key, UUID cfId, KeyState dm)
    {
        saveKeyState(key, cfId, dm, true);
    }

    /**
     * persists a dependency manager. Must be called within a lock held for this key & cfid pair
     */
    void saveKeyState(ByteBuffer key, UUID cfId, KeyState dm, boolean cache)
    {

        DataOutputBuffer out = new DataOutputBuffer((int) KeyState.serializer.serializedSize(dm, 0));
        try
        {
            KeyState.serializer.serialize(dm, out, 0);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);  // TODO: propagate original exception?
        }
        String depsReq = "INSERT INTO %s.%s (row_key, cf_id, data) VALUES (?, ?, ?)";
        QueryProcessor.executeInternal(String.format(depsReq, keyspace, table),
                                       key,
                                       cfId,
                                       ByteBuffer.wrap(out.getData()));

        if (cache)
        {
            this.cache.put(new CfKey(key, cfId), dm);
        }
    }

    void deleteKeyState(CfKey cfKey)
    {
        cache.invalidate(cfKey);
        String deleteReq = "DELETE FROM %s.%s WHERE row_key=? AND cf_id=?";
        QueryProcessor.executeInternal(String.format(deleteReq, keyspace, table), cfKey.key, cfKey.cfId);
    }

    /**
     * Checks that none of the key managers owned by the given token state
     * have any active instances in any epochs but the current one
     */
    public boolean canIncrementToEpoch(TokenState tokenState, long targetEpoch)
    {
        // TODO: pass in token range
        Iterator<CfKey> cfKeyIterator = getCfKeyIterator(tokenState, 10000);
        while (cfKeyIterator.hasNext())
        {
            CfKey cfKey = cfKeyIterator.next();
            KeyState keyState = loadKeyState(cfKey.key, cfKey.cfId);
            if (!keyState.canIncrementToEpoch(targetEpoch))
            {
                return false;
            }
        }
        return true;
    }

    public void updateEpoch(TokenState tokenState)
    {
        Iterator<CfKey> cfKeyIterator = getCfKeyIterator(tokenState, 10000);
        while (cfKeyIterator.hasNext())
        {
            CfKey cfKey = cfKeyIterator.next();
            updateEpoch(cfKey, tokenState.getEpoch());
        }
    }

    private void updateEpoch(CfKey cfKey, long epoch)
    {
        Lock lock = getCfKeyLock(cfKey);
        lock.lock();
        try
        {
            KeyState dm = loadKeyState(cfKey.key, cfKey.cfId);
            dm.setEpoch(epoch);
            saveKeyState(cfKey, dm);
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Returns true if we haven't been streamed any mutations
     * that are ahead of the current execution position
     */
    public boolean canExecute(CfKey cfKey)
    {
        Lock lock = getCfKeyLock(cfKey);
        lock.lock();
        try
        {
            KeyState ks = loadKeyState(cfKey.key, cfKey.cfId);
            return ks.canExecute();
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Reports that a row was streamed to us that contains a mutation
     * from an instance that hasn't been executed locally yet.
     */
    public boolean reportFutureRepair(CfKey cfKey, ExecutionInfo info)
    {
        Lock lock = getCfKeyLock(cfKey);
        lock.lock();
        try
        {
            KeyState ks = loadKeyState(cfKey.key, cfKey.cfId);
            return ks.setFutureExecution(info);
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Returns execution info for the given token range and replay position. If the replay position is too far in the
     * past to have data, the execution position directly before the first known mutation is returned. This is ok since
     * it will be in a current epoch which we know about or are in the process of recovering.
     *
     * This reads the current key state data off of disk, bypassing the cache and locks. So the likelihood of getting
     * stale data is high. This exists to generate metadata for streams.
     */
    public Iterator<Pair<ByteBuffer, ExecutionInfo>> getRangeExecutionInfo(UUID cfId, Range<Token> range, final ReplayPosition replayPosition)
    {
        Function<UntypedResultSet.Row, Pair<ByteBuffer, ExecutionInfo>> f;
        f = new Function<UntypedResultSet.Row, Pair<ByteBuffer, ExecutionInfo>>()
        {
            public Pair<ByteBuffer, ExecutionInfo> apply(UntypedResultSet.Row row)
            {
                ByteBuffer key = row.getBlob("row_key");
                KeyState keyState = deserialize(row.getBlob("data"));
                return Pair.create(key, keyState.getExecutionInfoAtPosition(replayPosition));
            }
        };
        TableIterable i = new TableIterable(cfId, range, 10000, true);
        return Iterables.transform(i, f).iterator();
    }

    private class TableIterable implements Iterable<UntypedResultSet.Row>
    {
        final UUID cfId;
        final Range<Token> range;
        final int limit;
        final boolean inclusive;

        private TableIterable(UUID cfId, Range<Token> range, int limit, boolean inclusive)
        {
            this.cfId = cfId;
            this.range = range;
            this.limit = limit;
            this.inclusive = inclusive;
        }

        @Override
        public Iterator<UntypedResultSet.Row> iterator()
        {
            return new TableIterator(cfId, range, limit, inclusive);
        }
    }

    private class TableIterator implements Iterator<UntypedResultSet.Row>
    {
        final UUID cfId;
        final Iterator<Range<Token>> rangeIterator;
        final int limit;
        final boolean inclusive;
        final ColumnFamilyStore cfs;

        volatile UntypedResultSet.Row next;
        volatile boolean endReached = false;
        volatile Iterator<UntypedResultSet.Row> rowIterator = null;
        volatile Range<Token> currentRange;
        volatile ByteBuffer lastKey = null;

        private TableIterator(UUID cfId, Range<Token> range, int limit, boolean inclusive)
        {
            this.cfId = cfId;
            rangeIterator = range.unwrap().iterator();
            currentRange = rangeIterator.next();
            this.limit = limit;
            this.inclusive = inclusive;
            cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        }


        Iterator<UntypedResultSet.Row> getRowIterator(Token left, Token right)
        {
            return getRowIterator(left, right, false);
        }

        Iterator<UntypedResultSet.Row> getRowIterator(Token left, Token right, boolean inclusive)
        {
            Token leftToken = left;
            while (true)
            {
                AbstractBounds<RowPosition> bounds;
                if (inclusive)
                {
                    bounds = new Bounds<RowPosition>(leftToken.minKeyBound(), right.maxKeyBound());
                }
                else
                {
                    bounds = new Range<RowPosition>(leftToken.maxKeyBound(), right.maxKeyBound());
                }
                List<Row> partitions = cfs.getRangeSlice(bounds,
                                                         null,
                                                         new IdentityQueryFilter(),
                                                         limit,
                                                         System.currentTimeMillis());

                endReached = partitions.size() < limit;

                String query = String.format("SELECT * FROM %s.%s", keyspace, table);

                UntypedResultSet rows = QueryProcessor.resultify(query, partitions);


                // in case we get a bunch of tombstones
                if (rows.size() == 0 && !endReached)
                {
                    leftToken = DatabaseDescriptor.getPartitioner().getToken(partitions.get(partitions.size() - 1).key.getKey());
                    continue;
                }

                return Iterables.filter(rows, new Predicate<UntypedResultSet.Row>()
                {
                    @Override
                    public boolean apply(UntypedResultSet.Row row)
                    {
                        return row.getUUID("cf_id").equals(cfId);
                    }
                }).iterator();
            }
        }

        void maybeSetNext()
        {
            if (next == null)
            {
                if (rowIterator == null)
                {
                    rowIterator = getRowIterator(currentRange.left, currentRange.right, inclusive);
                    if (!rowIterator.hasNext())
                    {
                        endReached = true;
                    }
                    maybeSetNext();
                }
                else if (rowIterator.hasNext())
                {
                    next = rowIterator.next();
                    lastKey = next.getBlob("row_key");
                }
                else if (!rowIterator.hasNext() && !endReached)
                {
                    Token lastToken = DatabaseDescriptor.getPartitioner().getToken(lastKey);
                    if (lastToken.equals(currentRange.right))
                    {
                        endReached = true;
                    }
                    else
                    {
                        rowIterator = getRowIterator(lastToken, currentRange.right);
                    }
                    maybeSetNext();
                }
                else if (!rowIterator.hasNext() && rangeIterator.hasNext())
                {
                    currentRange = rangeIterator.next();
                    rowIterator = null;
                    lastKey = null;
                    maybeSetNext();
                }
            }
        }

        @Override
        public boolean hasNext()
        {
            maybeSetNext();
            maybeSetNext();
            return next != null;
        }

        @Override
        public UntypedResultSet.Row next()
        {
            maybeSetNext();
            maybeSetNext();
            UntypedResultSet.Row row = next;
            next = null;
            return row;
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }
}

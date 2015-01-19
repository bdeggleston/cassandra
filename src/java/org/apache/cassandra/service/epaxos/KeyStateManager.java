package org.apache.cassandra.service.epaxos;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.Striped;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
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
    private final TokenStateManager tokenStateManager;

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
    // TODO: use token ranges instead of token state
    public Iterator<CfKey> getCfKeyIterator(final TokenState tokenState, final int limit)
    {
        // TODO: bound token range
        // TODO: handle num rows > limit
        // TODO: get the tail of wide rows
        String insert = String.format("SELECT row_key, cf_id FROM %s.%s LIMIT ?", keyspace, table);
        UntypedResultSet result = QueryProcessor.executeInternal(insert, limit);
        final Iterator<UntypedResultSet.Row> rowIterator = result.iterator();
        return new Iterator<CfKey>()
        {
            private CfKey next = null;

            private CfKey fromRow(UntypedResultSet.Row row)
            {
                return new CfKey(row.getBlob("row_key"), row.getUUID("cf_id"));
            }

            private void maybeSetNext()
            {
                if (next != null) return;
                while (rowIterator.hasNext())
                {
                    CfKey n = fromRow(rowIterator.next());
                    if (n.cfId.equals(tokenState.getCfId()))
                    {
                        next = n;
                        return;
                    }
                }
            }

            @Override
            public boolean hasNext()
            {
                maybeSetNext();
                return next != null;
            }

            @Override
            public CfKey next()
            {
                maybeSetNext();
                if (next == null)
                {
                    throw new NoSuchElementException();
                }
                else
                {
                    CfKey n = next;
                    next = null;
                    return n;
                }
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public Set<UUID> getCurrentDependencies(Instance instance)
    {
        if (instance instanceof QueryInstance)
        {
            SerializedRequest request = ((QueryInstance) instance).getQuery();
            CfKey cfKey = request.getCfKey();
            Set<UUID> deps = getCurrentDependencies(instance, cfKey);
            deps.remove(instance.getId());
            return deps;
        }
        else if (instance instanceof TokenInstance)
        {
            TokenState tokenState = tokenStateManager.get(instance); // FIXME: get from instance
            Set<UUID> deps = new HashSet<>(tokenStateManager.getCurrentDependencies((TokenInstance) instance));
            Iterator<CfKey> cfKeyIterator = getCfKeyIterator(tokenState, 10000);
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
        if (instance instanceof QueryInstance)
        {
            SerializedRequest request = ((QueryInstance) instance).getQuery();
            CfKey cfKey = request.getCfKey();
            recordMissingInstance(instance, cfKey);
        }
        else if (instance instanceof TokenInstance)
        {
            TokenState tokenState = tokenStateManager.get(instance); // FIXME: get from instance
            Iterator<CfKey> cfKeyIterator = getCfKeyIterator(tokenState, 10000);
            while (cfKeyIterator.hasNext())
            {
                CfKey cfKey = cfKeyIterator.next();
                recordMissingInstance(instance, cfKey);
            }
        }
        else
        {
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
        if (instance instanceof QueryInstance)
        {
            SerializedRequest request = ((QueryInstance) instance).getQuery();
            CfKey cfKey = request.getCfKey();
            recordAcknowledgedDeps(instance, cfKey);
        }
        else if (instance instanceof TokenInstance)
        {
            TokenState tokenState = tokenStateManager.get(instance); // FIXME: get from instance
            Iterator<CfKey> cfKeyIterator = getCfKeyIterator(tokenState, 10000);
            while (cfKeyIterator.hasNext())
            {
                CfKey cfKey = cfKeyIterator.next();
                recordAcknowledgedDeps(instance, cfKey);
            }
        }
        else
        {
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
        if (instance instanceof QueryInstance)
        {
            SerializedRequest request = ((QueryInstance) instance).getQuery();
            CfKey cfKey = request.getCfKey();
            recordExecuted(instance, cfKey, position);
        }
        else if (instance instanceof TokenInstance)
        {
            TokenState tokenState = tokenStateManager.get(instance); // FIXME: get from instance
            Iterator<CfKey> cfKeyIterator = getCfKeyIterator(tokenState, 10000);
            while (cfKeyIterator.hasNext())
            {
                CfKey cfKey = cfKeyIterator.next();
                recordExecuted(instance, cfKey, position);
            }
        }
        else
        {
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
        return dm;
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
     * Returns execution info for the given token range at the given replay position. If the given replay position
     * is too far in the past to have data, the execution position directly before the first known mutation is returned.
     * This is ok since it will be in a current epoch which we know about or are in the process of recovering.
     */
    public Iterator<Pair<ByteBuffer, ExecutionInfo>> getRangeExecutionInfo(UUID cfId, Range<Token> range, final ReplayPosition replayPosition)
    {
        // TODO: refactor
        final Iterator<Pair<ByteBuffer, KeyState>> iterator = new KeyStateIterator(cfId, range);
        return new Iterator<Pair<ByteBuffer, ExecutionInfo>>()
        {
            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public Pair<ByteBuffer, ExecutionInfo> next()
            {
                Pair<ByteBuffer, KeyState> next = iterator.next();
                return Pair.create(next.left, next.right.getExecutionInfoAtPosition(replayPosition));
            }

            @Override
            public void remove()
            {

            }
        };
    }

    /**
     * This reads the current key state data off of disk, bypassing the cache and locks. So
     * the likelihood of getting stale data is high. This exists to generate metadata for streams.
     */
    private class KeyStateIterator implements Iterator<Pair<ByteBuffer, KeyState>>
    {
        private static final int LIMIT = 10000;

        final UUID cfId;
        Range<Token> range;
        Iterator<UntypedResultSet.Row> iter = null;
        Pair<ByteBuffer, KeyState> next = null;
        boolean endReached = false;

        private KeyStateIterator(UUID cfId, Range<Token> range)
        {
            this.cfId = cfId;
            this.range = range;
        }

        private void fillIter()
        {
            ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);

            AbstractBounds<RowPosition> bounds = new Bounds<RowPosition>(range.left.minKeyBound(), range.right.maxKeyBound());
            IDiskAtomFilter atomFilter = new IdentityQueryFilter();
            List<Row> partitions = cfs.getRangeSlice(bounds, null, atomFilter, LIMIT);
            String query = String.format("SELECT * FROM %s.%s", keyspace, table);
            // TODO: handle more than initial limit
            UntypedResultSet results = QueryProcessor.resultify(query, partitions);
            endReached = results.size() < LIMIT;
            iter = results.iterator();
        }

        private void maybeSetNext()
        {
            if (next != null)
            {
                return;
            }

            if (iter == null || (!iter.hasNext() && !endReached))
            {
                fillIter();
            }

            while (iter.hasNext())
            {
                UntypedResultSet.Row nextRow = iter.next();
                if (!nextRow.getUUID("cf_id").equals(cfId))
                {
                    continue;
                }
                ByteBuffer key = nextRow.getBlob("row_key");
                KeyState keyState = deserialize(nextRow.getBlob("data"));
                next = Pair.create(key, keyState);
                return;
            }
        }

        @Override
        public boolean hasNext()
        {
            maybeSetNext();
            return next != null;
        }

        @Override
        public Pair<ByteBuffer, KeyState> next()
        {
            try
            {
                return next;
            }
            finally
            {
                next = null;
            }
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }
}

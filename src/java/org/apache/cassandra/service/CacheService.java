/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.service;

import java.io.DataInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.*;
import org.apache.cassandra.cache.AutoSavingCache.CacheSerializer;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class CacheService implements CacheServiceMBean
{
    private static final Logger logger = LoggerFactory.getLogger(CacheService.class);

    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=Caches";

    public static enum CacheType
    {
        KEY_CACHE("KeyCache"),
        ROW_CACHE("RowCache"),
        COUNTER_CACHE("CounterCache");

        private final String name;

        private CacheType(String typeName)
        {
            name = typeName;
        }

        public String toString()
        {
            return name;
        }
    }

    public final static CacheService instance = new CacheService(
            DatabaseDescriptor.instance, StageManager.instance, Schema.instance
    );

    public final AutoSavingCache<KeyCacheKey, RowIndexEntry> keyCache;
    public final AutoSavingCache<RowCacheKey, IRowCacheEntry> rowCache;
    public final AutoSavingCache<CounterCacheKey, ClockAndCount> counterCache;

    private final DatabaseDescriptor databaseDescriptor;
    private final StageManager stageManager;
    private final Schema schema;

    public CacheService(DatabaseDescriptor databaseDescriptor, StageManager stageManager, Schema schema)
    {
        assert databaseDescriptor != null;
        assert stageManager != null;
        assert schema != null;

        this.databaseDescriptor = databaseDescriptor;
        this.stageManager = stageManager;
        this.schema = schema;

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        try
        {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        keyCache = initKeyCache();
        rowCache = initRowCache();
        counterCache = initCounterCache();
    }

    /**
     * @return auto saving cache object
     */
    private AutoSavingCache<KeyCacheKey, RowIndexEntry> initKeyCache()
    {
        logger.info("Initializing key cache with capacity of {} MBs.", databaseDescriptor.getKeyCacheSizeInMB());

        long keyCacheInMemoryCapacity = databaseDescriptor.getKeyCacheSizeInMB() * 1024 * 1024;

        // as values are constant size we can use singleton weigher
        // where 48 = 40 bytes (average size of the key) + 8 bytes (size of value)
        ICache<KeyCacheKey, RowIndexEntry> kc;
        kc = ConcurrentLinkedHashCache.create(keyCacheInMemoryCapacity);
        AutoSavingCache<KeyCacheKey, RowIndexEntry> keyCache = new AutoSavingCache<>(kc, CacheType.KEY_CACHE, new KeyCacheSerializer());

        int keyCacheKeysToSave = databaseDescriptor.getKeyCacheKeysToSave();

        keyCache.scheduleSaving(databaseDescriptor.getKeyCacheSavePeriod(), keyCacheKeysToSave);

        return keyCache;
    }

    /**
     * @return initialized row cache
     */
    private AutoSavingCache<RowCacheKey, IRowCacheEntry> initRowCache()
    {
        logger.info("Initializing row cache with capacity of {} MBs", databaseDescriptor.getRowCacheSizeInMB());

        long rowCacheInMemoryCapacity = databaseDescriptor.getRowCacheSizeInMB() * 1024 * 1024;

        // cache object
        ICache<RowCacheKey, IRowCacheEntry> rc = new SerializingCacheProvider().create(rowCacheInMemoryCapacity);
        AutoSavingCache<RowCacheKey, IRowCacheEntry> rowCache = new AutoSavingCache<>(rc, CacheType.ROW_CACHE, new RowCacheSerializer());

        int rowCacheKeysToSave = databaseDescriptor.getRowCacheKeysToSave();

        rowCache.scheduleSaving(databaseDescriptor.getRowCacheSavePeriod(), rowCacheKeysToSave);

        return rowCache;
    }

    private AutoSavingCache<CounterCacheKey, ClockAndCount> initCounterCache()
    {
        logger.info("Initializing counter cache with capacity of {} MBs", databaseDescriptor.getCounterCacheSizeInMB());

        long capacity = databaseDescriptor.getCounterCacheSizeInMB() * 1024 * 1024;

        AutoSavingCache<CounterCacheKey, ClockAndCount> cache =
            new AutoSavingCache<>(ConcurrentLinkedHashCache.<CounterCacheKey, ClockAndCount>create(capacity),
                                  CacheType.COUNTER_CACHE,
                                  new CounterCacheSerializer());

        int keysToSave = databaseDescriptor.getCounterCacheKeysToSave();

        logger.info("Scheduling counter cache save to every {} seconds (going to save {} keys).",
                    databaseDescriptor.getCounterCacheSavePeriod(),
                    keysToSave == Integer.MAX_VALUE ? "all" : keysToSave);

        cache.scheduleSaving(databaseDescriptor.getCounterCacheSavePeriod(), keysToSave);

        return cache;
    }

    public long getKeyCacheHits()
    {
        return keyCache.getMetrics().hits.count();
    }

    public long getRowCacheHits()
    {
        return rowCache.getMetrics().hits.count();
    }

    public long getKeyCacheRequests()
    {
        return keyCache.getMetrics().requests.count();
    }

    public long getRowCacheRequests()
    {
        return rowCache.getMetrics().requests.count();
    }

    public double getKeyCacheRecentHitRate()
    {
        return keyCache.getMetrics().getRecentHitRate();
    }

    public double getRowCacheRecentHitRate()
    {
        return rowCache.getMetrics().getRecentHitRate();
    }

    public int getRowCacheSavePeriodInSeconds()
    {
        return databaseDescriptor.getRowCacheSavePeriod();
    }

    public void setRowCacheSavePeriodInSeconds(int seconds)
    {
        if (seconds < 0)
            throw new RuntimeException("RowCacheSavePeriodInSeconds must be non-negative.");

        databaseDescriptor.setRowCacheSavePeriod(seconds);
        rowCache.scheduleSaving(seconds, databaseDescriptor.getRowCacheKeysToSave());
    }

    public int getKeyCacheSavePeriodInSeconds()
    {
        return databaseDescriptor.getKeyCacheSavePeriod();
    }

    public void setKeyCacheSavePeriodInSeconds(int seconds)
    {
        if (seconds < 0)
            throw new RuntimeException("KeyCacheSavePeriodInSeconds must be non-negative.");

        databaseDescriptor.setKeyCacheSavePeriod(seconds);
        keyCache.scheduleSaving(seconds, databaseDescriptor.getKeyCacheKeysToSave());
    }

    public int getCounterCacheSavePeriodInSeconds()
    {
        return databaseDescriptor.getCounterCacheSavePeriod();
    }

    public void setCounterCacheSavePeriodInSeconds(int seconds)
    {
        if (seconds < 0)
            throw new RuntimeException("CounterCacheSavePeriodInSeconds must be non-negative.");

        databaseDescriptor.setCounterCacheSavePeriod(seconds);
        counterCache.scheduleSaving(seconds, databaseDescriptor.getCounterCacheKeysToSave());
    }

    public int getRowCacheKeysToSave()
    {
        return databaseDescriptor.getRowCacheKeysToSave();
    }

    public void setRowCacheKeysToSave(int count)
    {
        if (count < 0)
            throw new RuntimeException("RowCacheKeysToSave must be non-negative.");
        databaseDescriptor.setRowCacheKeysToSave(count);
        rowCache.scheduleSaving(getRowCacheSavePeriodInSeconds(), count);
    }

    public int getKeyCacheKeysToSave()
    {
        return databaseDescriptor.getKeyCacheKeysToSave();
    }

    public void setKeyCacheKeysToSave(int count)
    {
        if (count < 0)
            throw new RuntimeException("KeyCacheKeysToSave must be non-negative.");
        databaseDescriptor.setKeyCacheKeysToSave(count);
        keyCache.scheduleSaving(getKeyCacheSavePeriodInSeconds(), count);
    }

    public int getCounterCacheKeysToSave()
    {
        return databaseDescriptor.getCounterCacheKeysToSave();
    }

    public void setCounterCacheKeysToSave(int count)
    {
        if (count < 0)
            throw new RuntimeException("CounterCacheKeysToSave must be non-negative.");
        databaseDescriptor.setCounterCacheKeysToSave(count);
        counterCache.scheduleSaving(getCounterCacheSavePeriodInSeconds(), count);
    }

    public void invalidateKeyCache()
    {
        keyCache.clear();
    }

    public void invalidateKeyCacheForCf(UUID cfId)
    {
        Iterator<KeyCacheKey> keyCacheIterator = keyCache.getKeySet().iterator();
        while (keyCacheIterator.hasNext())
        {
            KeyCacheKey key = keyCacheIterator.next();
            if (key.cfId.equals(cfId))
                keyCacheIterator.remove();
        }
    }

    public void invalidateRowCache()
    {
        rowCache.clear();
    }

    public void invalidateRowCacheForCf(UUID cfId)
    {
        Iterator<RowCacheKey> rowCacheIterator = rowCache.getKeySet().iterator();
        while (rowCacheIterator.hasNext())
        {
            RowCacheKey rowCacheKey = rowCacheIterator.next();
            if (rowCacheKey.cfId.equals(cfId))
                rowCacheIterator.remove();
        }
    }

    public void invalidateCounterCacheForCf(UUID cfId)
    {
        Iterator<CounterCacheKey> counterCacheIterator = counterCache.getKeySet().iterator();
        while (counterCacheIterator.hasNext())
        {
            CounterCacheKey counterCacheKey = counterCacheIterator.next();
            if (counterCacheKey.cfId.equals(cfId))
                counterCacheIterator.remove();
        }
    }

    public void invalidateCounterCache()
    {
        counterCache.clear();
    }

    public long getRowCacheCapacityInBytes()
    {
        return rowCache.getMetrics().capacity.value();
    }

    public long getRowCacheCapacityInMB()
    {
        return getRowCacheCapacityInBytes() / 1024 / 1024;
    }

    public void setRowCacheCapacityInMB(long capacity)
    {
        if (capacity < 0)
            throw new RuntimeException("capacity should not be negative.");

        rowCache.setCapacity(capacity * 1024 * 1024);
    }

    public long getKeyCacheCapacityInBytes()
    {
        return keyCache.getMetrics().capacity.value();
    }

    public long getKeyCacheCapacityInMB()
    {
        return getKeyCacheCapacityInBytes() / 1024 / 1024;
    }

    public void setKeyCacheCapacityInMB(long capacity)
    {
        if (capacity < 0)
            throw new RuntimeException("capacity should not be negative.");

        keyCache.setCapacity(capacity * 1024 * 1024);
    }

    public void setCounterCacheCapacityInMB(long capacity)
    {
        if (capacity < 0)
            throw new RuntimeException("capacity should not be negative.");

        counterCache.setCapacity(capacity * 1024 * 1024);
    }

    public long getRowCacheSize()
    {
        return rowCache.getMetrics().size.value();
    }

    public long getRowCacheEntries()
    {
        return rowCache.size();
    }

    public long getKeyCacheSize()
    {
        return keyCache.getMetrics().size.value();
    }

    public long getKeyCacheEntries()
    {
        return keyCache.size();
    }

    public void saveCaches() throws ExecutionException, InterruptedException
    {
        List<Future<?>> futures = new ArrayList<>(3);
        logger.debug("submitting cache saves");

        futures.add(keyCache.submitWrite(databaseDescriptor.getKeyCacheKeysToSave()));
        futures.add(rowCache.submitWrite(databaseDescriptor.getRowCacheKeysToSave()));
        futures.add(counterCache.submitWrite(databaseDescriptor.getCounterCacheKeysToSave()));

        FBUtilities.waitOnFutures(futures);
        logger.debug("cache saves completed");
    }

    private class CounterCacheSerializer implements CacheSerializer<CounterCacheKey, ClockAndCount>
    {
        public void serialize(CounterCacheKey key, DataOutputPlus out) throws IOException
        {
            ByteBufferUtil.writeWithLength(key.partitionKey, out);
            ByteBufferUtil.writeWithLength(key.cellName, out);
        }

        public Future<Pair<CounterCacheKey, ClockAndCount>> deserialize(DataInputStream in, final ColumnFamilyStore cfs) throws IOException
        {
            final ByteBuffer partitionKey = ByteBufferUtil.readWithLength(in);
            final CellName cellName = cfs.metadata.comparator.cellFromByteBuffer(ByteBufferUtil.readWithLength(in));
            return stageManager.getStage(Stage.READ).submit(new Callable<Pair<CounterCacheKey, ClockAndCount>>()
            {
                public Pair<CounterCacheKey, ClockAndCount> call() throws Exception
                {
                    DecoratedKey key = cfs.partitioner.decorateKey(partitionKey);
                    QueryFilter filter = QueryFilter.getNamesFilter(key,
                                                                    cfs.metadata.cfName,
                                                                    FBUtilities.singleton(cellName, cfs.metadata.comparator),
                                                                    Long.MIN_VALUE);
                    ColumnFamily cf = cfs.getTopLevelColumns(filter, Integer.MIN_VALUE);
                    if (cf == null)
                        return null;
                    Cell cell = cf.getColumn(cellName);
                    if (cell == null || !cell.isLive(Long.MIN_VALUE))
                        return null;
                    ClockAndCount clockAndCount = CounterContext.instance().getLocalClockAndCount(cell.value());
                    return Pair.create(CounterCacheKey.create(cfs.metadata.cfId, partitionKey, cellName), clockAndCount);
                }
            });
        }
    }

    private static class RowCacheSerializer implements CacheSerializer<RowCacheKey, IRowCacheEntry>
    {
        public void serialize(RowCacheKey key, DataOutputPlus out) throws IOException
        {
            ByteBufferUtil.writeWithLength(key.key, out);
        }

        public Future<Pair<RowCacheKey, IRowCacheEntry>> deserialize(DataInputStream in, final ColumnFamilyStore cfs) throws IOException
        {
            final ByteBuffer buffer = ByteBufferUtil.readWithLength(in);
            return StageManager.instance.getStage(Stage.READ).submit(new Callable<Pair<RowCacheKey, IRowCacheEntry>>()
            {
                public Pair<RowCacheKey, IRowCacheEntry> call() throws Exception
                {
                    DecoratedKey key = cfs.partitioner.decorateKey(buffer);
                    QueryFilter cacheFilter = new QueryFilter(key, cfs.getColumnFamilyName(), cfs.readFilterForCache(), Integer.MIN_VALUE);
                    ColumnFamily data = cfs.getTopLevelColumns(cacheFilter, Integer.MIN_VALUE);
                    return Pair.create(new RowCacheKey(cfs.metadata.cfId, key), (IRowCacheEntry) data);
                }
            });
        }
    }

    private class KeyCacheSerializer implements CacheSerializer<KeyCacheKey, RowIndexEntry>
    {
        public void serialize(KeyCacheKey key, DataOutputPlus out) throws IOException
        {
            RowIndexEntry entry = CacheService.instance.keyCache.get(key);
            if (entry == null)
                return;
            ByteBufferUtil.writeWithLength(key.key, out);
            Descriptor desc = key.desc;
            out.writeInt(desc.generation);
            out.writeBoolean(true);
            CFMetaData cfm = schema.getCFMetaData(key.desc.ksname, key.desc.cfname);
            cfm.comparator.rowIndexEntrySerializer().serialize(entry, out);
        }

        public Future<Pair<KeyCacheKey, RowIndexEntry>> deserialize(DataInputStream input, ColumnFamilyStore cfs) throws IOException
        {
            int keyLength = input.readInt();
            if (keyLength > FBUtilities.MAX_UNSIGNED_SHORT)
            {
                throw new IOException(String.format("Corrupted key cache. Key length of %d is longer than maximum of %d",
                                                    keyLength, FBUtilities.MAX_UNSIGNED_SHORT));
            }
            ByteBuffer key = ByteBufferUtil.read(input, keyLength);
            int generation = input.readInt();
            SSTableReader reader = findDesc(generation, cfs.getSSTables());
            input.readBoolean(); // backwards compatibility for "promoted indexes" boolean
            if (reader == null)
            {
                RowIndexEntry.Serializer.skipPromotedIndex(input);
                return null;
            }
            RowIndexEntry entry = reader.metadata.comparator.rowIndexEntrySerializer().deserialize(input, reader.descriptor.version);
            return Futures.immediateFuture(Pair.create(new KeyCacheKey(cfs.metadata.cfId, reader.descriptor, key), entry));
        }

        private SSTableReader findDesc(int generation, Collection<SSTableReader> collection)
        {
            for (SSTableReader sstable : collection)
            {
                if (sstable.descriptor.generation == generation)
                    return sstable;
            }
            return null;
        }
    }
}

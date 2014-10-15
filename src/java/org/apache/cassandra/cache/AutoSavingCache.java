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
package org.apache.cassandra.cache;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.cliffc.high_scale_lib.NonBlockingHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.LengthAvailableInputStream;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;

public class AutoSavingCache<K extends CacheKey, V> extends InstrumentingCache<K, V>
{
    private static final Logger logger = LoggerFactory.getLogger(AutoSavingCache.class);

    /** True if a cache flush is currently executing: only one may execute at a time. */
    public static final Set<CacheService.CacheType> flushInProgress = new NonBlockingHashSet<CacheService.CacheType>();

    protected volatile ScheduledFuture<?> saveTask;
    protected final CacheService.CacheType cacheType;

    protected final ICacheSaver<K, V> cacheSaver;

    public AutoSavingCache(ICache<K, V> cache, CacheService.CacheType cacheType, ICacheSaver<K, V> cacheSaver)
    {
        super(cacheType.toString(), cache);
        this.cacheType = cacheType;
        this.cacheSaver = cacheSaver;
    }

    public File getCachePath(String ksName, String cfName, UUID cfId, String version)
    {
        return DatabaseDescriptor.getSerializedCachePath(ksName, cfName, cfId, cacheType, version);
    }

    public Writer getWriter(int keysToSave)
    {
        return new Writer(keysToSave);
    }

    public void scheduleSaving(int savePeriodInSeconds, final int keysToSave)
    {
        if (saveTask != null)
        {
            saveTask.cancel(false); // Do not interrupt an in-progress save
            saveTask = null;
        }
        if (savePeriodInSeconds > 0)
        {
            Runnable runnable = new Runnable()
            {
                public void run()
                {
                    submitWrite(keysToSave);
                }
            };
            saveTask = StorageService.optionalTasks.scheduleWithFixedDelay(runnable,
                                                                           savePeriodInSeconds,
                                                                           savePeriodInSeconds,
                                                                           TimeUnit.SECONDS);
        }
    }

    public int loadSaved(ColumnFamilyStore cfs)
    {
        int count = 0;
        long start = System.nanoTime();

        ICacheSaver.Reader<K, V> reader = cacheSaver.getReader(cfs);
        Iterator<Future<Pair<K, V>>> keyIter = reader.read();
        while (keyIter.hasNext())
        {
            try
            {
                Pair<K, V> entry = keyIter.next().get();
                count++;
                if (entry != null)
                    put(entry.left, entry.right);
            }
            catch (InterruptedException | ExecutionException e)
            {
                logger.debug(String.format("harmless error reading saved cache %s", cfs), e);
            }
        }
        if (logger.isDebugEnabled())
            logger.debug("completed reading ({} ms; {} keys) saved cache {}",
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start), count, cfs);
        return count;
    }

    public Future<?> submitWrite(int keysToSave)
    {
        return CompactionManager.instance.submitCacheWrite(getWriter(keysToSave));
    }

    public class Writer extends CompactionInfo.Holder
    {
        private final Set<K> keys;
        private final CompactionInfo info;
        private long keysWritten;

        protected Writer(int keysToSave)
        {
            if (keysToSave >= getKeySet().size())
                keys = getKeySet();
            else
                keys = hotKeySet(keysToSave);

            OperationType type;
            if (cacheType == CacheService.CacheType.KEY_CACHE)
                type = OperationType.KEY_CACHE_SAVE;
            else if (cacheType == CacheService.CacheType.ROW_CACHE)
                type = OperationType.ROW_CACHE_SAVE;
            else if (cacheType == CacheService.CacheType.COUNTER_CACHE)
                type = OperationType.COUNTER_CACHE_SAVE;
            else
                type = OperationType.UNKNOWN;

            info = new CompactionInfo(CFMetaData.denseCFMetaData(Keyspace.SYSTEM_KS, cacheType.toString(), BytesType.instance),
                                      type,
                                      0,
                                      keys.size(),
                                      "keys");
        }

        public CacheService.CacheType cacheType()
        {
            return cacheType;
        }

        public CompactionInfo getCompactionInfo()
        {
            // keyset can change in size, thus total can too
            return info.forProgress(keysWritten, Math.max(keysWritten, keys.size()));
        }

        public void saveCache()
        {
            keysWritten = cacheSaver.getWriter().write(keys.iterator());
        }
    }

}

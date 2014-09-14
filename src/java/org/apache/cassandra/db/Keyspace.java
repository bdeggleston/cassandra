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
package org.apache.cassandra.db;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

import org.apache.cassandra.locator.LocatorConfig;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.pager.QueryPagers;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.metrics.KeyspaceMetrics;

/**
 * It represents a Keyspace.
 */
public class Keyspace
{
    public static final String SYSTEM_KS = "system";
    private static final int DEFAULT_PAGE_SIZE = 10000;

    private static final Logger logger = LoggerFactory.getLogger(Keyspace.class);

    public final KeyspaceMetrics metric;

    public final KSMetaData metadata;
    public final OpOrder writeOrder = new OpOrder();

    /* ColumnFamilyStore per column family */
    private final ConcurrentMap<UUID, ColumnFamilyStore> columnFamilyStores = new ConcurrentHashMap<UUID, ColumnFamilyStore>();
    private volatile AbstractReplicationStrategy replicationStrategy;

    public static Keyspace clear(String keyspaceName, Schema schema)
    {
        synchronized (Keyspace.class)
        {
            Keyspace t = schema.removeKeyspaceInstance(keyspaceName);
            if (t != null)
            {
                for (ColumnFamilyStore cfs : t.getColumnFamilyStores())
                    t.unloadCf(cfs);
                t.metric.release();
            }
            return t;
        }
    }

    public Collection<ColumnFamilyStore> getColumnFamilyStores()
    {
        return Collections.unmodifiableCollection(columnFamilyStores.values());
    }

    public ColumnFamilyStore getColumnFamilyStore(String cfName)
    {
        UUID id = schema.getId(getName(), cfName);
        if (id == null)
            throw new IllegalArgumentException(String.format("Unknown keyspace/cf pair (%s.%s)", getName(), cfName));
        return getColumnFamilyStore(id);
    }

    public ColumnFamilyStore getColumnFamilyStore(UUID id)
    {
        ColumnFamilyStore cfs = columnFamilyStores.get(id);
        if (cfs == null)
            throw new IllegalArgumentException("Unknown CF " + id);
        return cfs;
    }

    /**
     * Take a snapshot of the specific column family, or the entire set of column families
     * if columnFamily is null with a given timestamp
     *
     * @param snapshotName     the tag associated with the name of the snapshot.  This value may not be null
     * @param columnFamilyName the column family to snapshot or all on null
     * @throws IOException if the column family doesn't exist
     */
    public void snapshot(String snapshotName, String columnFamilyName) throws IOException
    {
        assert snapshotName != null;
        boolean tookSnapShot = false;
        for (ColumnFamilyStore cfStore : columnFamilyStores.values())
        {
            if (columnFamilyName == null || cfStore.name.equals(columnFamilyName))
            {
                tookSnapShot = true;
                cfStore.snapshot(snapshotName);
            }
        }

        if ((columnFamilyName != null) && !tookSnapShot)
            throw new IOException("Failed taking snapshot. Table " + columnFamilyName + " does not exist.");
    }

    /**
     * @param clientSuppliedName may be null.
     * @return the name of the snapshot
     */
    public static String getTimestampedSnapshotName(String clientSuppliedName)
    {
        String snapshotName = Long.toString(System.currentTimeMillis());
        if (clientSuppliedName != null && !clientSuppliedName.equals(""))
        {
            snapshotName = snapshotName + "-" + clientSuppliedName;
        }
        return snapshotName;
    }

    /**
     * Check whether snapshots already exists for a given name.
     *
     * @param snapshotName the user supplied snapshot name
     * @return true if the snapshot exists
     */
    public boolean snapshotExists(String snapshotName)
    {
        assert snapshotName != null;
        for (ColumnFamilyStore cfStore : columnFamilyStores.values())
        {
            if (cfStore.snapshotExists(snapshotName))
                return true;
        }
        return false;
    }

    /**
     * Clear all the snapshots for a given keyspace.
     *
     * @param snapshotName the user supplied snapshot name. It empty or null,
     *                     all the snapshots will be cleaned
     */
    public static void clearSnapshot(String snapshotName, String keyspace, Directories.DataDirectory[] dataDirectories)
    {
        List<File> snapshotDirs = Directories.getKSChildDirectories(keyspace, dataDirectories);
        Directories.clearSnapshot(snapshotName, snapshotDirs);
    }

    /**
     * @return A list of open SSTableReaders
     */
    public List<SSTableReader> getAllSSTables()
    {
        List<SSTableReader> list = new ArrayList<SSTableReader>(columnFamilyStores.size());
        for (ColumnFamilyStore cfStore : columnFamilyStores.values())
            list.addAll(cfStore.getSSTables());
        return list;
    }

    private final Tracing tracing;
    private final Schema schema;
    private final ColumnFamilyStoreManager columnFamilyStoreManager;
    private final LocatorConfig locatorConfig;
    private final CommitLog commitLog;

    Keyspace(String keyspaceName, boolean loadSSTables, Tracing tracing, Schema schema, ColumnFamilyStoreManager columnFamilyStoreManager, LocatorConfig locatorConfig, CommitLog commitLog)
    {
        this.tracing = tracing;
        this.schema = schema;
        this.columnFamilyStoreManager = columnFamilyStoreManager;
        this.locatorConfig = locatorConfig;
        this.commitLog = commitLog;
        metadata = schema.getKSMetaData(keyspaceName);
        assert metadata != null : "Unknown keyspace " + keyspaceName;

        createReplicationStrategy(metadata);

        this.metric = new KeyspaceMetrics(this);
        for (CFMetaData cfm : new ArrayList<CFMetaData>(metadata.cfMetaData().values()))
        {
            logger.debug("Initializing {}.{}", getName(), cfm.cfName);
            initCf(cfm.cfId, cfm.cfName, loadSSTables);
        }
    }

    public void createReplicationStrategy(KSMetaData ksm)
    {
        replicationStrategy = AbstractReplicationStrategy.createReplicationStrategy(ksm.name,
                                                                                    ksm.strategyClass,
                                                                                    locatorConfig.getTokenMetadata(),
                                                                                    locatorConfig.getEndpointSnitch(),
                                                                                    ksm.strategyOptions,
                                                                                    locatorConfig);
    }

    // best invoked on the compaction mananger.
    public void dropCf(UUID cfId)
    {
        assert columnFamilyStores.containsKey(cfId);
        ColumnFamilyStore cfs = columnFamilyStores.remove(cfId);
        if (cfs == null)
            return;

        // wait for any outstanding reads/writes that might affect the CFS
        cfs.keyspace.writeOrder.awaitNewBarrier();
        cfs.readOrdering.awaitNewBarrier();

        unloadCf(cfs);
    }

    // disassociate a cfs from this keyspace instance.
    private void unloadCf(ColumnFamilyStore cfs)
    {
        cfs.forceBlockingFlush();
        cfs.invalidate();
    }

    /**
     * adds a cf to internal structures, ends up creating disk files).
     */
    public void initCf(UUID cfId, String cfName, boolean loadSSTables)
    {
        ColumnFamilyStore cfs = columnFamilyStores.get(cfId);

        if (cfs == null)
        {
            // CFS being created for the first time, either on server startup or new CF being added.
            // We don't worry about races here; startup is safe, and adding multiple idential CFs
            // simultaneously is a "don't do that" scenario.
            ColumnFamilyStore oldCfs = columnFamilyStores.putIfAbsent(cfId, columnFamilyStoreManager.createColumnFamilyStore(this, cfName, loadSSTables));
            // CFS mbean instantiation will error out before we hit this, but in case that changes...
            if (oldCfs != null)
                throw new IllegalStateException("added multiple mappings for cf id " + cfId);
        }
        else
        {
            // re-initializing an existing CF.  This will happen if you cleared the schema
            // on this node and it's getting repopulated from the rest of the cluster.
            assert cfs.name.equals(cfName);
            cfs.metadata.reload();
            cfs.reload();
        }
    }

    public Row getRow(QueryFilter filter)
    {
        ColumnFamilyStore cfStore = getColumnFamilyStore(filter.getColumnFamilyName());
        ColumnFamily columnFamily = cfStore.getColumnFamily(filter);
        return new Row(filter.key, columnFamily);
    }

    public void apply(Mutation mutation, boolean writeCommitLog)
    {
        apply(mutation, writeCommitLog, true);
    }

    /**
     * This method appends a row to the global CommitLog, then updates memtables and indexes.
     *
     * @param mutation       the row to write.  Must not be modified after calling apply, since commitlog append
     *                       may happen concurrently, depending on the CL Executor type.
     * @param writeCommitLog false to disable commitlog append entirely
     * @param updateIndexes  false to disable index updates (used by CollationController "defragmenting")
     */
    public void apply(Mutation mutation, boolean writeCommitLog, boolean updateIndexes)
    {
        try (OpOrder.Group opGroup = writeOrder.start())
        {
            // write the mutation to the commitlog and memtables
            ReplayPosition replayPosition = null;
            if (writeCommitLog)
            {
                tracing.trace("Appending to commitlog");
                replayPosition = commitLog.add(mutation);
            }

            DecoratedKey key = locatorConfig.getPartitioner().decorateKey(mutation.key());
            for (ColumnFamily cf : mutation.getColumnFamilies())
            {
                ColumnFamilyStore cfs = columnFamilyStores.get(cf.id());
                if (cfs == null)
                {
                    logger.error("Attempting to mutate non-existant table {}", cf.id());
                    continue;
                }

                tracing.trace("Adding to {} memtable", cf.metadata().cfName);
                SecondaryIndexManager.Updater updater = updateIndexes
                                                      ? cfs.indexManager.updaterFor(key, cf, opGroup)
                                                      : SecondaryIndexManager.nullUpdater;
                cfs.apply(key, cf, updater, opGroup, replayPosition);
            }
        }
    }

    public AbstractReplicationStrategy getReplicationStrategy()
    {
        return replicationStrategy;
    }

    /**
     * @param key row to index
     * @param cfs ColumnFamily to index row in
     * @param idxNames columns to index, in comparator order
     */
    public static void indexRow(DecoratedKey key,
                                ColumnFamilyStore cfs,
                                Set<String> idxNames,
                                DatabaseDescriptor databaseDescriptor,
                                Tracing tracing,
                                Schema schema,
                                KeyspaceManager keyspaceManager,
                                StorageProxy storageProxy,
                                MessagingService messagingService,
                                LocatorConfig locatorConfig,
                                DBConfig dbConfig)
    {
        if (logger.isDebugEnabled())
            logger.debug("Indexing row {} ", cfs.metadata.getKeyValidator().getString(key.getKey()));

        try (OpOrder.Group opGroup = cfs.keyspace.writeOrder.start())
        {
            Set<SecondaryIndex> indexes = cfs.indexManager.getIndexesByNames(idxNames);

            Iterator<ColumnFamily> pager = QueryPagers.pageRowLocally(cfs,
                                                                      key.getKey(),
                                                                      DEFAULT_PAGE_SIZE,
                                                                      databaseDescriptor,
                                                                      tracing,
                                                                      schema,
                                                                      keyspaceManager,
                                                                      storageProxy,
                                                                      messagingService,
                                                                      locatorConfig,
                                                                      dbConfig);
            while (pager.hasNext())
            {
                ColumnFamily cf = pager.next();
                ColumnFamily cf2 = cf.cloneMeShallow();
                for (Cell cell : cf)
                {
                    if (cfs.indexManager.indexes(cell.name(), indexes))
                        cf2.addColumn(cell);
                }
                cfs.indexManager.indexRow(key.getKey(), cf2, opGroup);
            }
        }
    }

    public List<Future<?>> flush()
    {
        List<Future<?>> futures = new ArrayList<Future<?>>(columnFamilyStores.size());
        for (UUID cfId : columnFamilyStores.keySet())
            futures.add(columnFamilyStores.get(cfId).forceFlush());
        return futures;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(name='" + getName() + "')";
    }

    public String getName()
    {
        return metadata.name;
    }
}

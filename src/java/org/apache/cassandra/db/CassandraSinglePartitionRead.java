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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.cassandra.cache.IRowCacheEntry;
import org.apache.cassandra.cache.RowCacheKey;
import org.apache.cassandra.cache.RowCacheSentinel;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.CachedBTreePartition;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorWithLowerBound;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.SinglePartitionPager;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.btree.BTreeSet;

public class CassandraSinglePartitionRead extends AbstractReadExecutable
{
    private final SinglePartitionReadCommand command;
    private int oldestUnrepairedTombstone = Integer.MAX_VALUE;

    public CassandraSinglePartitionRead(SinglePartitionReadCommand command)
    {
        super(command);
        this.command = command;
    }

    public PartitionIterator execute(ConsistencyLevel consistency, ClientState clientState, long queryStartNanoTime) throws RequestExecutionException
    {
        return StorageProxy.read(SinglePartitionReadCommand.Group.one(command), consistency, clientState, queryStartNanoTime);
    }

    @Override
    protected void recordLatency(TableMetrics metric, long latencyNanos)
    {
        metric.readLatency.addNano(latencyNanos);
    }

    @Override
    protected int oldestUnrepairedTombstone()
    {
        return oldestUnrepairedTombstone;
    }

    private static SinglePartitionPager getPager(SinglePartitionReadCommand command, PagingState pagingState, ProtocolVersion protocolVersion)
    {
        return new SinglePartitionPager(command, pagingState, protocolVersion);
    }

    @SuppressWarnings("resource") // we close the created iterator through closing the result of this method (and SingletonUnfilteredPartitionIterator ctor cannot fail)
    protected UnfilteredPartitionIterator queryStorage(ReadContext context)
    {
        return cfs.isRowCacheEnabled() ? getThroughCache(cfs, context) : executeDirect(context);
    }

    /**
     * Fetch the rows requested if in cache; if not, read it from disk and cache it.
     * <p>
     * If the partition is cached, and the filter given is within its bounds, we return
     * from cache, otherwise from disk.
     * <p>
     * If the partition is is not cached, we figure out what filter is "biggest", read
     * that from disk, then filter the result and either cache that or return it.
     */
    private UnfilteredPartitionIterator getThroughCache(ColumnFamilyStore cfs, ReadContext context)
    {
        assert !cfs.isIndex(); // CASSANDRA-5732
        assert cfs.isRowCacheEnabled() : String.format("Row cache is not enabled on table [%s]", cfs.name);

        RowCacheKey key = new RowCacheKey(command.metadata(), command.partitionKey());

        // Attempt a sentinel-read-cache sequence.  if a write invalidates our sentinel, we'll return our
        // (now potentially obsolete) data, but won't cache it. see CASSANDRA-3862
        // TODO: don't evict entire partitions on writes (#2864)
        IRowCacheEntry cached = CacheService.instance.rowCache.get(key);
        if (cached != null)
        {
            if (cached instanceof RowCacheSentinel)
            {
                // Some other read is trying to cache the value, just do a normal non-caching read
                Tracing.trace("Row cache miss (race)");
                cfs.metric.rowCacheMiss.inc();
                return executeDirect(context);
            }

            CachedPartition cachedPartition = (CachedPartition)cached;
            if (cfs.isFilterFullyCoveredBy(command.clusteringIndexFilter(), command.limits(), cachedPartition, command.nowInSec(), command.metadata().enforceStrictLiveness()))
            {
                cfs.metric.rowCacheHit.inc();
                Tracing.trace("Row cache hit");
                UnfilteredRowIterator unfilteredRowIterator = command.clusteringIndexFilter().getUnfilteredRowIterator(command.columnFilter(), cachedPartition);
                cfs.metric.updateSSTableIterated(0);
                return new SingletonUnfilteredPartitionIterator(unfilteredRowIterator);
            }

            cfs.metric.rowCacheHitOutOfRange.inc();
            Tracing.trace("Ignoring row cache as cached value could not satisfy query");
            return executeDirect(context);
        }

        cfs.metric.rowCacheMiss.inc();
        Tracing.trace("Row cache miss");

        // Note that on tables with no clustering keys, any positive value of
        // rowsToCache implies caching the full partition
        boolean cacheFullPartitions = command.metadata().clusteringColumns().size() > 0 ?
                                      command.metadata().params.caching.cacheAllRows() :
                                      command.metadata().params.caching.cacheRows();

        // To be able to cache what we read, what we read must at least covers what the cache holds, that
        // is the 'rowsToCache' first rows of the partition. We could read those 'rowsToCache' first rows
        // systematically, but we'd have to "extend" that to whatever is needed for the user query that the
        // 'rowsToCache' first rows don't cover and it's not trivial with our existing filters. So currently
        // we settle for caching what we read only if the user query does query the head of the partition since
        // that's the common case of when we'll be able to use the cache anyway. One exception is if we cache
        // full partitions, in which case we just always read it all and cache.
        if (cacheFullPartitions || command.clusteringIndexFilter().isHeadFilter())
        {
            RowCacheSentinel sentinel = new RowCacheSentinel();
            boolean sentinelSuccess = CacheService.instance.rowCache.putIfAbsent(key, sentinel);
            boolean sentinelReplaced = false;

            try
            {
                final int rowsToCache = command.metadata().params.caching.rowsPerPartitionToCache();
                final boolean enforceStrictLiveness = command.metadata().enforceStrictLiveness();

                SinglePartitionReadCommand newCommand = SinglePartitionReadCommand.fullPartitionRead(command.metadata(),
                                                                                                     command.nowInSec(),
                                                                                                     command.partitionKey());
                CassandraSinglePartitionRead newExecutable = new CassandraSinglePartitionRead(newCommand);
                @SuppressWarnings("resource") // we close on exception or upon closing the result of this method
                UnfilteredRowIterator iter = UnfilteredPartitionIterators.getOnlyElement(newExecutable.executeDirect(context), command);
                try
                {
                    // Use a custom iterator instead of DataLimits to avoid stopping the original iterator
                    UnfilteredRowIterator toCacheIterator = new WrappingUnfilteredRowIterator(iter)
                    {
                        private int rowsCounted = 0;

                        @Override
                        public boolean hasNext()
                        {
                            return rowsCounted < rowsToCache && super.hasNext();
                        }

                        @Override
                        public Unfiltered next()
                        {
                            Unfiltered unfiltered = super.next();
                            if (unfiltered.isRow())
                            {
                                Row row = (Row) unfiltered;
                                if (row.hasLiveData(command.nowInSec(), enforceStrictLiveness))
                                    rowsCounted++;
                            }
                            return unfiltered;
                        }
                    };

                    // We want to cache only rowsToCache rows
                    CachedPartition toCache = CachedBTreePartition.create(toCacheIterator, command.nowInSec());
                    if (sentinelSuccess && !toCache.isEmpty())
                    {
                        Tracing.trace("Caching {} rows", toCache.rowCount());
                        CacheService.instance.rowCache.replace(key, sentinel, toCache);
                        // Whether or not the previous replace has worked, our sentinel is not in the cache anymore
                        sentinelReplaced = true;
                    }

                    // We then re-filter out what this query wants.
                    // Note that in the case where we don't cache full partitions, it's possible that the current query is interested in more
                    // than what we've cached, so we can't just use toCache.
                    UnfilteredRowIterator cacheIterator = command.clusteringIndexFilter().getUnfilteredRowIterator(command.columnFilter(), toCache);
                    if (cacheFullPartitions)
                    {
                        // Everything is guaranteed to be in 'toCache', we're done with 'iter'
                        assert !iter.hasNext();
                        iter.close();
                        return new SingletonUnfilteredPartitionIterator(cacheIterator);
                    }
                    return new SingletonUnfilteredPartitionIterator(UnfilteredRowIterators.concat(cacheIterator,
                                                                                                  command.clusteringIndexFilter()
                                                                                                         .filterNotIndexed(command.columnFilter(), iter)));
                }
                catch (RuntimeException | Error e)
                {
                    iter.close();
                    throw e;
                }
            }
            finally
            {
                if (sentinelSuccess && !sentinelReplaced)
                    cfs.invalidateCachedPartition(key);
            }
        }

        Tracing.trace("Fetching data but not populating cache as query does not query from the start of the partition");
        return executeDirect(context);
    }

    /**
     * Queries both memtable and sstables to fetch the result of this query.
     * <p>
     * Please note that this method:
     *   1) does not check the row cache.
     *   2) does not apply the query limit, nor the row filter (and so ignore 2ndary indexes).
     *      Those are applied in {@link ReadExecutable#executeLocally}.
     *   3) does not record some of the read metrics (latency, scanned cells histograms) nor
     *      throws TombstoneOverwhelmingException.
     * It is publicly exposed because there is a few places where that is exactly what we want,
     * but it should be used only where you know you don't need thoses things.
     * <p>
     * Also note that one must have created a {@code ReadExecutionController} on the queried table and we require it as
     * a parameter to enforce that fact, even though it's not explicitlly used by the method.
     */
    public UnfilteredPartitionIterator executeDirect(ReadContext context)
    {
        assert context != null && context.validForReadOn(cfs);
        Tracing.trace("Executing single-partition query on {}", cfs.name);

        return new SingletonUnfilteredPartitionIterator(queryMemtableAndDiskInternal());
    }

    private UnfilteredRowIterator queryMemtableAndDiskInternal()
    {
        /*
         * We have 2 main strategies:
         *   1) We query memtables and sstables simulateneously. This is our most generic strategy and the one we use
         *      unless we have a names filter that we know we can optimize futher.
         *   2) If we have a name filter (so we query specific rows), we can make a bet: that all column for all queried row
         *      will have data in the most recent sstable(s), thus saving us from reading older ones. This does imply we
         *      have a way to guarantee we have all the data for what is queried, which is only possible for name queries
         *      and if we have neither non-frozen collections/UDTs nor counters (indeed, for a non-frozen collection or UDT,
         *      we can't guarantee an older sstable won't have some elements that weren't in the most recent sstables,
         *      and counters are intrinsically a collection of shards and so have the same problem).
         */
        if (command.clusteringIndexFilter() instanceof ClusteringIndexNamesFilter && !queriesMulticellType())
            return queryMemtableAndSSTablesInTimestampOrder(cfs, (ClusteringIndexNamesFilter)command.clusteringIndexFilter());

        Tracing.trace("Acquiring sstable references");
        ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, command.partitionKey()));
        List<UnfilteredRowIterator> iterators = new ArrayList<>(Iterables.size(view.memtables) + view.sstables.size());
        ClusteringIndexFilter filter = command.clusteringIndexFilter();
        long minTimestamp = Long.MAX_VALUE;

        try
        {
            for (Memtable memtable : view.memtables)
            {
                Partition partition = memtable.getPartition(command.partitionKey());
                if (partition == null)
                    continue;

                minTimestamp = Math.min(minTimestamp, memtable.getMinTimestamp());

                @SuppressWarnings("resource") // 'iter' is added to iterators which is closed on exception, or through the closing of the final merged iterator
                UnfilteredRowIterator iter = filter.getUnfilteredRowIterator(command.columnFilter(), partition);
                oldestUnrepairedTombstone = Math.min(oldestUnrepairedTombstone, partition.stats().minLocalDeletionTime);
                iterators.add(iter);
            }

            /*
             * We can't eliminate full sstables based on the timestamp of what we've already read like
             * in collectTimeOrderedData, but we still want to eliminate sstable whose maxTimestamp < mostRecentTombstone
             * we've read. We still rely on the sstable ordering by maxTimestamp since if
             *   maxTimestamp_s1 > maxTimestamp_s0,
             * we're guaranteed that s1 cannot have a row tombstone such that
             *   timestamp(tombstone) > maxTimestamp_s0
             * since we necessarily have
             *   timestamp(tombstone) <= maxTimestamp_s1
             * In other words, iterating in maxTimestamp order allow to do our mostRecentPartitionTombstone elimination
             * in one pass, and minimize the number of sstables for which we read a partition tombstone.
             */
            Collections.sort(view.sstables, SSTableReader.maxTimestampComparator);
            long mostRecentPartitionTombstone = Long.MIN_VALUE;
            int nonIntersectingSSTables = 0;
            List<SSTableReader> skippedSSTablesWithTombstones = null;
            SSTableReadMetricsCollector metricsCollector = new SSTableReadMetricsCollector();

            for (SSTableReader sstable : view.sstables)
            {
                // if we've already seen a partition tombstone with a timestamp greater
                // than the most recent update to this sstable, we can skip it
                if (sstable.getMaxTimestamp() < mostRecentPartitionTombstone)
                    break;

                if (!shouldInclude(sstable))
                {
                    nonIntersectingSSTables++;
                    if (sstable.mayHaveTombstones())
                    { // if sstable has tombstones we need to check after one pass if it can be safely skipped
                        if (skippedSSTablesWithTombstones == null)
                            skippedSSTablesWithTombstones = new ArrayList<>();
                        skippedSSTablesWithTombstones.add(sstable);

                    }
                    continue;
                }

                minTimestamp = Math.min(minTimestamp, sstable.getMinTimestamp());

                @SuppressWarnings("resource") // 'iter' is added to iterators which is closed on exception,
                // or through the closing of the final merged iterator
                UnfilteredRowIteratorWithLowerBound iter = makeIterator(cfs, sstable, metricsCollector);
                if (!sstable.isRepaired())
                    oldestUnrepairedTombstone = Math.min(oldestUnrepairedTombstone, sstable.getMinLocalDeletionTime());

                iterators.add(iter);
                mostRecentPartitionTombstone = Math.max(mostRecentPartitionTombstone,
                                                        iter.partitionLevelDeletion().markedForDeleteAt());
            }

            int includedDueToTombstones = 0;
            // Check for sstables with tombstones that are not expired
            if (skippedSSTablesWithTombstones != null)
            {
                for (SSTableReader sstable : skippedSSTablesWithTombstones)
                {
                    if (sstable.getMaxTimestamp() <= minTimestamp)
                        continue;

                    @SuppressWarnings("resource") // 'iter' is added to iterators which is close on exception,
                    // or through the closing of the final merged iterator
                    UnfilteredRowIteratorWithLowerBound iter = makeIterator(cfs, sstable, metricsCollector);
                    if (!sstable.isRepaired())
                        oldestUnrepairedTombstone = Math.min(oldestUnrepairedTombstone, sstable.getMinLocalDeletionTime());

                    iterators.add(iter);
                    includedDueToTombstones++;
                }
            }
            if (Tracing.isTracing())
                Tracing.trace("Skipped {}/{} non-slice-intersecting sstables, included {} due to tombstones",
                              nonIntersectingSSTables, view.sstables.size(), includedDueToTombstones);

            if (iterators.isEmpty())
                return EmptyIterators.unfilteredRow(cfs.metadata(), command.partitionKey(), filter.isReversed());

            StorageHook.instance.reportRead(cfs.metadata().id, command.partitionKey());
            return withSSTablesIterated(iterators, cfs.metric, metricsCollector);
        }
        catch (RuntimeException | Error e)
        {
            try
            {
                FBUtilities.closeAll(iterators);
            }
            catch (Exception suppressed)
            {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    private UnfilteredRowIteratorWithLowerBound makeIterator(ColumnFamilyStore cfs,
                                                             SSTableReader sstable,
                                                             SSTableReadsListener listener)
    {
        return StorageHook.instance.makeRowIteratorWithLowerBound(cfs,
                                                                  command.partitionKey(),
                                                                  sstable,
                                                                  command.clusteringIndexFilter(),
                                                                  command.columnFilter(),
                                                                  listener);

    }


    /**
     * Return a wrapped iterator that when closed will update the sstables iterated and READ sample metrics.
     * Note that we cannot use the Transformations framework because they greedily get the static row, which
     * would cause all iterators to be initialized and hence all sstables to be accessed.
     */
    private UnfilteredRowIterator withSSTablesIterated(List<UnfilteredRowIterator> iterators,
                                                       TableMetrics metrics,
                                                       SSTableReadMetricsCollector metricsCollector)
    {
        @SuppressWarnings("resource") //  Closed through the closing of the result of the caller method.
        UnfilteredRowIterator merged = UnfilteredRowIterators.merge(iterators, command.nowInSec());

        if (!merged.isEmpty())
        {
            DecoratedKey key = merged.partitionKey();
            metrics.samplers.get(TableMetrics.Sampler.READS).addSample(key.getKey(), key.hashCode(), 1);
        }

        class UpdateSstablesIterated extends Transformation
        {
            public void onPartitionClose()
            {
                int mergedSSTablesIterated = metricsCollector.getMergedSSTables();
                metrics.updateSSTableIterated(mergedSSTablesIterated);
                Tracing.trace("Merged data from memtables and {} sstables", mergedSSTablesIterated);
            }
        };
        return Transformation.apply(merged, new UpdateSstablesIterated());
    }


    private boolean queriesMulticellType()
    {
        for (ColumnMetadata column : command.columnFilter().fetchedColumns())
        {
            if (column.type.isMultiCell() || column.type.isCounter())
                return true;
        }
        return false;
    }


    /**
     * Do a read by querying the memtable(s) first, and then each relevant sstables sequentially by order of the sstable
     * max timestamp.
     *
     * This is used for names query in the hope of only having to query the 1 or 2 most recent query and then knowing nothing
     * more recent could be in the older sstables (which we can only guarantee if we know exactly which row we queries, and if
     * no collection or counters are included).
     * This method assumes the filter is a {@code ClusteringIndexNamesFilter}.
     */
    private UnfilteredRowIterator queryMemtableAndSSTablesInTimestampOrder(ColumnFamilyStore cfs, ClusteringIndexNamesFilter filter)
    {
        Tracing.trace("Acquiring sstable references");
        ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, command.partitionKey()));

        ImmutableBTreePartition result = null;

        Tracing.trace("Merging memtable contents");
        for (Memtable memtable : view.memtables)
        {
            Partition partition = memtable.getPartition(command.partitionKey());
            if (partition == null)
                continue;

            try (UnfilteredRowIterator iter = filter.getUnfilteredRowIterator(command.columnFilter(), partition))
            {
                if (iter.isEmpty())
                    continue;

                result = add(iter, result, filter, false);
            }
        }

        /* add the SSTables on disk */
        Collections.sort(view.sstables, SSTableReader.maxTimestampComparator);
        boolean onlyUnrepaired = true;
        // read sorted sstables
        SSTableReadMetricsCollector metricsCollector = new SSTableReadMetricsCollector();
        for (SSTableReader sstable : view.sstables)
        {
            // if we've already seen a partition tombstone with a timestamp greater
            // than the most recent update to this sstable, we're done, since the rest of the sstables
            // will also be older
            if (result != null && sstable.getMaxTimestamp() < result.partitionLevelDeletion().markedForDeleteAt())
                break;

            long currentMaxTs = sstable.getMaxTimestamp();
            filter = reduceFilter(filter, result, currentMaxTs);
            if (filter == null)
                break;

            if (!shouldInclude(sstable))
            {
                // This mean that nothing queried by the filter can be in the sstable. One exception is the top-level partition deletion
                // however: if it is set, it impacts everything and must be included. Getting that top-level partition deletion costs us
                // some seek in general however (unless the partition is indexed and is in the key cache), so we first check if the sstable
                // has any tombstone at all as a shortcut.
                if (!sstable.mayHaveTombstones())
                    continue; // no tombstone at all, we can skip that sstable

                // We need to get the partition deletion and include it if it's live. In any case though, we're done with that sstable.
                try (UnfilteredRowIterator iter = StorageHook.instance.makeRowIterator(cfs,
                                                                                       sstable,
                                                                                       command.partitionKey(),
                                                                                       filter.getSlices(command.metadata()),
                                                                                       command.columnFilter(),
                                                                                       filter.isReversed(),
                                                                                       metricsCollector))
                {
                    if (!iter.partitionLevelDeletion().isLive())
                        result = add(UnfilteredRowIterators.noRowsIterator(iter.metadata(), iter.partitionKey(), Rows.EMPTY_STATIC_ROW, iter.partitionLevelDeletion(), filter.isReversed()), result, filter, sstable.isRepaired());
                    else
                        result = add(iter, result, filter, sstable.isRepaired());
                }
                continue;
            }

            try (UnfilteredRowIterator iter = StorageHook.instance.makeRowIterator(cfs,
                                                                                   sstable,
                                                                                   command.partitionKey(),
                                                                                   filter.getSlices(command.metadata()),
                                                                                   command.columnFilter(),
                                                                                   filter.isReversed(),
                                                                                   metricsCollector))
            {
                if (iter.isEmpty())
                    continue;

                if (sstable.isRepaired())
                    onlyUnrepaired = false;
                result = add(iter, result, filter, sstable.isRepaired());
            }
        }

        cfs.metric.updateSSTableIterated(metricsCollector.getMergedSSTables());

        if (result == null || result.isEmpty())
            return EmptyIterators.unfilteredRow(command.metadata(), command.partitionKey(), false);

        DecoratedKey key = result.partitionKey();
        cfs.metric.samplers.get(TableMetrics.Sampler.READS).addSample(key.getKey(), key.hashCode(), 1);
        StorageHook.instance.reportRead(cfs.metadata.id, command.partitionKey());

        // "hoist up" the requested data into a more recent sstable
        if (metricsCollector.getMergedSSTables() > cfs.getMinimumCompactionThreshold()
            && onlyUnrepaired
            && !cfs.isAutoCompactionDisabled()
            && cfs.getCompactionStrategyManager().shouldDefragment())
        {
            // !!WARNING!!   if we stop copying our data to a heap-managed object,
            //               we will need to track the lifetime of this mutation as well
            Tracing.trace("Defragmenting requested data");

            try (UnfilteredRowIterator iter = result.unfilteredIterator(command.columnFilter(), Slices.ALL, false))
            {
                final Mutation mutation = new Mutation(PartitionUpdate.fromIterator(iter, command.columnFilter()));
                StageManager.getStage(Stage.MUTATION).execute(() -> {
                    // skipping commitlog and index updates is fine since we're just de-fragmenting existing data
                    Keyspace.open(mutation.getKeyspaceName()).apply(mutation, false, false);
                });
            }
        }

        return result.unfilteredIterator(command.columnFilter(), Slices.ALL, command.clusteringIndexFilter().isReversed());
    }

    private ImmutableBTreePartition add(UnfilteredRowIterator iter, ImmutableBTreePartition result, ClusteringIndexNamesFilter filter, boolean isRepaired)
    {
        if (!isRepaired)
            oldestUnrepairedTombstone = Math.min(oldestUnrepairedTombstone, iter.stats().minLocalDeletionTime);

        int maxRows = Math.max(filter.requestedRows().size(), 1);
        if (result == null)
            return ImmutableBTreePartition.create(iter, maxRows);

        try (UnfilteredRowIterator merged = UnfilteredRowIterators.merge(Arrays.asList(iter, result.unfilteredIterator(command.columnFilter(), Slices.ALL, filter.isReversed())), command.nowInSec()))
        {
            return ImmutableBTreePartition.create(merged, maxRows);
        }
    }

    private ClusteringIndexNamesFilter reduceFilter(ClusteringIndexNamesFilter filter, Partition result, long sstableTimestamp)
    {
        if (result == null)
            return filter;

        SearchIterator<Clustering, Row> searchIter = result.searchIterator(command.columnFilter(), false);

        RegularAndStaticColumns columns = command.columnFilter().fetchedColumns();
        NavigableSet<Clustering> clusterings = filter.requestedRows();

        // We want to remove rows for which we have values for all requested columns. We have to deal with both static and regular rows.
        // TODO: we could also remove a selected column if we've found values for every requested row but we'll leave
        // that for later.

        boolean removeStatic = false;
        if (!columns.statics.isEmpty())
        {
            Row staticRow = searchIter.next(Clustering.STATIC_CLUSTERING);
            removeStatic = staticRow != null && canRemoveRow(staticRow, columns.statics, sstableTimestamp);
        }

        NavigableSet<Clustering> toRemove = null;
        for (Clustering clustering : clusterings)
        {
            Row row = searchIter.next(clustering);
            if (row == null || !canRemoveRow(row, columns.regulars, sstableTimestamp))
                continue;

            if (toRemove == null)
                toRemove = new TreeSet<>(result.metadata().comparator);
            toRemove.add(clustering);
        }

        if (!removeStatic && toRemove == null)
            return filter;

        // Check if we have everything we need
        boolean hasNoMoreStatic = columns.statics.isEmpty() || removeStatic;
        boolean hasNoMoreClusterings = clusterings.isEmpty() || (toRemove != null && toRemove.size() == clusterings.size());
        if (hasNoMoreStatic && hasNoMoreClusterings)
            return null;

        if (toRemove != null)
        {
            BTreeSet.Builder<Clustering> newClusterings = BTreeSet.builder(result.metadata().comparator);
            newClusterings.addAll(Sets.difference(clusterings, toRemove));
            clusterings = newClusterings.build();
        }
        return new ClusteringIndexNamesFilter(clusterings, filter.isReversed());
    }

    private boolean canRemoveRow(Row row, Columns requestedColumns, long sstableTimestamp)
    {
        // We can remove a row if it has data that is more recent that the next sstable to consider for the data that the query
        // cares about. And the data we care about is 1) the row timestamp (since every query cares if the row exists or not)
        // and 2) the requested columns.
        if (row.primaryKeyLivenessInfo().isEmpty() || row.primaryKeyLivenessInfo().timestamp() <= sstableTimestamp)
            return false;

        for (ColumnMetadata column : requestedColumns)
        {
            Cell cell = row.getCell(column);
            if (cell == null || cell.timestamp() <= sstableTimestamp)
                return false;
        }
        return true;
    }

    private boolean shouldInclude(SSTableReader sstable)
    {
        // If some static columns are queried, we should always include the sstable: the clustering values stats of the sstable
        // don't tell us if the sstable contains static values in particular.
        // TODO: we could record if a sstable contains any static value at all.
        if (!command.columnFilter().fetchedColumns().statics.isEmpty())
            return true;

        return command.clusteringIndexFilter().shouldInclude(sstable);
    }

    /**
     * {@code SSTableReaderListener} used to collect metrics about SSTable read access.
     */
    private static final class SSTableReadMetricsCollector implements SSTableReadsListener
    {
        /**
         * The number of SSTables that need to be merged. This counter is only updated for single partition queries
         * since this has been the behavior so far.
         */
        private int mergedSSTables;

        @Override
        public void onSSTableSelected(SSTableReader sstable, RowIndexEntry<?> indexEntry, SelectionReason reason)
        {
            sstable.incrementReadCount();
            mergedSSTables++;
        }

        /**
         * Returns the number of SSTables that need to be merged.
         * @return the number of SSTables that need to be merged.
         */
        public int getMergedSSTables()
        {
            return mergedSSTables;
        }
    }
}

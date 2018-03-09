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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorWithLowerBound;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.btree.BTreeSet;

public class CassandraStorageHandler implements StorageHandler
{
    private final ColumnFamilyStore cfs;

    public CassandraStorageHandler(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
    }

    @Override
    public UnfilteredRowIterator querySinglePartition(SinglePartitionReadCommand readCommand)
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
        ClusteringIndexFilter filter = readCommand.clusteringIndexFilter();
        DecoratedKey partitionKey = readCommand.partitionKey();
        ColumnFilter columnFilter = readCommand.columnFilter();

        if (filter instanceof ClusteringIndexNamesFilter && !queriesMulticellType(readCommand))
            return queryMemtableAndSSTablesInTimestampOrder(cfs, readCommand, (ClusteringIndexNamesFilter)filter);

        Tracing.trace("Acquiring sstable references");
        ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, partitionKey));
        List<UnfilteredRowIterator> iterators = new ArrayList<>(Iterables.size(view.memtables) + view.sstables.size());
        long minTimestamp = Long.MAX_VALUE;

        try
        {
            for (Memtable memtable : view.memtables)
            {
                Partition partition = memtable.getPartition(partitionKey);
                if (partition == null)
                    continue;

                minTimestamp = Math.min(minTimestamp, memtable.getMinTimestamp());

                @SuppressWarnings("resource") // 'iter' is added to iterators which is closed on exception, or through the closing of the final merged iterator
                UnfilteredRowIterator iter = filter.getUnfilteredRowIterator(columnFilter, partition);
                readCommand.updateUnrepairedTombstone(partition.stats().minLocalDeletionTime);
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

                if (!shouldInclude(readCommand, sstable))
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
                UnfilteredRowIteratorWithLowerBound iter = makeIterator(cfs, readCommand, sstable, metricsCollector);
                if (!sstable.isRepaired())
                    readCommand.updateUnrepairedTombstone(sstable.getMinLocalDeletionTime());

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
                    UnfilteredRowIteratorWithLowerBound iter = makeIterator(cfs, readCommand, sstable, metricsCollector);
                    if (!sstable.isRepaired())
                        readCommand.updateUnrepairedTombstone(sstable.getMinLocalDeletionTime());

                    iterators.add(iter);
                    includedDueToTombstones++;
                }
            }
            if (Tracing.isTracing())
                Tracing.trace("Skipped {}/{} non-slice-intersecting sstables, included {} due to tombstones",
                              nonIntersectingSSTables, view.sstables.size(), includedDueToTombstones);

            if (iterators.isEmpty())
                return EmptyIterators.unfilteredRow(cfs.metadata(), partitionKey, filter.isReversed());

            StorageHook.instance.reportRead(cfs.metadata().id, partitionKey);
            return withSSTablesIterated(readCommand, iterators, cfs.metric, metricsCollector);
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

    private boolean shouldInclude(SinglePartitionReadCommand readCommand, SSTableReader sstable)
    {
        // If some static columns are queried, we should always include the sstable: the clustering values stats of the sstable
        // don't tell us if the sstable contains static values in particular.
        // TODO: we could record if a sstable contains any static value at all.
        if (!readCommand.columnFilter().fetchedColumns().statics.isEmpty())
            return true;

        List<ByteBuffer> minClusteringValues = sstable.getSSTableMetadata().minClusteringValues;
        List<ByteBuffer> maxClusteringValues = sstable.getSSTableMetadata().maxClusteringValues;
        return readCommand.clusteringIndexFilter().intersects(sstable.metadata().comparator,
                                                              minClusteringValues,
                                                              maxClusteringValues);
    }

    private UnfilteredRowIteratorWithLowerBound makeIterator(ColumnFamilyStore cfs,
                                                             SinglePartitionReadCommand readCommand,
                                                             SSTableReader sstable,
                                                             SSTableReadsListener listener)
    {
        return StorageHook.instance.makeRowIteratorWithLowerBound(cfs,
                                                                  readCommand.partitionKey(),
                                                                  sstable,
                                                                  readCommand.clusteringIndexFilter(),
                                                                  readCommand.columnFilter(),
                                                                  listener);

    }

    /**
     * Return a wrapped iterator that when closed will update the sstables iterated and READ sample metrics.
     * Note that we cannot use the Transformations framework because they greedily get the static row, which
     * would cause all iterators to be initialized and hence all sstables to be accessed.
     */
    private UnfilteredRowIterator withSSTablesIterated(SinglePartitionReadCommand readCommand,
                                                       List<UnfilteredRowIterator> iterators,
                                                       TableMetrics metrics,
                                                       SSTableReadMetricsCollector metricsCollector)
    {
        @SuppressWarnings("resource") //  Closed through the closing of the result of the caller method.
        UnfilteredRowIterator merged = UnfilteredRowIterators.merge(iterators, readCommand.nowInSec());

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

    private boolean queriesMulticellType(SinglePartitionReadCommand readCommand)
    {
        for (ColumnMetadata column : readCommand.columnFilter().fetchedColumns())
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
    private UnfilteredRowIterator queryMemtableAndSSTablesInTimestampOrder(ColumnFamilyStore cfs,
                                                                           SinglePartitionReadCommand readCommand,
                                                                           ClusteringIndexNamesFilter filter)
    {
        Tracing.trace("Acquiring sstable references");
        DecoratedKey partitionKey = readCommand.partitionKey();
        ColumnFilter columnFilter = readCommand.columnFilter();
        TableMetadata metadata = readCommand.metadata();
        ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, partitionKey));

        ImmutableBTreePartition result = null;

        Tracing.trace("Merging memtable contents");
        for (Memtable memtable : view.memtables)
        {
            Partition partition = memtable.getPartition(partitionKey);
            if (partition == null)
                continue;

            try (UnfilteredRowIterator iter = filter.getUnfilteredRowIterator(columnFilter, partition))
            {
                if (iter.isEmpty())
                    continue;

                result = add(readCommand, iter, result, filter, false);
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
            filter = reduceFilter(readCommand, filter, result, currentMaxTs);
            if (filter == null)
                break;

            if (!shouldInclude(readCommand, sstable))
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
                                                                                       partitionKey,
                                                                                       filter.getSlices(metadata),
                                                                                       columnFilter,
                                                                                       filter.isReversed(),
                                                                                       metricsCollector))
                {
                    if (!iter.partitionLevelDeletion().isLive())
                        result = add(readCommand,
                                     UnfilteredRowIterators.noRowsIterator(iter.metadata(),
                                                                           iter.partitionKey(),
                                                                           Rows.EMPTY_STATIC_ROW,
                                                                           iter.partitionLevelDeletion(),
                                                                           filter.isReversed()),
                                     result, filter, sstable.isRepaired());
                    else
                        result = add(readCommand, iter, result, filter, sstable.isRepaired());
                }
                continue;
            }

            try (UnfilteredRowIterator iter = StorageHook.instance.makeRowIterator(cfs,
                                                                                   sstable,
                                                                                   partitionKey,
                                                                                   filter.getSlices(metadata),
                                                                                   columnFilter,
                                                                                   filter.isReversed(),
                                                                                   metricsCollector))
            {
                if (iter.isEmpty())
                    continue;

                if (sstable.isRepaired())
                    onlyUnrepaired = false;
                result = add(readCommand, iter, result, filter, sstable.isRepaired());
            }
        }

        cfs.metric.updateSSTableIterated(metricsCollector.getMergedSSTables());

        if (result == null || result.isEmpty())
            return EmptyIterators.unfilteredRow(metadata, partitionKey, false);

        DecoratedKey key = result.partitionKey();
        cfs.metric.samplers.get(TableMetrics.Sampler.READS).addSample(key.getKey(), key.hashCode(), 1);
        StorageHook.instance.reportRead(cfs.metadata.id, partitionKey);

        // "hoist up" the requested data into a more recent sstable
        if (metricsCollector.getMergedSSTables() > cfs.getMinimumCompactionThreshold()
            && onlyUnrepaired
            && !cfs.isAutoCompactionDisabled()
            && cfs.getCompactionStrategyManager().shouldDefragment())
        {
            // !!WARNING!!   if we stop copying our data to a heap-managed object,
            //               we will need to track the lifetime of this mutation as well
            Tracing.trace("Defragmenting requested data");

            try (UnfilteredRowIterator iter = result.unfilteredIterator(columnFilter, Slices.ALL, false))
            {
                final Mutation mutation = new Mutation(PartitionUpdate.fromIterator(iter, columnFilter));
                StageManager.getStage(Stage.MUTATION).execute(() -> {
                    // skipping commitlog and index updates is fine since we're just de-fragmenting existing data
                    Keyspace.open(mutation.getKeyspaceName()).apply(mutation, false, false);
                });
            }
        }

        return result.unfilteredIterator(columnFilter, Slices.ALL, readCommand.clusteringIndexFilter().isReversed());
    }

    private ImmutableBTreePartition add(SinglePartitionReadCommand readCommand,
                                        UnfilteredRowIterator iter,
                                        ImmutableBTreePartition result,
                                        ClusteringIndexNamesFilter filter,
                                        boolean isRepaired)
    {
        if (!isRepaired)
            readCommand.updateUnrepairedTombstone(iter.stats().minLocalDeletionTime);

        int maxRows = Math.max(filter.requestedRows().size(), 1);
        if (result == null)
            return ImmutableBTreePartition.create(iter, maxRows);

        try (UnfilteredRowIterator merged =
                UnfilteredRowIterators.merge(Arrays.asList(iter, result.unfilteredIterator(readCommand.columnFilter(),
                                                                                           Slices.ALL,
                                                                                           filter.isReversed())),
                                             readCommand.nowInSec()))
        {
            return ImmutableBTreePartition.create(merged, maxRows);
        }
    }

    private ClusteringIndexNamesFilter reduceFilter(SinglePartitionReadCommand readCommand,
                                                    ClusteringIndexNamesFilter filter,
                                                    Partition result,
                                                    long sstableTimestamp)
    {
        if (result == null)
            return filter;

        SearchIterator<Clustering, Row> searchIter = result.searchIterator(readCommand.columnFilter(), false);

        RegularAndStaticColumns columns = readCommand.columnFilter().fetchedColumns();
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

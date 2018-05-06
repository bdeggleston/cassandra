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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.lifecycle.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.pager.*;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.btree.BTreeSet;

/**
 * A read command that selects a (part of a) single partition.
 */
public class SinglePartitionReadCommand extends ReadCommand
{
    protected static final SelectionDeserializer selectionDeserializer = new Deserializer();

    private final DecoratedKey partitionKey;
    private final ClusteringIndexFilter clusteringIndexFilter;

    private int oldestUnrepairedTombstone = Integer.MAX_VALUE;

    @VisibleForTesting
    protected SinglePartitionReadCommand(boolean isDigest,
                                         int digestVersion,
                                         TableMetadata metadata,
                                         int nowInSec,
                                         ColumnFilter columnFilter,
                                         RowFilter rowFilter,
                                         DataLimits limits,
                                         DecoratedKey partitionKey,
                                         ClusteringIndexFilter clusteringIndexFilter,
                                         IndexMetadata index)
    {
        super(Kind.SINGLE_PARTITION, isDigest, digestVersion, metadata, nowInSec, columnFilter, rowFilter, limits, index);
        assert partitionKey.getPartitioner() == metadata.partitioner;
        this.partitionKey = partitionKey;
        this.clusteringIndexFilter = clusteringIndexFilter;
    }

    /**
     * Creates a new read command on a single partition.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param columnFilter the column filter to use for the query.
     * @param rowFilter the row filter to use for the query.
     * @param limits the limits to use for the query.
     * @param partitionKey the partition key for the partition to query.
     * @param clusteringIndexFilter the clustering index filter to use for the query.
     * @param indexMetadata explicitly specified index to use for the query
     *
     * @return a newly created read command.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata,
                                                    int nowInSec,
                                                    ColumnFilter columnFilter,
                                                    RowFilter rowFilter,
                                                    DataLimits limits,
                                                    DecoratedKey partitionKey,
                                                    ClusteringIndexFilter clusteringIndexFilter,
                                                    IndexMetadata indexMetadata)
    {
        return new SinglePartitionReadCommand(false,
                                              0,
                                              metadata,
                                              nowInSec,
                                              columnFilter,
                                              rowFilter,
                                              limits,
                                              partitionKey,
                                              clusteringIndexFilter,
                                              indexMetadata);
    }

    /**
     * Creates a new read command on a single partition.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param columnFilter the column filter to use for the query.
     * @param rowFilter the row filter to use for the query.
     * @param limits the limits to use for the query.
     * @param partitionKey the partition key for the partition to query.
     * @param clusteringIndexFilter the clustering index filter to use for the query.
     *
     * @return a newly created read command.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata,
                                                    int nowInSec,
                                                    ColumnFilter columnFilter,
                                                    RowFilter rowFilter,
                                                    DataLimits limits,
                                                    DecoratedKey partitionKey,
                                                    ClusteringIndexFilter clusteringIndexFilter)
    {
        return create(metadata,
                      nowInSec,
                      columnFilter,
                      rowFilter,
                      limits,
                      partitionKey,
                      clusteringIndexFilter,
                      findIndex(metadata, rowFilter));
    }

    /**
     * Creates a new read command on a single partition.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param columnFilter the column filter to use for the query.
     * @param filter the clustering index filter to use for the query.
     *
     * @return a newly created read command. The returned command will use no row filter and have no limits.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata,
                                                    int nowInSec,
                                                    DecoratedKey key,
                                                    ColumnFilter columnFilter,
                                                    ClusteringIndexFilter filter)
    {
        return create(metadata, nowInSec, columnFilter, RowFilter.NONE, DataLimits.NONE, key, filter);
    }

    /**
     * Creates a new read command that queries a single partition in its entirety.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     *
     * @return a newly created read command that queries all the rows of {@code key}.
     */
    public static SinglePartitionReadCommand fullPartitionRead(TableMetadata metadata, int nowInSec, DecoratedKey key)
    {
        return create(metadata, nowInSec, key, Slices.ALL);
    }

    /**
     * Creates a new read command that queries a single partition in its entirety.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     *
     * @return a newly created read command that queries all the rows of {@code key}.
     */
    public static SinglePartitionReadCommand fullPartitionRead(TableMetadata metadata, int nowInSec, ByteBuffer key)
    {
        return create(metadata, nowInSec, metadata.partitioner.decorateKey(key), Slices.ALL);
    }

    /**
     * Creates a new single partition slice command for the provided single slice.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param slice the slice of rows to query.
     *
     * @return a newly created read command that queries {@code slice} in {@code key}. The returned query will
     * query every columns for the table (without limit or row filtering) and be in forward order.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata, int nowInSec, DecoratedKey key, Slice slice)
    {
        return create(metadata, nowInSec, key, Slices.with(metadata.comparator, slice));
    }

    /**
     * Creates a new single partition slice command for the provided slices.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param slices the slices of rows to query.
     *
     * @return a newly created read command that queries the {@code slices} in {@code key}. The returned query will
     * query every columns for the table (without limit or row filtering) and be in forward order.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata, int nowInSec, DecoratedKey key, Slices slices)
    {
        ClusteringIndexSliceFilter filter = new ClusteringIndexSliceFilter(slices, false);
        return create(metadata, nowInSec, ColumnFilter.all(metadata), RowFilter.NONE, DataLimits.NONE, key, filter);
    }

    /**
     * Creates a new single partition slice command for the provided slices.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param slices the slices of rows to query.
     *
     * @return a newly created read command that queries the {@code slices} in {@code key}. The returned query will
     * query every columns for the table (without limit or row filtering) and be in forward order.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata, int nowInSec, ByteBuffer key, Slices slices)
    {
        return create(metadata, nowInSec, metadata.partitioner.decorateKey(key), slices);
    }

    /**
     * Creates a new single partition name command for the provided rows.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param names the clustering for the rows to query.
     *
     * @return a newly created read command that queries the {@code names} in {@code key}. The returned query will
     * query every columns (without limit or row filtering) and be in forward order.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata, int nowInSec, DecoratedKey key, NavigableSet<Clustering> names)
    {
        ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(names, false);
        return create(metadata, nowInSec, ColumnFilter.all(metadata), RowFilter.NONE, DataLimits.NONE, key, filter);
    }

    /**
     * Creates a new single partition name command for the provided row.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param name the clustering for the row to query.
     *
     * @return a newly created read command that queries {@code name} in {@code key}. The returned query will
     * query every columns (without limit or row filtering).
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata, int nowInSec, DecoratedKey key, Clustering name)
    {
        return create(metadata, nowInSec, key, FBUtilities.singleton(name, metadata.comparator));
    }

    public SinglePartitionReadCommand copy()
    {
        return new SinglePartitionReadCommand(isDigestQuery(),
                                              digestVersion(),
                                              metadata(),
                                              nowInSec(),
                                              columnFilter(),
                                              rowFilter(),
                                              limits(),
                                              partitionKey(),
                                              clusteringIndexFilter(),
                                              indexMetadata());
    }

    public SinglePartitionReadCommand copyAsDigestQuery()
    {
        return new SinglePartitionReadCommand(true,
                                              digestVersion(),
                                              metadata(),
                                              nowInSec(),
                                              columnFilter(),
                                              rowFilter(),
                                              limits(),
                                              partitionKey(),
                                              clusteringIndexFilter(),
                                              indexMetadata());
    }

    public SinglePartitionReadCommand withUpdatedLimit(DataLimits newLimits)
    {
        return new SinglePartitionReadCommand(isDigestQuery(),
                                              digestVersion(),
                                              metadata(),
                                              nowInSec(),
                                              columnFilter(),
                                              rowFilter(),
                                              newLimits,
                                              partitionKey(),
                                              clusteringIndexFilter(),
                                              indexMetadata());
    }

    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    public ClusteringIndexFilter clusteringIndexFilter()
    {
        return clusteringIndexFilter;
    }

    public ClusteringIndexFilter clusteringIndexFilter(DecoratedKey key)
    {
        return clusteringIndexFilter;
    }

    public long getTimeout()
    {
        return DatabaseDescriptor.getReadRpcTimeout();
    }

    public boolean selectsKey(DecoratedKey key)
    {
        if (!this.partitionKey().equals(key))
            return false;

        return rowFilter().partitionKeyRestrictionsAreSatisfiedBy(key, metadata().partitionKeyType);
    }

    public boolean selectsClustering(DecoratedKey key, Clustering clustering)
    {
        if (clustering == Clustering.STATIC_CLUSTERING)
            return !columnFilter().fetchedColumns().statics.isEmpty();

        if (!clusteringIndexFilter().selects(clustering))
            return false;

        return rowFilter().clusteringKeyRestrictionsAreSatisfiedBy(clustering);
    }

    /**
     * Returns a new command suitable to paging from the last returned row.
     *
     * @param lastReturned the last row returned by the previous page. The newly created command
     * will only query row that comes after this (in query order). This can be {@code null} if this
     * is the first page.
     * @param limits the limits to use for the page to query.
     *
     * @return the newly create command.
     */
    public SinglePartitionReadCommand forPaging(Clustering lastReturned, DataLimits limits)
    {
        // We shouldn't have set digest yet when reaching that point
        assert !isDigestQuery();
        return create(metadata(),
                      nowInSec(),
                      columnFilter(),
                      rowFilter(),
                      limits,
                      partitionKey(),
                      lastReturned == null ? clusteringIndexFilter() : clusteringIndexFilter.forPaging(metadata().comparator, lastReturned, false));
    }

    public PartitionIterator execute(ConsistencyLevel consistency, ClientState clientState, long queryStartNanoTime) throws RequestExecutionException
    {
        return StorageProxy.read(Group.one(this), consistency, clientState, queryStartNanoTime);
    }

    @Override
    public SinglePartitionPager getPager(PagingState pagingState, ProtocolVersion protocolVersion)
    {
        return getPager(this, pagingState, protocolVersion);
    }

    private static SinglePartitionPager getPager(SinglePartitionReadCommand command, PagingState pagingState, ProtocolVersion protocolVersion)
    {
        return new SinglePartitionPager(command, pagingState, protocolVersion);
    }

    protected void recordLatency(TableMetrics metric, long latencyNanos)
    {
        metric.readLatency.addNano(latencyNanos);
    }

    private boolean shouldInclude(SSTableReader sstable)
    {
        // If some static columns are queried, we should always include the sstable: the clustering values stats of the sstable
        // don't tell us if the sstable contains static values in particular.
        // TODO: we could record if a sstable contains any static value at all.
        if (!columnFilter().fetchedColumns().statics.isEmpty())
            return true;

        return clusteringIndexFilter().shouldInclude(sstable);
    }

    private UnfilteredRowIteratorWithLowerBound makeIterator(ColumnFamilyStore cfs,
                                                             SSTableReader sstable,
                                                             SSTableReadsListener listener)
    {
        return StorageHook.instance.makeRowIteratorWithLowerBound(cfs,
                                                                  partitionKey(),
                                                                  sstable,
                                                                  clusteringIndexFilter(),
                                                                  columnFilter(),
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
        UnfilteredRowIterator merged = UnfilteredRowIterators.merge(iterators, nowInSec());

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
        for (ColumnMetadata column : columnFilter().fetchedColumns())
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
        ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, partitionKey()));

        ImmutableBTreePartition result = null;

        Tracing.trace("Merging memtable contents");
        for (Memtable memtable : view.memtables)
        {
            Partition partition = memtable.getPartition(partitionKey());
            if (partition == null)
                continue;

            try (UnfilteredRowIterator iter = filter.getUnfilteredRowIterator(columnFilter(), partition))
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
                                                                                       partitionKey(),
                                                                                       filter.getSlices(metadata()),
                                                                                       columnFilter(),
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
                                                                                   partitionKey(),
                                                                                   filter.getSlices(metadata()),
                                                                                   columnFilter(),
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
            return EmptyIterators.unfilteredRow(metadata(), partitionKey(), false);

        DecoratedKey key = result.partitionKey();
        cfs.metric.samplers.get(TableMetrics.Sampler.READS).addSample(key.getKey(), key.hashCode(), 1);
        StorageHook.instance.reportRead(cfs.metadata.id, partitionKey());

        // "hoist up" the requested data into a more recent sstable
        if (metricsCollector.getMergedSSTables() > cfs.getMinimumCompactionThreshold()
            && onlyUnrepaired
            && !cfs.isAutoCompactionDisabled()
            && cfs.getCompactionStrategyManager().shouldDefragment())
        {
            // !!WARNING!!   if we stop copying our data to a heap-managed object,
            //               we will need to track the lifetime of this mutation as well
            Tracing.trace("Defragmenting requested data");

            try (UnfilteredRowIterator iter = result.unfilteredIterator(columnFilter(), Slices.ALL, false))
            {
                final Mutation mutation = new Mutation(PartitionUpdate.fromIterator(iter, columnFilter()));
                StageManager.getStage(Stage.MUTATION).execute(() -> {
                    // skipping commitlog and index updates is fine since we're just de-fragmenting existing data
                    Keyspace.open(mutation.getKeyspaceName()).apply(mutation, false, false);
                });
            }
        }

        return result.unfilteredIterator(columnFilter(), Slices.ALL, clusteringIndexFilter().isReversed());
    }

    private ImmutableBTreePartition add(UnfilteredRowIterator iter, ImmutableBTreePartition result, ClusteringIndexNamesFilter filter, boolean isRepaired)
    {
        if (!isRepaired)
            oldestUnrepairedTombstone = Math.min(oldestUnrepairedTombstone, iter.stats().minLocalDeletionTime);

        int maxRows = Math.max(filter.requestedRows().size(), 1);
        if (result == null)
            return ImmutableBTreePartition.create(iter, maxRows);

        try (UnfilteredRowIterator merged = UnfilteredRowIterators.merge(Arrays.asList(iter, result.unfilteredIterator(columnFilter(), Slices.ALL, filter.isReversed())), nowInSec()))
        {
            return ImmutableBTreePartition.create(merged, maxRows);
        }
    }

    private ClusteringIndexNamesFilter reduceFilter(ClusteringIndexNamesFilter filter, Partition result, long sstableTimestamp)
    {
        if (result == null)
            return filter;

        SearchIterator<Clustering, Row> searchIter = result.searchIterator(columnFilter(), false);

        RegularAndStaticColumns columns = columnFilter().fetchedColumns();
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

    @Override
    public boolean selectsFullPartition()
    {
        return metadata().isStaticCompactTable() ||
               (clusteringIndexFilter.selectsAllPartition() && !rowFilter().hasExpressionOnClusteringOrRegularColumns());
    }

    @Override
    public String toString()
    {
        return String.format("Read(%s columns=%s rowFilter=%s limits=%s key=%s filter=%s, nowInSec=%d)",
                             metadata().toString(),
                             columnFilter(),
                             rowFilter(),
                             limits(),
                             metadata().partitionKeyType.getString(partitionKey().getKey()),
                             clusteringIndexFilter.toString(metadata()),
                             nowInSec());
    }

    public MessageOut<ReadCommand> createMessage()
    {
        return new MessageOut<>(MessagingService.Verb.READ, this, serializer);
    }

    protected void appendCQLWhereClause(StringBuilder sb)
    {
        sb.append(" WHERE ");

        sb.append(ColumnMetadata.toCQLString(metadata().partitionKeyColumns())).append(" = ");
        DataRange.appendKeyString(sb, metadata().partitionKeyType, partitionKey().getKey());

        // We put the row filter first because the clustering index filter can end by "ORDER BY"
        if (!rowFilter().isEmpty())
            sb.append(" AND ").append(rowFilter());

        String filterString = clusteringIndexFilter().toCQLString(metadata());
        if (!filterString.isEmpty())
            sb.append(" AND ").append(filterString);
    }

    protected void serializeSelection(DataOutputPlus out, int version) throws IOException
    {
        metadata().partitionKeyType.writeValue(partitionKey().getKey(), out);
        ClusteringIndexFilter.serializer.serialize(clusteringIndexFilter(), out, version);
    }

    protected long selectionSerializedSize(int version)
    {
        return metadata().partitionKeyType.writtenLength(partitionKey().getKey())
             + ClusteringIndexFilter.serializer.serializedSize(clusteringIndexFilter(), version);
    }

    public boolean isLimitedToOnePartition()
    {
        return true;
    }

    /**
     * Groups multiple single partition read commands.
     */
    public static class Group implements QueryGroup
    {
        public final List<SinglePartitionReadCommand> commands;
        private final DataLimits limits;
        private final int nowInSec;
        private final boolean selectsFullPartitions;

        public Group(List<SinglePartitionReadCommand> commands, DataLimits limits)
        {
            assert !commands.isEmpty();
            this.commands = commands;
            this.limits = limits;
            SinglePartitionReadCommand firstCommand = commands.get(0);
            this.nowInSec = firstCommand.nowInSec();
            this.selectsFullPartitions = firstCommand.selectsFullPartition();
            for (int i = 1; i < commands.size(); i++)
                assert commands.get(i).nowInSec() == nowInSec;
        }

        public static Group one(SinglePartitionReadCommand command)
        {
            return new Group(Collections.singletonList(command), command.limits());
        }

        public PartitionIterator execute(ConsistencyLevel consistency, ClientState clientState, long queryStartNanoTime) throws RequestExecutionException
        {
            return StorageProxy.read(this, consistency, clientState, queryStartNanoTime);
        }

        public int nowInSec()
        {
            return nowInSec;
        }

        public DataLimits limits()
        {
            return limits;
        }

        public TableMetadata metadata()
        {
            return commands.get(0).metadata();
        }

        @Override
        public boolean selectsFullPartition()
        {
            return selectsFullPartitions;
        }

        public PartitionIterator executeInternal(ReadContext context)
        {
            // Note that the only difference between the command in a group must be the partition key on which
            // they applied.
            boolean enforceStrictLiveness = commands.get(0).metadata().enforceStrictLiveness();
            return limits.filter(UnfilteredPartitionIterators.filter(executeLocally(context, false), nowInSec),
                                 nowInSec,
                                 selectsFullPartitions,
                                 enforceStrictLiveness);
        }

        public UnfilteredPartitionIterator executeLocally(ReadContext context)
        {
            return executeLocally(context, true);
        }

        /**
         * Implementation of {@link QueryGroup#executeLocally(ReadContext)}.
         *
         * @param context - the {@code ReadContext} protecting the read.
         * @param sort - whether to sort the inner commands by partition key, required for merging the iterator
         *               later on. This will be false when called by {@link QueryGroup#executeInternal(ReadContext)}
         *               because in this case it is safe to do so as there is no merging involved and we don't want to
         *               change the old behavior which was to not sort by partition.
         *
         * @return - the iterator that can be used to retrieve the query result.
         */
        private UnfilteredPartitionIterator executeLocally(ReadContext context, boolean sort)
        {
            ReadHandler readHandler = commands.get(0).getCfs().getReadHandler();
            List<Pair<DecoratedKey, UnfilteredPartitionIterator>> partitions = new ArrayList<>(commands.size());
            for (SinglePartitionReadCommand cmd : commands)
                partitions.add(Pair.of(cmd.partitionKey, readHandler.executeLocally(context, cmd)));

            if (sort)
                Collections.sort(partitions, (p1, p2) -> p1.getLeft().compareTo(p2.getLeft()));

            return UnfilteredPartitionIterators.concat(partitions.stream().map(p -> p.getRight()).collect(Collectors.toList()));
        }

        public QueryPager getPager(PagingState pagingState, ProtocolVersion protocolVersion)
        {
            if (commands.size() == 1)
                return SinglePartitionReadCommand.getPager(commands.get(0), pagingState, protocolVersion);

            return new MultiPartitionPager(this, pagingState, protocolVersion);
        }

        public boolean selectsKey(DecoratedKey key)
        {
            return Iterables.any(commands, c -> c.selectsKey(key));
        }

        public boolean selectsClustering(DecoratedKey key, Clustering clustering)
        {
            return Iterables.any(commands, c -> c.selectsClustering(key, clustering));
        }

        @Override
        public ReadContext getReadContext()
        {
            if (commands.isEmpty())
                return ReadContext.empty();

            ReadCommand command = commands.get(0);
            return command.getCfs().getReadHandler().contextForCommand(command);
        }

        @Override
        public String toString()
        {
            return commands.toString();
        }
    }

    private static class Deserializer extends SelectionDeserializer
    {
        public ReadCommand deserialize(DataInputPlus in,
                                       int version,
                                       boolean isDigest,
                                       int digestVersion,
                                       TableMetadata metadata,
                                       int nowInSec,
                                       ColumnFilter columnFilter,
                                       RowFilter rowFilter,
                                       DataLimits limits,
                                       IndexMetadata index)
        throws IOException
        {
            DecoratedKey key = metadata.partitioner.decorateKey(metadata.partitionKeyType.readValue(in, DatabaseDescriptor.getMaxValueSize()));
            ClusteringIndexFilter filter = ClusteringIndexFilter.serializer.deserialize(in, version, metadata);
            return new SinglePartitionReadCommand(isDigest, digestVersion, metadata, nowInSec, columnFilter, rowFilter, limits, key, filter, index);
        }
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

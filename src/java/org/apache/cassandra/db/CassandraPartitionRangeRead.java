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
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.PartitionRangeQueryPager;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;

public class CassandraPartitionRangeRead extends AbstractReadExecutable
{
    private final PartitionRangeReadCommand command;
    private int oldestUnrepairedTombstone = Integer.MAX_VALUE;

    public CassandraPartitionRangeRead(PartitionRangeReadCommand command)
    {
        super(command);
        this.command = command;
    }

    @Override
    public PartitionIterator execute(ConsistencyLevel consistency, ClientState clientState, long queryStartNanoTime) throws RequestExecutionException
    {
        return StorageProxy.getRangeSlice(command, consistency, queryStartNanoTime);
    }

    @Override
    public QueryPager getPager(PagingState pagingState, ProtocolVersion protocolVersion)
    {
        return new PartitionRangeQueryPager(command, pagingState, protocolVersion);
    }

    @Override
    protected void recordLatency(TableMetrics metric, long latencyNanos)
    {
        metric.rangeLatency.addNano(latencyNanos);
    }

    @VisibleForTesting
    public UnfilteredPartitionIterator queryStorage(final ColumnFamilyStore cfs, ReadExecutionController executionController)
    {
        ColumnFamilyStore.ViewFragment view = cfs.select(View.selectLive(command.dataRange().keyRange()));
        Tracing.trace("Executing seq scan across {} sstables for {}", view.sstables.size(), command.dataRange().keyRange().getString(command.metadata().partitionKeyType));

        // fetch data from current memtable, historical memtables, and SSTables in the correct order.
        final List<UnfilteredPartitionIterator> iterators = new ArrayList<>(Iterables.size(view.memtables) + view.sstables.size());

        try
        {
            for (Memtable memtable : view.memtables)
            {
                @SuppressWarnings("resource") // We close on exception and on closing the result returned by this method
                Memtable.MemtableUnfilteredPartitionIterator iter = memtable.makePartitionIterator(command.columnFilter(), command.dataRange());
                oldestUnrepairedTombstone = Math.min(oldestUnrepairedTombstone, iter.getMinLocalDeletionTime());
                iterators.add(iter);
            }

            SSTableReadsListener readCountUpdater = newReadCountUpdater();
            for (SSTableReader sstable : view.sstables)
            {
                @SuppressWarnings("resource") // We close on exception and on closing the result returned by this method
                UnfilteredPartitionIterator iter = sstable.getScanner(command.columnFilter(), command.dataRange(), readCountUpdater);
                iterators.add(iter);
                if (!sstable.isRepaired())
                    oldestUnrepairedTombstone = Math.min(oldestUnrepairedTombstone, sstable.getMinLocalDeletionTime());
            }
            // iterators can be empty for offline tools
            return iterators.isEmpty() ? EmptyIterators.unfilteredPartition(command.metadata())
                                       : checkCacheFilter(UnfilteredPartitionIterators.mergeLazily(iterators, command.nowInSec()), cfs);
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

    /**
     * Creates a new {@code SSTableReadsListener} to update the SSTables read counts.
     * @return a new {@code SSTableReadsListener} to update the SSTables read counts.
     */
    private static SSTableReadsListener newReadCountUpdater()
    {
        return new SSTableReadsListener()
        {
            @Override
            public void onScanningStarted(SSTableReader sstable)
            {
                sstable.incrementReadCount();
            }
        };
    }

    @Override
    protected int oldestUnrepairedTombstone()
    {
        return oldestUnrepairedTombstone;
    }

    private UnfilteredPartitionIterator checkCacheFilter(UnfilteredPartitionIterator iter, final ColumnFamilyStore cfs)
    {
        class CacheFilter extends Transformation
        {
            @Override
            public BaseRowIterator applyToPartition(BaseRowIterator iter)
            {
                // Note that we rely on the fact that until we actually advance 'iter', no really costly operation is actually done
                // (except for reading the partition key from the index file) due to the call to mergeLazily in queryStorage.
                DecoratedKey dk = iter.partitionKey();

                // Check if this partition is in the rowCache and if it is, if  it covers our filter
                CachedPartition cached = cfs.getRawCachedPartition(dk);
                ClusteringIndexFilter filter = command.dataRange().clusteringIndexFilter(dk);

                if (cached != null && cfs.isFilterFullyCoveredBy(filter,
                                                                 command.limits(),
                                                                 cached,
                                                                 command.nowInSec(),
                                                                 iter.metadata().enforceStrictLiveness()))
                {
                    // We won't use 'iter' so close it now.
                    iter.close();

                    return filter.getUnfilteredRowIterator(command.columnFilter(), cached);
                }

                return iter;
            }
        }
        return Transformation.apply(iter, new CacheFilter());
    }


}

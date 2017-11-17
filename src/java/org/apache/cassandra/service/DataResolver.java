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

import java.net.InetAddress;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.reads.repair.ReadRepair;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.filter.DataLimits.Counter;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.*;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.net.*;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;

public class DataResolver extends ResponseResolver
{
    @VisibleForTesting
    final List<AsyncOneResponse> repairResults = Collections.synchronizedList(new ArrayList<>());
    private final long queryStartNanoTime;
    private final boolean enforceStrictLiveness;

    public DataResolver(Keyspace keyspace, ReadCommand command, ConsistencyLevel consistency, int maxResponseCount, long queryStartNanoTime, ReadRepair readRepair)
    {
        super(keyspace, command, consistency, readRepair, maxResponseCount);
        this.queryStartNanoTime = queryStartNanoTime;
        this.enforceStrictLiveness = command.metadata().enforceStrictLiveness();
    }

    public PartitionIterator getData()
    {
        ReadResponse response = responses.iterator().next().payload;
        return UnfilteredPartitionIterators.filter(response.makeIterator(command), command.nowInSec());
    }

    public boolean isDataPresent()
    {
        return !responses.isEmpty();
    }

    public PartitionIterator resolve()
    {
        // We could get more responses while this method runs, which is ok (we're happy to ignore any response not here
        // at the beginning of this method), so grab the response count once and use that through the method.
        int count = responses.size();
        List<UnfilteredPartitionIterator> iters = new ArrayList<>(count);
        InetAddress[] sources = new InetAddress[count];
        for (int i = 0; i < count; i++)
        {
            MessageIn<ReadResponse> msg = responses.get(i);
            iters.add(msg.payload.makeIterator(command));
            sources[i] = msg.from;
        }

        /*
         * Even though every response, individually, will honor the limit, it is possible that we will, after the merge,
         * have more rows than the client requested. To make sure that we still conform to the original limit,
         * we apply a top-level post-reconciliation counter to the merged partition iterator.
         *
         * Short read protection logic (ShortReadRowsProtection.moreContents()) relies on this counter to be applied
         * to the current partition to work. For this reason we have to apply the counter transformation before
         * empty partition discard logic kicks in - for it will eagerly consume the iterator.
         *
         * That's why the order here is: 1) merge; 2) filter rows; 3) count; 4) discard empty partitions
         *
         * See CASSANDRA-13747 for more details.
         */

        DataLimits.Counter mergedResultCounter =
        command.limits().newCounter(command.nowInSec(), true, command.selectsFullPartition(), enforceStrictLiveness);

        UnfilteredPartitionIterator merged = mergeWithShortReadProtection(iters, sources, mergedResultCounter);
        FilteredPartitions filtered =
        FilteredPartitions.filter(merged, new Filter(command.nowInSec(), command.metadata().enforceStrictLiveness()));
        PartitionIterator counted = Transformation.apply(filtered, mergedResultCounter);
        return Transformation.apply(counted, new EmptyPartitionsDiscarder());
    }

    private UnfilteredPartitionIterator mergeWithShortReadProtection(List<UnfilteredPartitionIterator> results,
                                                                     InetAddress[] sources,
                                                                     DataLimits.Counter mergedResultCounter)
    {
        // If we have only one results, there is no read repair to do and we can't get short reads
        if (results.size() == 1)
            return results.get(0);

        /*
         * So-called short reads stems from nodes returning only a subset of the results they have due to the limit,
         * but that subset not being enough post-reconciliation. So if we don't have a limit, don't bother.
         */
        if (!command.limits().isUnlimited())
            for (int i = 0; i < results.size(); i++)
                results.set(i, extendWithShortReadProtection(results.get(i), sources[i], mergedResultCounter));

        return UnfilteredPartitionIterators.merge(results, command.nowInSec(), readRepair.getMergeListener(sources));
    }

    private UnfilteredPartitionIterator extendWithShortReadProtection(UnfilteredPartitionIterator partitions,
                                                                      InetAddress source,
                                                                      DataLimits.Counter mergedResultCounter)
    {
        DataLimits.Counter singleResultCounter =
        command.limits().newCounter(command.nowInSec(), false, command.selectsFullPartition(), enforceStrictLiveness).onlyCount();

        ShortReadPartitionsProtection protection =
        new ShortReadPartitionsProtection(source, singleResultCounter, mergedResultCounter, queryStartNanoTime);

        /*
         * The order of extention and transformations is important here. Extending with more partitions has to happen
         * first due to the way BaseIterator.hasMoreContents() works: only transformations applied after extension will
         * be called on the first partition of the extended iterator.
         *
         * Additionally, we want singleResultCounter to be applied after SRPP, so that its applyToPartition() method will
         * be called last, after the extension done by SRRP.applyToPartition() call. That way we preserve the same order
         * when it comes to calling SRRP.moreContents() and applyToRow() callbacks.
         *
         * See ShortReadPartitionsProtection.applyToPartition() for more details.
         */

        // extend with moreContents() only if it's a range read command with no partition key specified
        if (!command.isLimitedToOnePartition())
            partitions = MorePartitions.extend(partitions, protection);     // register SRPP.moreContents()

        partitions = Transformation.apply(partitions, protection);          // register SRPP.applyToPartition()
        partitions = Transformation.apply(partitions, singleResultCounter); // register the per-source counter

        return partitions;
    }

    public void evaluateAllResponses()
    {
        // We need to fully consume the results to trigger read repairs if appropriate
        try (PartitionIterator iterator = resolve())
        {
            PartitionIterators.consume(iterator);
        }
    }

    public void evaluateAllResponses(TraceState traceState)
    {
        evaluateAllResponses();
    }

    /*
         * We have a potential short read if the result from a given node contains the requested number of rows
         * (i.e. it has stopped returning results due to the limit), but some of them haven't
         * made it into the final post-reconciliation result due to other nodes' row, range, and/or partition tombstones.
         *
         * If that is the case, then that node may have more rows that we should fetch, as otherwise we could
         * ultimately return fewer rows than required. Also, those additional rows may contain tombstones which
         * which we also need to fetch as they may shadow rows or partitions from other replicas' results, which we would
         * otherwise return incorrectly.
         */
    private class ShortReadPartitionsProtection extends Transformation<UnfilteredRowIterator> implements MorePartitions<UnfilteredPartitionIterator>
    {
        private final InetAddress source;

        private final DataLimits.Counter singleResultCounter; // unmerged per-source counter
        private final DataLimits.Counter mergedResultCounter; // merged end-result counter

        private DecoratedKey lastPartitionKey; // key of the last observed partition

        private boolean partitionsFetched; // whether we've seen any new partitions since iteration start or last moreContents() call

        private final long queryStartNanoTime;

        private ShortReadPartitionsProtection(InetAddress source,
                                              DataLimits.Counter singleResultCounter,
                                              DataLimits.Counter mergedResultCounter,
                                              long queryStartNanoTime)
        {
            this.source = source;
            this.singleResultCounter = singleResultCounter;
            this.mergedResultCounter = mergedResultCounter;
            this.queryStartNanoTime = queryStartNanoTime;
        }

        @Override
        public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            partitionsFetched = true;

            lastPartitionKey = partition.partitionKey();

            /*
             * Extend for moreContents() then apply protection to track lastClustering by applyToRow().
             *
             * If we don't apply the transformation *after* extending the partition with MoreRows,
             * applyToRow() method of protection will not be called on the first row of the new extension iterator.
             */
            ShortReadRowsProtection protection = new ShortReadRowsProtection(partition.metadata(), partition.partitionKey());
            return Transformation.apply(MoreRows.extend(partition, protection), protection);
        }

        /*
         * We only get here once all the rows and partitions in this iterator have been iterated over, and so
         * if the node had returned the requested number of rows but we still get here, then some results were
         * skipped during reconciliation.
         */
        public UnfilteredPartitionIterator moreContents()
        {
            // never try to request additional partitions from replicas if our reconciled partitions are already filled to the limit
            assert !mergedResultCounter.isDone();

            // we do not apply short read protection when we have no limits at all
            assert !command.limits().isUnlimited();

            /*
             * If this is a single partition read command or an (indexed) partition range read command with
             * a partition key specified, then we can't and shouldn't try fetch more partitions.
             */
            assert !command.isLimitedToOnePartition();

            /*
             * If the returned result doesn't have enough rows/partitions to satisfy even the original limit, don't ask for more.
             *
             * Can only take the short cut if there is no per partition limit set. Otherwise it's possible to hit false
             * positives due to some rows being uncounted for in certain scenarios (see CASSANDRA-13911).
             */
            if (!singleResultCounter.isDone() && command.limits().perPartitionCount() == DataLimits.NO_LIMIT)
                return null;

            /*
             * Either we had an empty iterator as the initial response, or our moreContents() call got us an empty iterator.
             * There is no point to ask the replica for more rows - it has no more in the requested range.
             */
            if (!partitionsFetched)
                return null;
            partitionsFetched = false;

            /*
             * We are going to fetch one partition at a time for thrift and potentially more for CQL.
             * The row limit will either be set to the per partition limit - if the command has no total row limit set, or
             * the total # of rows remaining - if it has some. If we don't grab enough rows in some of the partitions,
             * then future ShortReadRowsProtection.moreContents() calls will fetch the missing ones.
             */
            int toQuery = command.limits().count() != DataLimits.NO_LIMIT
                        ? command.limits().count() - counted(mergedResultCounter)
                        : command.limits().perPartitionCount();

            ColumnFamilyStore.metricsFor(command.metadata().id).shortReadProtectionRequests.mark();
            Tracing.trace("Requesting {} extra rows from {} for short read protection", toQuery, source);

            PartitionRangeReadCommand cmd = makeFetchAdditionalPartitionReadCommand(toQuery);
            return executeReadCommand(cmd);
        }

        // Counts the number of rows for regular queries and the number of groups for GROUP BY queries
        private int counted(Counter counter)
        {
            return command.limits().isGroupByLimit()
                 ? counter.rowCounted()
                 : counter.counted();
        }

        private PartitionRangeReadCommand makeFetchAdditionalPartitionReadCommand(int toQuery)
        {
            PartitionRangeReadCommand cmd = (PartitionRangeReadCommand) command;

            DataLimits newLimits = cmd.limits().forShortReadRetry(toQuery);

            AbstractBounds<PartitionPosition> bounds = cmd.dataRange().keyRange();
            AbstractBounds<PartitionPosition> newBounds = bounds.inclusiveRight()
                                                        ? new Range<>(lastPartitionKey, bounds.right)
                                                        : new ExcludingBounds<>(lastPartitionKey, bounds.right);
            DataRange newDataRange = cmd.dataRange().forSubRange(newBounds);

            return cmd.withUpdatedLimitsAndDataRange(newLimits, newDataRange);
        }

        private class ShortReadRowsProtection extends Transformation implements MoreRows<UnfilteredRowIterator>
        {
            private final TableMetadata metadata;
            private final DecoratedKey partitionKey;

            private Clustering lastClustering; // clustering of the last observed row

            private int lastCounted = 0; // last seen recorded # before attempting to fetch more rows
            private int lastFetched = 0; // # rows returned by last attempt to get more (or by the original read command)
            private int lastQueried = 0; // # extra rows requested from the replica last time

            private ShortReadRowsProtection(TableMetadata metadata, DecoratedKey partitionKey)
            {
                this.metadata = metadata;
                this.partitionKey = partitionKey;
            }

            @Override
            public Row applyToRow(Row row)
            {
                lastClustering = row.clustering();
                return row;
            }

            /*
             * We only get here once all the rows in this iterator have been iterated over, and so if the node
             * had returned the requested number of rows but we still get here, then some results were skipped
             * during reconciliation.
             */
            public UnfilteredRowIterator moreContents()
            {
                // never try to request additional rows from replicas if our reconciled partition is already filled to the limit
                assert !mergedResultCounter.isDoneForPartition();

                // we do not apply short read protection when we have no limits at all
                assert !command.limits().isUnlimited();

                /*
                 * If the returned partition doesn't have enough rows to satisfy even the original limit, don't ask for more.
                 *
                 * Can only take the short cut if there is no per partition limit set. Otherwise it's possible to hit false
                 * positives due to some rows being uncounted for in certain scenarios (see CASSANDRA-13911).
                 */
                if (!singleResultCounter.isDoneForPartition() && command.limits().perPartitionCount() == DataLimits.NO_LIMIT)
                    return null;

                /*
                 * If the replica has no live rows in the partition, don't try to fetch more.
                 *
                 * Note that the previous branch [if (!singleResultCounter.isDoneForPartition()) return null] doesn't
                 * always cover this scenario:
                 * isDoneForPartition() is defined as [isDone() || rowInCurrentPartition >= perPartitionLimit],
                 * and will return true if isDone() returns true, even if there are 0 rows counted in the current partition.
                 *
                 * This can happen with a range read if after 1+ rounds of short read protection requests we managed to fetch
                 * enough extra rows for other partitions to satisfy the singleResultCounter's total row limit, but only
                 * have tombstones in the current partition.
                 *
                 * One other way we can hit this condition is when the partition only has a live static row and no regular
                 * rows. In that scenario the counter will remain at 0 until the partition is closed - which happens after
                 * the moreContents() call.
                 */
                if (countedInCurrentPartition(singleResultCounter) == 0)
                    return null;

                /*
                 * This is a table with no clustering columns, and has at most one row per partition - with EMPTY clustering.
                 * We already have the row, so there is no point in asking for more from the partition.
                 */
                if (Clustering.EMPTY == lastClustering)
                    return null;

                lastFetched = countedInCurrentPartition(singleResultCounter) - lastCounted;
                lastCounted = countedInCurrentPartition(singleResultCounter);

                // getting back fewer rows than we asked for means the partition on the replica has been fully consumed
                if (lastQueried > 0 && lastFetched < lastQueried)
                    return null;

                /*
                 * At this point we know that:
                 *     1. the replica returned [repeatedly?] as many rows as we asked for and potentially has more
                 *        rows in the partition
                 *     2. at least one of those returned rows was shadowed by a tombstone returned from another
                 *        replica
                 *     3. we haven't satisfied the client's limits yet, and should attempt to query for more rows to
                 *        avoid a short read
                 *
                 * In the ideal scenario, we would get exactly min(a, b) or fewer rows from the next request, where a and b
                 * are defined as follows:
                 *     [a] limits.count() - mergedResultCounter.counted()
                 *     [b] limits.perPartitionCount() - mergedResultCounter.countedInCurrentPartition()
                 *
                 * It would be naive to query for exactly that many rows, as it's possible and not unlikely
                 * that some of the returned rows would also be shadowed by tombstones from other hosts.
                 *
                 * Note: we don't know, nor do we care, how many rows from the replica made it into the reconciled result;
                 * we can only tell how many in total we queried for, and that [0, mrc.countedInCurrentPartition()) made it.
                 *
                 * In general, our goal should be to minimise the number of extra requests - *not* to minimise the number
                 * of rows fetched: there is a high transactional cost for every individual request, but a relatively low
                 * marginal cost for each extra row requested.
                 *
                 * As such it's better to overfetch than to underfetch extra rows from a host; but at the same
                 * time we want to respect paging limits and not blow up spectacularly.
                 *
                 * Note: it's ok to retrieve more rows that necessary since singleResultCounter is not stopping and only
                 * counts.
                 *
                 * With that in mind, we'll just request the minimum of (count(), perPartitionCount()) limits.
                 *
                 * See CASSANDRA-13794 for more details.
                 */
                lastQueried = Math.min(command.limits().count(), command.limits().perPartitionCount());

                ColumnFamilyStore.metricsFor(metadata.id).shortReadProtectionRequests.mark();
                Tracing.trace("Requesting {} extra rows from {} for short read protection", lastQueried, source);

                SinglePartitionReadCommand cmd = makeFetchAdditionalRowsReadCommand(lastQueried);
                return UnfilteredPartitionIterators.getOnlyElement(executeReadCommand(cmd), cmd);
            }

            // Counts the number of rows for regular queries and the number of groups for GROUP BY queries
            private int countedInCurrentPartition(Counter counter)
            {
                return command.limits().isGroupByLimit()
                     ? counter.rowCountedInCurrentPartition()
                     : counter.countedInCurrentPartition();
            }

            private SinglePartitionReadCommand makeFetchAdditionalRowsReadCommand(int toQuery)
            {
                ClusteringIndexFilter filter = command.clusteringIndexFilter(partitionKey);
                if (null != lastClustering)
                    filter = filter.forPaging(metadata.comparator, lastClustering, false);

                return SinglePartitionReadCommand.create(command.metadata(),
                                                         command.nowInSec(),
                                                         command.columnFilter(),
                                                         command.rowFilter(),
                                                         command.limits().forShortReadRetry(toQuery),
                                                         partitionKey,
                                                         filter,
                                                         command.indexMetadata());
            }
        }

        private UnfilteredPartitionIterator executeReadCommand(ReadCommand cmd)
        {
            DataResolver resolver = new DataResolver(keyspace, cmd, ConsistencyLevel.ONE, 1, queryStartNanoTime, readRepair);
            ReadCallback handler = new ReadCallback(resolver, ConsistencyLevel.ONE, cmd, Collections.singletonList(source), queryStartNanoTime, readRepair);

            if (StorageProxy.canDoLocalRequest(source))
                StageManager.getStage(Stage.READ).maybeExecuteImmediately(new StorageProxy.LocalReadRunnable(cmd, handler));
            else
                MessagingService.instance().sendRRWithFailure(cmd.createMessage(), source, handler);

            // We don't call handler.get() because we want to preserve tombstones since we're still in the middle of merging node results.
            handler.awaitResults();
            assert resolver.responses.size() == 1;
            return resolver.responses.get(0).payload.makeIterator(command);
        }
    }
}

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

import java.util.function.LongPredicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PurgeFunction;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.StoppingTransformation;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexNotAvailableException;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

public abstract class AbstractReadExecutable implements ReadExecutable.Local
{
    private static final int TEST_ITERATION_DELAY_MILLIS = Integer.parseInt(System.getProperty("cassandra.test.read_iteration_delay_ms", "0"));
    private static final Logger logger = LoggerFactory.getLogger(AbstractReadExecutable.class);

    private final ReadCommand command;
    protected final ColumnFamilyStore cfs;

    public AbstractReadExecutable(ReadCommand command)
    {
        this.command = command;
        this.cfs = Keyspace.openAndGetStore(command.metadata());
    }

    protected abstract int oldestUnrepairedTombstone();
    protected abstract void recordLatency(TableMetrics metric, long latencyNanos);
    protected abstract UnfilteredPartitionIterator queryStorage(ReadContext context);

    @Override
    public PartitionIterator executeInternal(ReadContext context)
    {
        return UnfilteredPartitionIterators.filter(executeLocally(context), command.nowInSec());
    }

    /**
     * Executes this command on the local host.
     *
     * @param context the execution controller spanning this command
     *
     * @return an iterator over the result of executing this command locally.
     */
    @SuppressWarnings("resource") // The result iterator is closed upon exceptions (we know it's fine to potentially not close the intermediary
    // iterators created inside the try as long as we do close the original resultIterator), or by closing the result.
    @Override
    public UnfilteredPartitionIterator executeLocally(ReadContext context)
    {
        long startTimeNanos = System.nanoTime();

        Index index = getIndex();

        Index.Searcher searcher = null;
        if (index != null)
        {
            if (!cfs.indexManager.isIndexQueryable(index))
                throw new IndexNotAvailableException(index);

            searcher = index.searcherFor(command);
            Tracing.trace("Executing read on {}.{} using index {}", cfs.metadata.keyspace, cfs.metadata.name, index.getIndexMetadata().name);
        }

        UnfilteredPartitionIterator resultIterator = searcher == null
                                                     ? queryStorage(context)
                                                     : searcher.search(context);

        try
        {
            resultIterator = withStateTracking(resultIterator);
            resultIterator = withMetricsRecording(withoutPurgeableTombstones(resultIterator, cfs), cfs.metric, startTimeNanos);

            // If we've used a 2ndary index, we know the result already satisfy the primary expression used, so
            // no point in checking it again.
            RowFilter updatedFilter = searcher == null
                                      ? command.rowFilter()
                                      : index.getPostIndexQueryFilter(command.rowFilter());

            // TODO: We'll currently do filtering by the rowFilter here because it's convenient. However,
            // we'll probably want to optimize by pushing it down the layer (like for dropped columns) as it
            // would be more efficient (the sooner we discard stuff we know we don't care, the less useless
            // processing we do on it).
            return command.limits().filter(updatedFilter.filter(resultIterator, command.nowInSec()),
                                           command.nowInSec(),
                                           command.selectsFullPartition());
        }
        catch (RuntimeException | Error e)
        {
            resultIterator.close();
            throw e;
        }
    }

    public Index getIndex()
    {
        return null != command.index()
               ? cfs.indexManager.getIndex(command.index())
               : null;
    }

    /**
     * Wraps the provided iterator so that metrics on what is scanned by the command are recorded.
     * This also log warning/trow TombstoneOverwhelmingException if appropriate.
     */
    private UnfilteredPartitionIterator withMetricsRecording(UnfilteredPartitionIterator iter, final TableMetrics metric, final long startTimeNanos)
    {
        class MetricRecording extends Transformation<UnfilteredRowIterator>
        {
            private final int failureThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
            private final int warningThreshold = DatabaseDescriptor.getTombstoneWarnThreshold();

            private final boolean respectTombstoneThresholds = !SchemaConstants.isLocalSystemKeyspace(command.metadata().keyspace);
            private final boolean enforceStrictLiveness = command.metadata().enforceStrictLiveness();

            private int liveRows = 0;
            private int tombstones = 0;

            private DecoratedKey currentKey;

            @Override
            public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator iter)
            {
                currentKey = iter.partitionKey();
                return Transformation.apply(iter, this);
            }

            @Override
            public Row applyToStatic(Row row)
            {
                return applyToRow(row);
            }

            @Override
            public Row applyToRow(Row row)
            {
                boolean hasTombstones = false;
                for (Cell cell : row.cells())
                {
                    if (!cell.isLive(command.nowInSec()))
                    {
                        countTombstone(row.clustering());
                        hasTombstones = true; // allows to avoid counting an extra tombstone if the whole row expired
                    }
                }

                if (row.hasLiveData(command.nowInSec(), enforceStrictLiveness))
                    ++liveRows;
                else if (!row.primaryKeyLivenessInfo().isLive(command.nowInSec())
                         && row.hasDeletion(command.nowInSec())
                         && !hasTombstones)
                {
                    // We're counting primary key deletions only here.
                    countTombstone(row.clustering());
                }

                return row;
            }

            @Override
            public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
            {
                countTombstone(marker.clustering());
                return marker;
            }

            private void countTombstone(ClusteringPrefix clustering)
            {
                ++tombstones;
                if (tombstones > failureThreshold && respectTombstoneThresholds)
                {
                    String query = command.toCQLString();
                    Tracing.trace("Scanned over {} tombstones for query {}; query aborted (see tombstone_failure_threshold)", failureThreshold, query);
                    metric.tombstoneFailures.inc();
                    throw new TombstoneOverwhelmingException(tombstones, query, command.metadata(), currentKey, clustering);
                }
            }

            @Override
            public void onClose()
            {
                recordLatency(metric, System.nanoTime() - startTimeNanos);

                metric.tombstoneScannedHistogram.update(tombstones);
                metric.liveScannedHistogram.update(liveRows);

                boolean warnTombstones = tombstones > warningThreshold && respectTombstoneThresholds;
                if (warnTombstones)
                {
                    String msg = String.format(
                    "Read %d live rows and %d tombstone cells for query %1.512s (see tombstone_warn_threshold)",
                    liveRows, tombstones, command.toCQLString());
                    ClientWarn.instance.warn(msg);
                    if (tombstones < failureThreshold)
                    {
                        metric.tombstoneWarnings.inc();
                    }

                    logger.warn(msg);
                }

                Tracing.trace("Read {} live rows and {} tombstone cells{}",
                              liveRows, tombstones,
                              (warnTombstones ? " (see tombstone_warn_threshold)" : ""));
            }
        };

        return Transformation.apply(iter, new MetricRecording());
    }

    protected class CheckForAbort extends StoppingTransformation<UnfilteredRowIterator>
    {
        long lastChecked = 0;

        protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            if (maybeAbort())
            {
                partition.close();
                return null;
            }

            return Transformation.apply(partition, this);
        }

        protected Row applyToRow(Row row)
        {
            if (TEST_ITERATION_DELAY_MILLIS > 0)
                maybeDelayForTesting();

            return maybeAbort() ? null : row;
        }

        private boolean maybeAbort()
        {
            /**
             * The value returned by ApproximateTime.currentTimeMillis() is updated only every
             * {@link ApproximateTime.CHECK_INTERVAL_MS}, by default 10 millis. Since MonitorableImpl
             * relies on ApproximateTime, we don't need to check unless the approximate time has elapsed.
             */
            if (lastChecked == ApproximateTime.currentTimeMillis())
                return false;

            lastChecked = ApproximateTime.currentTimeMillis();

            if (command.isAborted())
            {
                stop();
                return true;
            }

            return false;
        }

        private void maybeDelayForTesting()
        {
            if (!command.metadata().keyspace.startsWith("system"))
                FBUtilities.sleepQuietly(TEST_ITERATION_DELAY_MILLIS);
        }
    }

    protected UnfilteredPartitionIterator withStateTracking(UnfilteredPartitionIterator iter)
    {
        return Transformation.apply(iter, new CheckForAbort());
    }


    // Skip purgeable tombstones. We do this because it's safe to do (post-merge of the memtable and sstable at least), it
    // can save us some bandwith, and avoid making us throw a TombstoneOverwhelmingException for purgeable tombstones (which
    // are to some extend an artefact of compaction lagging behind and hence counting them is somewhat unintuitive).
    protected UnfilteredPartitionIterator withoutPurgeableTombstones(UnfilteredPartitionIterator iterator, ColumnFamilyStore cfs)
    {
        class WithoutPurgeableTombstones extends PurgeFunction
        {
            public WithoutPurgeableTombstones()
            {
                super(command.nowInSec(), cfs.gcBefore(command.nowInSec()), oldestUnrepairedTombstone(),
                      cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones(),
                      iterator.metadata().enforceStrictLiveness());
            }

            protected LongPredicate getPurgeEvaluator()
            {
                return time -> true;
            }
        }
        return Transformation.apply(iterator, new WithoutPurgeableTombstones());
    }
}

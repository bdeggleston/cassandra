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

package org.apache.cassandra.service.reads.repair;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.AsyncRepairCallback;
import org.apache.cassandra.service.reads.DataResolver;
import org.apache.cassandra.service.reads.DigestResolver;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.service.reads.ResponseResolver;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;

/**
 * 'Classic' read repair. Doesn't allow the client read to return until
 *  updates have been written to nodes needing correction.
 */
public class BlockingReadRepair implements ReadRepair
{
    private static final Logger logger = LoggerFactory.getLogger(BlockingReadRepair.class);

    private final Keyspace keyspace;
    private final ColumnFamilyStore cfs;
    private final ReadCommand command;
    private final List<InetAddressAndPort> endpoints;
    private final int blockFor;
    private final long queryStartNanoTime;
    private final ConsistencyLevel consistency;

    private final Queue<BlockingPartitionRepair> repairs = new ConcurrentLinkedQueue<>();

    private volatile DigestRepair digestRepair = null;

    private static class DigestRepair
    {
        private final DataResolver resolver;
        private final ReadCallback callback;
        private final List<InetAddressAndPort> contactedEndpoints;
        private final Consumer<PartitionIterator> resultConsumer;

        public DigestRepair(DataResolver resolver, ReadCallback callback, List<InetAddressAndPort> contactedEndpoints, Consumer<PartitionIterator> resultConsumer)
        {
            this.resolver = resolver;
            this.callback = callback;
            this.contactedEndpoints = contactedEndpoints;
            this.resultConsumer = resultConsumer;
        }
    }

    public BlockingReadRepair(ReadCommand command,
                              List<InetAddressAndPort> endpoints,
                              int blockFor, long queryStartNanoTime,
                              ConsistencyLevel consistency)
    {
        TableMetadata metadata = command.metadata();
        this.keyspace = Keyspace.open(command.metadata().keyspace);
        this.cfs = keyspace.getColumnFamilyStore(metadata.id);
        this.command = command;
        this.endpoints = endpoints;
        this.blockFor = blockFor;
        this.queryStartNanoTime = queryStartNanoTime;
        this.consistency = consistency;
    }

    private boolean shouldSpeculate()
    {
        ConsistencyLevel speculativeCL = consistency.isDatacenterLocal() ? ConsistencyLevel.LOCAL_QUORUM : ConsistencyLevel.QUORUM;
        return consistency != ConsistencyLevel.EACH_QUORUM
               && consistency.satisfies(speculativeCL, keyspace)
               && cfs.sampleLatencyNanos <= TimeUnit.MILLISECONDS.toNanos(command.getTimeout());
    }

    public UnfilteredPartitionIterators.MergeListener getMergeListener(InetAddressAndPort[] endpoints)
    {
        return new PartitionIteratorMergeListener(endpoints, command, this);
    }

    @Override
    public void repairPartition(DecoratedKey key, Map<InetAddressAndPort, PartitionUpdate> updates, InetAddressAndPort[] destinations)
    {
        BlockingPartitionRepair handler = new BlockingPartitionRepair(keyspace, key, consistency, updates, consistency.blockFor(keyspace), destinations);
        handler.sendInitialRepairs();
        repairs.add(handler);
    }

    @Override
    public void maybeSendAdditionalRepairs()
    {
        if (!shouldSpeculate())
            return;

        for (BlockingPartitionRepair repairHandler: repairs)
        {
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.metadata().id);
            repairHandler.maybeSendAdditionalRepairs(cfs.sampleLatencyNanos, TimeUnit.NANOSECONDS);
        }
    }

    @Override
    public void awaitRepairs()
    {
        boolean timedOut = false;
        for (BlockingPartitionRepair repairHandler: repairs)
        {
            if (!repairHandler.awaitRepairs(DatabaseDescriptor.getWriteRpcTimeout(), TimeUnit.MILLISECONDS))
            {
                timedOut = true;
            }
        }
        if (timedOut)
        {
            // We got all responses, but timed out while repairing
            int blockFor = consistency.blockFor(keyspace);
            if (Tracing.isTracing())
                Tracing.trace("Timed out while read-repairing after receiving all {} data and digest responses", blockFor);
            else
                logger.debug("Timeout while read-repairing after receiving all {} data and digest responses", blockFor);

            throw new ReadTimeoutException(consistency, blockFor - 1, blockFor, true);
        }

    }

    @Override
    public void startForegroundRepair(DigestResolver digestResolver, List<InetAddressAndPort> allEndpoints, List<InetAddressAndPort> contactedEndpoints, Consumer<PartitionIterator> resultConsumer)
    {
        ReadRepairMetrics.repairedBlocking.mark();

        // Do a full data read to resolve the correct response (and repair node that need be)
        Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
        DataResolver resolver = new DataResolver(keyspace, command, consistency, allEndpoints.size(), queryStartNanoTime, this);
        ReadCallback callback = new ReadCallback(resolver, consistency, blockFor, command, keyspace, allEndpoints, queryStartNanoTime, this);

        digestRepair = new DigestRepair(resolver, callback, contactedEndpoints, resultConsumer);

        for (InetAddressAndPort endpoint : contactedEndpoints)
        {
            Tracing.trace("Enqueuing full data read to {}", endpoint);
            MessagingService.instance().sendRRWithFailure(command.createMessage(), endpoint, callback);
        }
    }

    @Override
    public void maybeSendAdditionalDataRequests()
    {
        DigestRepair repair = digestRepair;
        if (repair == null || !shouldSpeculate())
            return;

        if (!(command instanceof SinglePartitionReadCommand))
            return;

        if (repair.callback.await(cfs.sampleLatencyNanos, TimeUnit.NANOSECONDS))
            return;

        DecoratedKey key = ((SinglePartitionReadCommand) command).partitionKey();

        Set<InetAddressAndPort> contacted = Sets.newHashSet(repair.contactedEndpoints);
        Iterable<InetAddressAndPort> candidates = ReadRepairs.getCandidateEndpoints(keyspace, key, consistency);
        candidates = Iterables.filter(candidates, e -> !contacted.contains(e));

        if (Iterables.isEmpty(candidates))
            return;

        ReadRepairMetrics.speculatedDataRequest.mark();

        for (InetAddressAndPort endpoint: Iterables.filter(candidates, e -> !contacted.contains(e)))
        {
            Tracing.trace("Enqueuing speculative full data read to {}", endpoint);
            MessagingService.instance().sendRR(command.createMessage(), endpoint, repair.callback);
        }
    }

    @Override
    public void awaitForegroundRepairFinish() throws ReadTimeoutException
    {
        if (digestRepair != null)
        {
            digestRepair.callback.awaitResults();
            digestRepair.resultConsumer.accept(digestRepair.resolver.resolve());
        }
    }

    @Override
    public void maybeStartBackgroundRepair(ResponseResolver resolver)
    {
        TraceState traceState = Tracing.instance.get();
        if (traceState != null)
            traceState.trace("Initiating read-repair");
        StageManager.getStage(Stage.READ_REPAIR).execute(() -> resolver.evaluateAllResponses(traceState));
    }

    @Override
    public void backgroundDigestRepair(TraceState traceState)
    {
        if (traceState != null)
            traceState.trace("Digest mismatch");
        if (logger.isDebugEnabled())
            logger.debug("Digest mismatch");

        ReadRepairMetrics.repairedBackground.mark();

        Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
        final DataResolver repairResolver = new DataResolver(keyspace, command, consistency, endpoints.size(), queryStartNanoTime, this);
        AsyncRepairCallback repairHandler = new AsyncRepairCallback(repairResolver, endpoints.size());

        for (InetAddressAndPort endpoint : endpoints)
            MessagingService.instance().sendRR(command.createMessage(), endpoint, repairHandler);
    }
}

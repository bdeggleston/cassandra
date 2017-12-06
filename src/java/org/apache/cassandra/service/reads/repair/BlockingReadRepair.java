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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.AsyncRepairCallback;
import org.apache.cassandra.service.DataResolver;
import org.apache.cassandra.service.DigestResolver;
import org.apache.cassandra.service.ReadCallback;
import org.apache.cassandra.service.ResponseResolver;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.Clock;

import static org.apache.cassandra.db.ConsistencyLevel.*;

/**
 * 'Classic' read repair. Doesn't allow the client read to return until
 *  updates have been written to nodes needing correction.
 */
public class BlockingReadRepair implements ReadRepair
{
    private static final Logger logger = LoggerFactory.getLogger(BlockingReadRepair.class);

    private final ReadCommand command;
    private final List<InetAddress> endpoints;
    private final long queryStartNanoTime;
    private final ConsistencyLevel consistency;

    private final List<PartitionRepair> repairs = new ArrayList<>();

    private volatile DigestRepair digestRepair = null;
    private final Keyspace keyspace;
    private final ColumnFamilyStore cfs;
    private final boolean stronglyConsistent;

    private static class DigestRepair
    {
        final DataResolver dataResolver;
        final ReadCallback readCallback;
        final List<InetAddress> initialContacts;
        final Consumer<PartitionIterator> resultConsumer;

        public DigestRepair(DataResolver dataResolver, ReadCallback readCallback, List<InetAddress> initialContacts, Consumer<PartitionIterator> resultConsumer)
        {
            this.dataResolver = dataResolver;
            this.readCallback = readCallback;
            this.initialContacts = initialContacts;
            this.resultConsumer = resultConsumer;
        }
    }

    /**
     * If true, we should block on read repair responses to guarantee monotonic reads
     * @return
     */
    private static boolean canSatisfyQuorumFor(Keyspace keyspace, ConsistencyLevel consistency)
    {
        switch (consistency)
        {
            case QUORUM:
            case ALL:
            case LOCAL_QUORUM:
            case EACH_QUORUM:
            case SERIAL:
            case LOCAL_SERIAL:
                return true;
            default:
                int blockFor = consistency.blockFor(keyspace);
                return blockFor >= (consistency.isDatacenterLocal()
                                    ? localQuorumFor(keyspace)
                                    : quorumFor(keyspace));
        }
    }

    public BlockingReadRepair(ReadCommand command,
                              List<InetAddress> endpoints,
                              long queryStartNanoTime,
                              ConsistencyLevel consistency)
    {
        this.command = command;
        this.endpoints = endpoints;
        this.queryStartNanoTime = queryStartNanoTime;
        this.consistency = consistency;

        this.keyspace = Keyspace.open(command.metadata().keyspace);
        this.cfs = keyspace.getColumnFamilyStore(command.metadata().id);
        this.stronglyConsistent = canSatisfyQuorumFor(keyspace, consistency);
    }

    public UnfilteredPartitionIterators.MergeListener getMergeListener(InetAddress[] endpoints)
    {
        return new PartitionIteratorMergeListener(endpoints, command, this);
    }

    public class PartitionRepair extends AbstractFuture<Object> implements IAsyncCallback<Object>
    {
        private final Map<InetAddress, Optional<PartitionUpdate>> repairs;
        private final Set<InetAddress> acked = new HashSet<>();
        private int blockFor;
        private final long sent = Clock.instance.nanoTime();

        public PartitionRepair(Map<InetAddress, Optional<PartitionUpdate>> repairs)
        {
            this.repairs = repairs;

            int blockFor = 0;
            for (Optional<PartitionUpdate> repair: repairs.values())
            {
                if (repair.isPresent())
                {
                    blockFor++;
                }

            }
            this.blockFor = blockFor;
        }

        public void sendInitialRepairs()
        {
            for (Map.Entry<InetAddress, Optional<PartitionUpdate>> entry: repairs.entrySet())
            {
                if (entry.getValue().isPresent())
                {
                    sendRepair(entry.getKey(), entry.getValue().get(), this);
                }
            }
        }

        synchronized void ack(InetAddress endpoint)
        {
            acked.add(endpoint);
            if (acked.size() == blockFor)
            {
                set(null);
            }
        }

        public void response(MessageIn<Object> msg)
        {
            ack(msg.from);
        }

        public boolean isLatencyForSnitch()
        {
            return false;
        }

        private boolean awaitCompletion(long timeout, TimeUnit timeoutUnit)
        {
            long elapsed = Clock.instance.nanoTime() - sent;
            long remaining = Math.max(0, timeoutUnit.toNanos(timeout) - elapsed);

            try
            {
                get(remaining, TimeUnit.NANOSECONDS);
                return true;
            }
            catch (InterruptedException | ExecutionException e)
            {
                throw new AssertionError(e);
            }
            catch (TimeoutException e)
            {
                return false;
            }
        }

        synchronized PartitionUpdate getUnackedRepair()
        {
            List<PartitionUpdate> updates = null;
            for (Map.Entry<InetAddress, Optional<PartitionUpdate>> entry: repairs.entrySet())
            {
                if (acked.contains(entry.getKey()) || !entry.getValue().isPresent())
                    continue;
                if (updates == null)
                {
                    updates = new ArrayList<>(blockFor);
                }

                updates.add(entry.getValue().get());
            }

            return (updates == null || updates.isEmpty()) ? null : PartitionUpdate.merge(updates);
        }

        void maybeSendAdditionalRepairs(long timeout, TimeUnit timeoutUnit)
        {
            if (!awaitCompletion(timeout, timeoutUnit))
            {
                PartitionUpdate update = null;
                for (InetAddress endpoint: getCandidateEndpoints())
                {
                    if (repairs.containsKey(endpoint))
                        continue;

                    if (update == null)
                    {
                        update = getUnackedRepair();
                        if (update == null)
                            return;
                    }

                    sendRepair(endpoint, update, this);
                }
            }
        }
    }

    void sendDataRequest(InetAddress endpoint, ReadCallback callback)
    {
        Tracing.trace("Enqueuing full data read to {}", endpoint);
        MessagingService.instance().sendRRWithFailure(command.createMessage(), endpoint, callback);
    }

    public void startForegroundRepair(DigestResolver digestResolver, List<InetAddress> allEndpoints, List<InetAddress> contactedEndpoints, Consumer<PartitionIterator> resultConsumer)
    {
        if (!stronglyConsistent)
        {
            // if this consistency level doesn't provide any strong consistency guarantees, there's no need to block on read repairs
            resultConsumer.accept(digestResolver.getData());
            StageManager.getStage(Stage.READ_REPAIR).execute(() -> backgroundDigestRepair(digestResolver, Tracing.instance.get()));
        }
        else
        {
            ReadRepairMetrics.repairedBlocking.mark();

            // Do a full data read to resolve the correct response (and repair node that need be)
            Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
            DataResolver resolver = new DataResolver(keyspace, command, ConsistencyLevel.ALL, allEndpoints.size(), queryStartNanoTime, this);
            ReadCallback readCallback = new ReadCallback(resolver, ConsistencyLevel.ALL, contactedEndpoints.size(), command,
                                                         keyspace, allEndpoints, queryStartNanoTime, this);

            digestRepair = new DigestRepair(resolver, readCallback, contactedEndpoints, resultConsumer);

            for (InetAddress endpoint : contactedEndpoints)
            {
                sendDataRequest(endpoint, readCallback);
            }
        }
    }

    private boolean shouldSpeculate()
    {
        return stronglyConsistent && cfs.sampleLatencyNanos <= TimeUnit.MILLISECONDS.toNanos(command.getTimeout());
    }

    void sendRepair(InetAddress endpoint, PartitionUpdate update, IAsyncCallback callback)
    {
        // use a separate verb here because we don't want these to be get the white glove hint-
        // on-timeout behavior that a "real" mutation gets
        Tracing.trace("Sending read-repair-mutation to {}", endpoint);
        Mutation mutation = new Mutation(update);
        MessageOut<Mutation> msg = mutation.createMessage(MessagingService.Verb.READ_REPAIR);
        MessagingService.instance().sendRR(msg, endpoint, callback);
    }

    public void maybeSendAdditionalDataRequests()
    {
        if (digestRepair != null && shouldSpeculate() && !digestRepair.readCallback.await(speculativeWaitTimeNanos(), TimeUnit.NANOSECONDS))
        {
            Set<InetAddress> contacted = Sets.newHashSet(digestRepair.initialContacts);
            List<InetAddress> candidates = getCandidateEndpoints();

            for (InetAddress endpoint: Iterables.filter(candidates, contacted::contains))
            {
                sendDataRequest(endpoint, digestRepair.readCallback);
            }
        }
    }

    public void awaitForegroundRepairFinish() throws ReadTimeoutException
    {
        if (digestRepair != null)
        {
            digestRepair.readCallback.awaitResults();
            digestRepair.resultConsumer.accept(digestRepair.dataResolver.resolve());
        }
    }

    long speculativeWaitTimeNanos()
    {
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.metadata().id);
        long waitTimeNanos = cfs.sampleLatencyNanos;

        // no latency information, or we're overloaded
        if (waitTimeNanos > TimeUnit.MILLISECONDS.toNanos(command.getTimeout()) || waitTimeNanos == 0)
        {
            // try to choose a default value
            waitTimeNanos = TimeUnit.MILLISECONDS.toNanos((long)(DatabaseDescriptor.getReadRpcTimeout() / 4.0));
        }
        return waitTimeNanos;
    }

    public void repairPartition(Map<InetAddress, Optional<PartitionUpdate>> updates)
    {
        PartitionRepair repair = new PartitionRepair(updates);
        repair.sendInitialRepairs();
        repairs.add(repair);
    }

    public void maybeSendAdditionalRepairs()
    {
        if (shouldSpeculate())
        {
            long speculateNanos = speculativeWaitTimeNanos();
            for (PartitionRepair repair: repairs)
            {
                repair.maybeSendAdditionalRepairs(speculateNanos, TimeUnit.NANOSECONDS);
            }
        }
    }

    public void awaitRepairDelivery()
    {
        try
        {
            Futures.allAsList(repairs).get(DatabaseDescriptor.getWriteRpcTimeout(), TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException ex)
        {
            // We got all responses, but timed out while repairing
            Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
            int blockFor = consistency.blockFor(keyspace);
            if (Tracing.isTracing())
                Tracing.trace("Timed out while read-repairing after receiving all {} data and digest responses", blockFor);
            else
                logger.debug("Timeout while read-repairing after receiving all {} data and digest responses", blockFor);

            throw new ReadTimeoutException(consistency, blockFor - 1, blockFor, true);
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void maybeStartBackgroundRepair(ResponseResolver resolver)
    {
        TraceState traceState = Tracing.instance.get();
        if (traceState != null)
            traceState.trace("Initiating read-repair");
        StageManager.getStage(Stage.READ_REPAIR).execute(() -> resolver.evaluateAllResponses(traceState));
    }

    public void backgroundDigestRepair(DigestResolver digestResolver, TraceState traceState)
    {
        if (traceState != null)
            traceState.trace("Digest mismatch");
        if (logger.isDebugEnabled())
            logger.debug("Digest mismatch");

        ReadRepairMetrics.repairedBackground.mark();

        Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
        final DataResolver repairResolver = new DataResolver(keyspace, command, consistency, endpoints.size(), queryStartNanoTime, this);
        AsyncRepairCallback repairHandler = new AsyncRepairCallback(repairResolver, endpoints.size());

        for (InetAddress endpoint : endpoints)
            MessagingService.instance().sendRR(command.createMessage(), endpoint, repairHandler);
    }

    /**
     * Returns all of the endpoints that are replicas for the given key. If the consistency level is datacenter
     * local, only the endpoints in the local dc will be returned.
     */
    static List<InetAddress> getCandidateEndpoints(Keyspace keyspace, DecoratedKey key, ConsistencyLevel consistency)
    {
        List<InetAddress> endpoints = StorageProxy.getLiveSortedEndpoints(keyspace, key);
        if (consistency.isDatacenterLocal() && keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)
        {
            endpoints = Lists.newArrayList(Iterables.filter(endpoints, ConsistencyLevel::isLocal));
        }
        return endpoints;
    }

    List<InetAddress> getCandidateEndpoints()
    {
        if (command instanceof SinglePartitionReadCommand)
        {
            SinglePartitionReadCommand sprCommand = (SinglePartitionReadCommand) command;
            return getCandidateEndpoints(keyspace, sprCommand.partitionKey(), consistency);
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }
}

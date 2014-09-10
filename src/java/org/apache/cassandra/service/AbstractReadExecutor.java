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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData.SpeculativeRetry.RetryType;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.ReadRepairDecision;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

/**
 * Sends a read request to the replicas needed to satisfy a given ConsistencyLevel.
 *
 * Optionally, may perform additional requests to provide redundancy against replica failure:
 * AlwaysSpeculatingReadExecutor will always send a request to one extra replica, while
 * SpeculatingReadExecutor will wait until it looks like the original request is in danger
 * of timing out before performing extra reads.
 */
public abstract class AbstractReadExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractReadExecutor.class);

    protected final ReadCommand command;
    protected final List<InetAddress> targetReplicas;
    protected final RowDigestResolver resolver;
    protected final ReadCallback<ReadResponse, Row> handler;
    protected final DatabaseDescriptor databaseDescriptor;
    protected final MessagingService messagingService;
    protected final Schema schema;
    protected final StageManager stageManager;
    protected final DBConfig dbConfig;

    AbstractReadExecutor(ReadCommand command, ConsistencyLevel consistencyLevel, List<InetAddress> targetReplicas,
                         DatabaseDescriptor databaseDescriptor, MessagingService messagingService, Schema schema, StageManager stageManager, KeyspaceManager keyspaceManager, DBConfig dbConfig)
    {
        this.command = command;
        this.targetReplicas = targetReplicas;
        this.databaseDescriptor = databaseDescriptor;
        this.messagingService = messagingService;
        this.schema = schema;
        this.stageManager = stageManager;
        this.dbConfig = dbConfig;
        resolver = new RowDigestResolver(command.ksName, command.key, this.dbConfig.getPartitioner());
        handler = new ReadCallback<>(resolver,
                                     consistencyLevel,
                                     command,
                                     targetReplicas,
                                     dbConfig.getLocatorConfig(),
                                     this.databaseDescriptor,
                                     this.messagingService,
                                     this.stageManager,
                                     keyspaceManager);
    }

    private boolean isLocalRequest(InetAddress replica)
    {
        return replica.equals(databaseDescriptor.getBroadcastAddress()) && StorageProxy.OPTIMIZE_LOCAL_REQUESTS;
    }

    protected void makeDataRequests(Iterable<InetAddress> endpoints)
    {
        boolean readLocal = false;
        for (InetAddress endpoint : endpoints)
        {
            if (isLocalRequest(endpoint))
            {
                readLocal = true;
            }
            else
            {
                logger.trace("reading data from {}", endpoint);
                messagingService.sendRR(command.createMessage(messagingService), endpoint, handler);
            }
        }
        if (readLocal)
        {
            logger.trace("reading data locally");
            stageManager.getStage(Stage.READ).maybeExecuteImmediately(new StorageProxy.LocalReadRunnable(command, handler));
        }
    }

    protected void makeDigestRequests(Iterable<InetAddress> endpoints)
    {
        ReadCommand digestCommand = command.copy();
        digestCommand.setDigestQuery(true);
        MessageOut<?> message = digestCommand.createMessage(messagingService);
        for (InetAddress endpoint : endpoints)
        {
            if (isLocalRequest(endpoint))
            {
                logger.trace("reading digest locally");
                stageManager.getStage(Stage.READ).execute(new StorageProxy.LocalReadRunnable(digestCommand, handler));
            }
            else
            {
                logger.trace("reading digest from {}", endpoint);
                messagingService.sendRR(message, endpoint, handler);
            }
        }
    }

    /**
     * Perform additional requests if it looks like the original will time out.  May block while it waits
     * to see if the original requests are answered first.
     */
    public abstract void maybeTryAdditionalReplicas();

    /**
     * Get the replicas involved in the [finished] request.
     *
     * @return target replicas + the extra replica, *IF* we speculated.
     */
    public abstract Collection<InetAddress> getContactedReplicas();

    /**
     * send the initial set of requests
     */
    public abstract void executeAsync();

    /**
     * wait for an answer.  Blocks until success or timeout, so it is caller's
     * responsibility to call maybeTryAdditionalReplicas first.
     */
    public Row get() throws ReadTimeoutException, DigestMismatchException
    {
        return handler.get();
    }

    /**
     * @return an executor appropriate for the configured speculative read policy
     */
    public static AbstractReadExecutor getReadExecutor(ReadCommand command, ConsistencyLevel consistencyLevel, DatabaseDescriptor databaseDescriptor, MessagingService messagingService,
                                                       Schema schema, StageManager stageManager, KeyspaceManager keyspaceManager, StorageProxy storageProxy, StorageService storageService, DBConfig dbConfig) throws UnavailableException
    {
        Keyspace keyspace = keyspaceManager.open(command.ksName);
        List<InetAddress> allReplicas = storageProxy.getLiveSortedEndpoints(keyspace, command.key);
        ReadRepairDecision repairDecision = schema.getCFMetaData(command.ksName, command.cfName).newReadRepairDecision();
        List<InetAddress> targetReplicas = consistencyLevel.filterForQuery(keyspace, allReplicas, repairDecision, databaseDescriptor.getLocalDataCenter(), databaseDescriptor.getEndpointSnitch(), databaseDescriptor.getLocalComparator());

        // Throw UAE early if we don't have enough replicas.
        consistencyLevel.assureSufficientLiveNodes(keyspace, targetReplicas, databaseDescriptor.getLocalDataCenter(), databaseDescriptor.getEndpointSnitch());

        // Fat client. Speculating read executors need access to cfs metrics and sampled latency, and fat clients
        // can't provide that. So, for now, fat clients will always use NeverSpeculatingReadExecutor.
        if (storageService.isClientMode())
            return new NeverSpeculatingReadExecutor(command, consistencyLevel, targetReplicas, databaseDescriptor, messagingService, schema, stageManager, keyspaceManager, dbConfig);

        if (repairDecision != ReadRepairDecision.NONE)
            ReadRepairMetrics.attempted.mark();

        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.cfName);
        RetryType retryType = cfs.metadata.getSpeculativeRetry().type;

        // Speculative retry is disabled *OR* there are simply no extra replicas to speculate.
        if (retryType == RetryType.NONE || consistencyLevel.blockFor(keyspace, databaseDescriptor.getLocalDataCenter()) == allReplicas.size())
            return new NeverSpeculatingReadExecutor(command, consistencyLevel, targetReplicas, databaseDescriptor, messagingService, schema, stageManager, keyspaceManager, dbConfig);

        if (targetReplicas.size() == allReplicas.size())
        {
            // CL.ALL, RRD.GLOBAL or RRD.DC_LOCAL and a single-DC.
            // We are going to contact every node anyway, so ask for 2 full data requests instead of 1, for redundancy
            // (same amount of requests in total, but we turn 1 digest request into a full blown data request).
            return new AlwaysSpeculatingReadExecutor(cfs, command, consistencyLevel, targetReplicas, databaseDescriptor, messagingService, schema, stageManager, keyspaceManager, dbConfig);
        }

        // RRD.NONE or RRD.DC_LOCAL w/ multiple DCs.
        InetAddress extraReplica = allReplicas.get(targetReplicas.size());
        // With repair decision DC_LOCAL all replicas/target replicas may be in different order, so
        // we might have to find a replacement that's not already in targetReplicas.
        if (repairDecision == ReadRepairDecision.DC_LOCAL && targetReplicas.contains(extraReplica))
        {
            for (InetAddress address : allReplicas)
            {
                if (!targetReplicas.contains(address))
                {
                    extraReplica = address;
                    break;
                }
            }
        }
        targetReplicas.add(extraReplica);

        if (retryType == RetryType.ALWAYS)
            return new AlwaysSpeculatingReadExecutor(cfs, command, consistencyLevel, targetReplicas, databaseDescriptor, messagingService, schema, stageManager, keyspaceManager, dbConfig);
        else // PERCENTILE or CUSTOM.
            return new SpeculatingReadExecutor(cfs, command, consistencyLevel, targetReplicas, databaseDescriptor, messagingService, schema, stageManager, keyspaceManager, dbConfig);
    }

    private static class NeverSpeculatingReadExecutor extends AbstractReadExecutor
    {
        public NeverSpeculatingReadExecutor(ReadCommand command, ConsistencyLevel consistencyLevel, List<InetAddress> targetReplicas,
                                            DatabaseDescriptor databaseDescriptor, MessagingService messagingService, Schema schema, StageManager stageManager, KeyspaceManager keyspaceManager, DBConfig dbConfig)
        {
            super(command, consistencyLevel, targetReplicas, databaseDescriptor, messagingService, schema, stageManager, keyspaceManager, dbConfig);
        }

        public void executeAsync()
        {
            makeDataRequests(targetReplicas.subList(0, 1));
            if (targetReplicas.size() > 1)
                makeDigestRequests(targetReplicas.subList(1, targetReplicas.size()));
        }

        public void maybeTryAdditionalReplicas()
        {
            // no-op
        }

        public Collection<InetAddress> getContactedReplicas()
        {
            return targetReplicas;
        }
    }

    private static class SpeculatingReadExecutor extends AbstractReadExecutor
    {
        private final ColumnFamilyStore cfs;
        private volatile boolean speculated = false;

        public SpeculatingReadExecutor(ColumnFamilyStore cfs,
                                       ReadCommand command,
                                       ConsistencyLevel consistencyLevel,
                                       List<InetAddress> targetReplicas,
                                       DatabaseDescriptor databaseDescriptor,
                                       MessagingService messagingService,
                                       Schema schema,
                                       StageManager stageManager,
                                       KeyspaceManager keyspaceManager,
                                       DBConfig dbConfig)
        {
            super(command, consistencyLevel, targetReplicas, databaseDescriptor, messagingService, schema, stageManager, keyspaceManager, dbConfig);
            this.cfs = cfs;
        }

        public void executeAsync()
        {
            // if CL + RR result in covering all replicas, getReadExecutor forces AlwaysSpeculating.  So we know
            // that the last replica in our list is "extra."
            List<InetAddress> initialReplicas = targetReplicas.subList(0, targetReplicas.size() - 1);

            if (handler.blockfor < initialReplicas.size())
            {
                // We're hitting additional targets for read repair.  Since our "extra" replica is the least-
                // preferred by the snitch, we do an extra data read to start with against a replica more
                // likely to reply; better to let RR fail than the entire query.
                makeDataRequests(initialReplicas.subList(0, 2));
                if (initialReplicas.size() > 2)
                    makeDigestRequests(initialReplicas.subList(2, initialReplicas.size()));
            }
            else
            {
                // not doing read repair; all replies are important, so it doesn't matter which nodes we
                // perform data reads against vs digest.
                makeDataRequests(initialReplicas.subList(0, 1));
                if (initialReplicas.size() > 1)
                    makeDigestRequests(initialReplicas.subList(1, initialReplicas.size()));
            }
        }

        public void maybeTryAdditionalReplicas()
        {
            // no latency information, or we're overloaded
            if (cfs.sampleLatencyNanos > TimeUnit.MILLISECONDS.toNanos(command.getTimeout()))
                return;

            if (!handler.await(cfs.sampleLatencyNanos, TimeUnit.NANOSECONDS))
            {
                // Could be waiting on the data, or on enough digests.
                ReadCommand retryCommand = command;
                if (resolver.getData() != null)
                {
                    retryCommand = command.copy();
                    retryCommand.setDigestQuery(true);
                }

                InetAddress extraReplica = Iterables.getLast(targetReplicas);
                logger.trace("speculating read retry on {}", extraReplica);
                messagingService.sendRR(retryCommand.createMessage(messagingService), extraReplica, handler);
                speculated = true;

                cfs.metric.speculativeRetries.inc();
            }
        }

        public Collection<InetAddress> getContactedReplicas()
        {
            return speculated
                 ? targetReplicas
                 : targetReplicas.subList(0, targetReplicas.size() - 1);
        }
    }

    private static class AlwaysSpeculatingReadExecutor extends AbstractReadExecutor
    {
        private final ColumnFamilyStore cfs;

        public AlwaysSpeculatingReadExecutor(ColumnFamilyStore cfs,
                                             ReadCommand command,
                                             ConsistencyLevel consistencyLevel,
                                             List<InetAddress> targetReplicas,
                                             DatabaseDescriptor databaseDescriptor,
                                             MessagingService messagingService,
                                             Schema schema,
                                             StageManager stageManager,
                                             KeyspaceManager keyspaceManager,
                                             DBConfig dbConfig)
        {
            super(command, consistencyLevel, targetReplicas, databaseDescriptor, messagingService, schema, stageManager, keyspaceManager, dbConfig);
            this.cfs = cfs;
        }

        public void maybeTryAdditionalReplicas()
        {
            // no-op
        }

        public Collection<InetAddress> getContactedReplicas()
        {
            return targetReplicas;
        }

        @Override
        public void executeAsync()
        {
            makeDataRequests(targetReplicas.subList(0, targetReplicas.size() > 1 ? 2 : 1));
            if (targetReplicas.size() > 2)
                makeDigestRequests(targetReplicas.subList(2, targetReplicas.size()));
            cfs.metric.speculativeRetries.inc();
        }
    }
}

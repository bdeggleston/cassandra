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

package org.apache.cassandra.service.accord;

import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import accord.impl.AbstractConfigurationService;
import accord.local.Node;
import accord.topology.Topology;
import accord.utils.Invariants;
import org.agrona.collections.Long2ObjectHashMap;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.accord.AccordKeyspace.EpochDiskState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.listeners.ChangeListener;

// TODO: listen to FailureDetector and rearrange fast path accordingly
public class AccordConfigurationService extends AbstractConfigurationService<AccordConfigurationService.EpochState, AccordConfigurationService.EpochHistory> implements ChangeListener, AccordEndpointMapper, AccordLocalSyncNotifier.Listener
{
    private final MessageDelivery messagingService;
    private final IFailureDetector failureDetector;
    private EpochDiskState diskState = EpochDiskState.EMPTY;
    private enum State { INITIALIZED, LOADING, STARTED }

    private State state = State.INITIALIZED;
    private volatile EndpointMapping mapping = EndpointMapping.EMPTY;
    private final Long2ObjectHashMap<AccordLocalSyncNotifier> syncNotifiers = new Long2ObjectHashMap<>();

    public enum SyncStatus { NOT_STARTED, NOTIFYING, COMPLETED }

    static class EpochState extends AbstractConfigurationService.AbstractEpochState
    {
        SyncStatus syncStatus = SyncStatus.NOT_STARTED;

        public EpochState(long epoch)
        {
            super(epoch);
        }

        void setSyncStatus(SyncStatus status)
        {
            this.syncStatus = status;
        }
    }

    static class EpochHistory extends AbstractConfigurationService.AbstractEpochHistory<EpochState>
    {
        @Override
        protected EpochState createEpochState(long epoch)
        {
            return new EpochState(epoch);
        }
    }

    public AccordConfigurationService(Node.Id node, MessageDelivery messagingService, IFailureDetector failureDetector)
    {
        super(node);
        this.messagingService = messagingService;
        this.failureDetector = failureDetector;
    }

    public AccordConfigurationService(Node.Id node)
    {
        this(node, MessagingService.instance(), FailureDetector.instance);
    }

    @Override
    protected EpochHistory createEpochHistory()
    {
        return new EpochHistory();
    }

    public synchronized void start()
    {
        Invariants.checkState(state == State.INITIALIZED);
        state = State.LOADING;
        updateMapping(ClusterMetadata.current());
        diskState = AccordKeyspace.loadTopologies(((epoch, topology, syncStatus, pendingSyncNotify, remoteSyncComplete) -> {
            if (topology != null)
                reportTopology(topology, syncStatus == SyncStatus.NOT_STARTED);

            getOrCreateEpochState(epoch).setSyncStatus(syncStatus);
            if (syncStatus == SyncStatus.NOTIFYING)
                syncNotifiers.put(epoch, new AccordLocalSyncNotifier(epoch, localId, pendingSyncNotify, this, messagingService, failureDetector, this));

            remoteSyncComplete.forEach(id -> remoteSyncComplete(id, epoch));
        }));
        syncNotifiers.values().forEach(AccordLocalSyncNotifier::start);
        state = State.STARTED;
    }

    @Override
    public Node.Id mappedId(InetAddressAndPort endpoint)
    {
        return Invariants.nonNull(mapping.mappedId(endpoint));
    }

    @Override
    public InetAddressAndPort mappedEndpoint(Node.Id id)
    {
        return Invariants.nonNull(mapping.mappedEndpoint(id));
    }

    @VisibleForTesting
    EpochDiskState diskState()
    {
        return diskState;
    }

    @VisibleForTesting
    synchronized void updateMapping(EndpointMapping mapping)
    {
        if (mapping.epoch() > this.mapping.epoch())
            this.mapping = mapping;
    }

    synchronized void updateMapping(ClusterMetadata metadata)
    {
        updateMapping(AccordTopologyUtils.directoryToMapping(metadata.epoch.getEpoch(), metadata.directory));
    }

    @Override
    public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next)
    {
        updateMapping(next);
        reportTopology(AccordTopologyUtils.createAccordTopology(next));
    }

    @Override
    protected void fetchTopologyInternal(long epoch)
    {
        ClusterMetadataService.instance().maybeCatchup(Epoch.create(epoch));
    }

    @Override
    protected synchronized void localSyncComplete(Topology topology)
    {
        long epoch = topology.epoch();
        EpochState epochState = getOrCreateEpochState(epoch);
        if (epochState.syncStatus != SyncStatus.NOT_STARTED)
            return;

        Set<Node.Id> pendingNotification = topology.nodes().stream().filter(i -> !localId.equals(i)).collect(Collectors.toSet());
        AccordLocalSyncNotifier notifier = new AccordLocalSyncNotifier(epoch, localId, pendingNotification, this, messagingService, failureDetector, this);
        syncNotifiers.put(epoch, notifier);
        diskState = AccordKeyspace.setNotifyingLocalSync(epoch, pendingNotification, diskState);
        epochState.setSyncStatus(SyncStatus.NOTIFYING);
        notifier.start();
    }

    @Override
    public long currentEpoch()
    {
        return super.currentEpoch();
    }

    @Override
    public synchronized void onEndpointAck(Node.Id id, long epoch)
    {
        EpochState epochState = getOrCreateEpochState(epoch);
        if (epochState.syncStatus != SyncStatus.NOTIFYING)
            return;
        diskState = AccordKeyspace.markLocalSyncAck(id, epoch, diskState);
    }

    @Override
    public synchronized void onComplete(long epoch)
    {
        EpochState epochState = getOrCreateEpochState(epoch);
        epochState.setSyncStatus(SyncStatus.COMPLETED);
        diskState = AccordKeyspace.setCompletedLocalSync(epoch, diskState);
    }

    @Override
    protected synchronized void topologyUpdatePreListenerNotify(Topology topology)
    {
        if (state == State.STARTED)
            diskState = AccordKeyspace.saveTopology(topology, diskState);
    }

    @Override
    protected void remoteSyncCompletePreListenerNotify(Node.Id node, long epoch)
    {
        if (state == State.STARTED)
            diskState = AccordKeyspace.markRemoteTopologySync(node, epoch, diskState);
    }

    @Override
    protected void truncateTopologiesPreListenerNotify(long epoch)
    {
        Invariants.checkState(state == State.STARTED);
    }

    @Override
    protected void truncateTopologiesPostListenerNotify(long epoch)
    {
        if (state == State.STARTED)
            diskState = AccordKeyspace.truncateTopologyUntil(epoch, diskState);
    }
}

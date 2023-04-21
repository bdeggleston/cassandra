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

import com.google.common.annotations.VisibleForTesting;

import accord.impl.AbstractConfigurationService;
import accord.local.Node;
import accord.topology.Topology;
import accord.utils.Invariants;
import org.apache.cassandra.service.accord.AccordKeyspace.EpochDiskState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.listeners.ChangeListener;

// TODO: listen to FailureDetector and rearrange fast path accordingly
public class TCMConfigurationService extends AbstractConfigurationService implements ChangeListener
{
    private EpochDiskState diskState = EpochDiskState.EMPTY;
    private enum State { INITIALIZED, LOADING, STARTED }

    private State state = State.INITIALIZED;

    public TCMConfigurationService(Node.Id node)
    {
        super(node);
    }

    public synchronized void start()
    {
        Invariants.checkState(state == State.INITIALIZED);
        state = State.LOADING;
        diskState = AccordKeyspace.loadTopologies(((epoch, topology, ids) -> {
            if (topology != null)
                reportTopology(topology);
            ids.forEach(id -> epochSyncComplete(id, epoch));
        }));
        state = State.STARTED;
    }

    @VisibleForTesting
    EpochDiskState diskState()
    {
        return diskState;
    }

    @Override
    public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next)
    {
        reportTopology(AccordTopologyUtils.createAccordTopology(next));
    }

    @Override
    protected void fetchTopologyInternal(long epoch)
    {
        ClusterMetadataService.instance().maybeCatchup(Epoch.create(epoch));
    }

    @Override
    protected synchronized void beginEpochSync(long epoch)
    {
        if (state != State.STARTED)
            return;

        // TODO: run a barrier txn and/or accord repair?
        Topology topology = getTopologyForEpoch(epoch);
        topology.nodes().forEach(id -> epochSyncComplete(id, epoch));
    }

    @Override
    protected synchronized void topologyUpdatePreListenerNotify(Topology topology)
    {
        if (state == State.STARTED)
            diskState = AccordKeyspace.saveTopology(topology, diskState);
    }

    @Override
    protected void epochSyncCompletePreListenerNotify(Node.Id node, long epoch)
    {
        if (state == State.STARTED)
            diskState = AccordKeyspace.markTopologySynced(node, epoch, diskState);
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

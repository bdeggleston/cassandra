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
    private final Listener preListener;
    private final Listener postListener;

    public TCMConfigurationService(Node.Id node)
    {
        super(node);
        this.preListener = new Listener()
        {
            @Override
            public void onTopologyUpdate(Topology topology)
            {
                synchronized (TCMConfigurationService.this)
                {
                    if (state == State.STARTED)
                        diskState = AccordKeyspace.saveTopology(topology, diskState);
                }
            }

            @Override
            public void onEpochSyncComplete(Node.Id node, long epoch)
            {
                synchronized (TCMConfigurationService.this)
                {
                    if (state == State.STARTED)
                        diskState = AccordKeyspace.markTopologySynced(node, epoch, diskState);
                }
            }

            @Override
            public void truncateTopologyUntil(long epoch)
            {
                synchronized (TCMConfigurationService.this)
                {
                    Invariants.checkState(state == State.STARTED);
                }
            }
        };

        this.postListener = new Listener()
        {
            @Override
            public void onTopologyUpdate(Topology topology) {}

            @Override
            public void onEpochSyncComplete(Node.Id node, long epoch) {}

            @Override
            public void truncateTopologyUntil(long epoch)
            {
                synchronized (TCMConfigurationService.this)
                {
                    if (state == State.STARTED)
                        diskState = AccordKeyspace.truncateTopologyUntil(epoch, diskState);
                }
            }
        };
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
    protected void beginEpochSync(long epoch)
    {
        // TODO: run a barrier txn?
    }

    @Override
    protected Listener preListener()
    {
        return preListener;
    }

    @Override
    protected Listener postListener()
    {
        return postListener;
    }
}

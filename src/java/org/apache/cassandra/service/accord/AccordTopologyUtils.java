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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.Invariants;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.DataPlacements;

public class AccordTopologyUtils
{
    private static Shard createShard(TokenRange range, Directory directory, EndpointsForRange reads, EndpointsForRange writes)
    {
        Function<InetAddressAndPort, Node.Id> endpointMapper = e -> {
            NodeId tcmId = directory.peerId(e);
            // FIXME: this doesn't work
            return new Node.Id((int) tcmId.uuid.getLeastSignificantBits());
        };
        Set<InetAddressAndPort> endpoints = reads.endpoints();
        Set<InetAddressAndPort> writeEndpoints = writes.endpoints();
        List<Node.Id> nodes = endpoints.stream().map(endpointMapper).collect(Collectors.toList());
        Set<Node.Id> fastPath = new HashSet<>(nodes);  // TODO: support fast path updates
        Set<Node.Id> pending = endpoints.equals(writeEndpoints) ?
                               Collections.emptySet() :
                               writeEndpoints.stream().filter(e -> !endpoints.contains(e)).map(endpointMapper).collect(Collectors.toSet());

        return new Shard(range, nodes, fastPath, pending);
    }

    private static TokenRange minRange(String keyspace, Token token)
    {
        return new TokenRange(AccordRoutingKey.SentinelKey.min(keyspace), new AccordRoutingKey.TokenKey(keyspace, token));
    }

    private static TokenRange maxRange(String keyspace, Token token)
    {
        return new TokenRange(new AccordRoutingKey.TokenKey(keyspace, token), AccordRoutingKey.SentinelKey.max(keyspace));
    }

    private static TokenRange range(String keyspace, Token left, Token right)
    {
        return new TokenRange(new AccordRoutingKey.TokenKey(keyspace, left), new AccordRoutingKey.TokenKey(keyspace, right));
    }

    private static TokenRange range(String keyspace, Range<Token> range)
    {
        return new TokenRange(new AccordRoutingKey.TokenKey(keyspace, range.left), new AccordRoutingKey.TokenKey(keyspace, range.right));
    }

    public static List<Shard> createShards(KeyspaceMetadata keyspace, DataPlacements placements, Directory directory)
    {
        ReplicationParams replication = keyspace.params.replication;
        DataPlacement placement = placements.get(replication);

        List<Range<Token>> ranges = placement.reads.ranges();
        List<Shard> shards = new ArrayList<>(ranges.size() + 1);
        Shard finalShard = null;
        for (Range<Token> range : ranges)
        {
            EndpointsForRange reads = placement.reads.forRange(range);
            EndpointsForRange writes = placement.reads.forRange(range);

            if (shards.isEmpty())
            {
                Invariants.checkState(range.isWrapAround());
                shards.add(createShard(minRange(keyspace.name, range.right), directory, reads, writes));
                finalShard = createShard(maxRange(keyspace.name, range.left), directory, reads, writes);
            }
            else
            {
                shards.add(createShard(range(keyspace.name, range), directory, reads, writes));
            }
        }
        shards.add(finalShard);

        return shards;
    }

    public static Topology createAccordTopology(Epoch epoch, DistributedSchema schema, DataPlacements placements, Directory directory)
    {
        List<Shard> shards = new ArrayList<>();
        for (KeyspaceMetadata keyspace : schema.getKeyspaces())
            shards.addAll(createShards(keyspace, placements, directory));
        return new Topology(epoch.getEpoch(), shards.toArray(new Shard[0]));
    }

    public static Topology createAccordTopology(ClusterMetadata metadata)
    {
        return createAccordTopology(metadata.epoch, metadata.schema, metadata.placements, metadata.directory);
    }

    public static Topology createAccordTopology(long epoch)
    {
        throw new UnsupportedOperationException("Switch to ClusterMetadata -> Accord Topology");
    }
}

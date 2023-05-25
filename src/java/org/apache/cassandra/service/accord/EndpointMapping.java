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

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import accord.local.Node;
import accord.utils.Invariants;
import org.apache.cassandra.locator.InetAddressAndPort;

class EndpointMapping implements AccordEndpointMapper
{
    public static final EndpointMapping EMPTY = new EndpointMapping(0, ImmutableMap.of(), ImmutableMap.of());
    private final long epoch;
    private final ImmutableMap<Node.Id, InetAddressAndPort> idToEndpoint;
    private final ImmutableMap<InetAddressAndPort, Node.Id> endpointToId;

    private EndpointMapping(long epoch,
                            ImmutableMap<Node.Id, InetAddressAndPort> idToEndpoint,
                            ImmutableMap<InetAddressAndPort, Node.Id> endpointToId)
    {
        this.epoch = epoch;
        this.idToEndpoint = idToEndpoint;
        this.endpointToId = endpointToId;
    }

    long epoch()
    {
        return epoch;
    }

    @Override
    public Node.Id mappedId(InetAddressAndPort endpoint)
    {
        return endpointToId.get(endpoint);
    }

    @Override
    public InetAddressAndPort mappedEndpoint(Node.Id id)
    {
        return idToEndpoint.get(id);
    }

    static class Builder
    {
        private final long epoch;
        private final Map<Node.Id, InetAddressAndPort> idToEndpoint = new HashMap<>();
        private final Map<InetAddressAndPort, Node.Id> endpointToId = new HashMap<>();

        public Builder(long epoch)
        {
            this.epoch = epoch;
        }

        public Builder add(InetAddressAndPort endpoint, Node.Id id)
        {
            Invariants.checkArgument(!idToEndpoint.containsKey(id));
            Invariants.checkArgument(!endpointToId.containsKey(endpoint));
            idToEndpoint.put(id, endpoint);
            endpointToId.put(endpoint, id);
            return this;
        }

        public EndpointMapping build()
        {
            return new EndpointMapping(epoch, ImmutableMap.copyOf(idToEndpoint), ImmutableMap.copyOf(endpointToId));
        }
    }

    static Builder builder(long epoch)
    {
        return new Builder(epoch);
    }
}

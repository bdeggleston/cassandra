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

package org.apache.cassandra.locator;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.UnavailableException;

/**
 * Decorated Endpoint
 */
public class Replica
{
    private final InetAddressAndPort endpoint;
    private final boolean full;

    public Replica(InetAddressAndPort endpoint, boolean full)
    {
        Preconditions.checkNotNull(endpoint);
        this.endpoint = endpoint;
        this.full = full;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Replica replica = (Replica) o;
        return full == replica.full &&
               Objects.equals(endpoint, replica.endpoint);
    }

    public int hashCode()
    {

        return Objects.hash(endpoint, full);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(full ? "Full" : "Transient");
        sb.append('{').append(getEndpoint()).append('}');
        return sb.toString();
    }

    public final InetAddressAndPort getEndpoint()
    {
        return endpoint;
    }

    public ReplicatedRange decorateRange(Token left, Token right)
    {
        return new ReplicatedRange(left, right, full);
    }

    public ReplicatedRange decorateRange(Range<Token> range)
    {
        return decorateRange(range.left, range.right);
    }

    public boolean isFull()
    {
        return full;
    }

    public final boolean isTransient()
    {
        return !isFull();
    }


    public static Replica full(InetAddressAndPort endpoint)
    {
        return new Replica(endpoint, true);
    }

    public static Replica trans(InetAddressAndPort endpoint)
    {
        return new Replica(endpoint, false);
    }

    public static void assureSufficientFullReplica(Collection<Replica> replicas, ConsistencyLevel cl) throws UnavailableException
    {
        if (!Iterables.any(replicas, Replica::isFull))
        {
            throw new UnavailableException(cl, "At least one full replica required", 1, 0);
        }
    }

    public static Iterable<InetAddressAndPort> toEndpoints(Iterable<Replica> replicas)
    {
        return Iterables.transform(replicas, Replica::getEndpoint);
    }

    public static List<InetAddressAndPort> toEndpointList(List<Replica> replicas)
    {
        return Lists.newArrayList(toEndpoints(replicas));
    }
}


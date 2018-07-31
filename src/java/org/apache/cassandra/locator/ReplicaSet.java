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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class ReplicaSet extends ReplicaCollection
{
    static final ReplicaSet EMPTY = new ReplicaSet(ImmutableSet.of());

    public static ReplicaSet empty()
    {
        return EMPTY;
    }

    private static final Set<Collector.Characteristics> SET_COLLECTOR_CHARACTERISTICS = ImmutableSet.of(Collector.Characteristics.UNORDERED, Collector.Characteristics.IDENTITY_FINISH);
    public static final Collector<Replica, ReplicaSet, ReplicaSet> COLLECTOR = new Collector<Replica, ReplicaSet, ReplicaSet>()
    {
        private final Supplier<ReplicaSet> supplier = ReplicaSet::new;
        private final BiConsumer<ReplicaSet, Replica> accumulator = (set, replica) -> set.add(replica);
        private final BinaryOperator<ReplicaSet> combiner = (a, b) -> {
            if (a.size() > b.size())
            {
                a.addAll(b);
                return a;
            }
            b.addAll(a);
            return b;
        };
        private final Function<ReplicaSet, ReplicaSet> finisher = set -> set;

        public Supplier<ReplicaSet> supplier()
        {
            return supplier;
        }

        public BiConsumer<ReplicaSet, Replica> accumulator()
        {
            return accumulator;
        }

        public BinaryOperator<ReplicaSet> combiner()
        {
            return combiner;
        }

        public Function<ReplicaSet, ReplicaSet> finisher()
        {
            return finisher;
        }

        public Set<Characteristics> characteristics()
        {
            return SET_COLLECTOR_CHARACTERISTICS;
        }
    };

    private final Set<Replica> replicaSet;

    public ReplicaSet()
    {
        this(new HashSet<>());
    }

    public ReplicaSet(int expectedSize)
    {
        this(Sets.newHashSetWithExpectedSize(expectedSize));
    }

    public ReplicaSet(ReplicaCollection replicas)
    {
        this(Sets.newHashSetWithExpectedSize(replicas.size()));
        Iterables.addAll(replicaSet, replicas);
    }

    private ReplicaSet(Set<Replica> replicaSet)
    {
        this.replicaSet = replicaSet;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicaSet that = (ReplicaSet) o;
        return Objects.equals(replicaSet, that.replicaSet);
    }

    public int hashCode()
    {
        return replicaSet.hashCode();
    }

    @Override
    public boolean add(Replica replica)
    {
        Preconditions.checkNotNull(replica);
        return replicaSet.add(replica);
    }

    @Override
    public void addAll(Iterable<Replica> replicas)
    {
        Iterables.addAll(replicaSet, replicas);
    }

    @Override
    public void removeEndpoint(InetAddressAndPort endpoint)
    {
        Preconditions.checkNotNull(endpoint);
        Iterator<Replica> iterator = replicaSet.iterator();
        while (iterator.hasNext())
        {
            Replica next = iterator.next();
            if (next.getEndpoint().equals(endpoint))
            {
                iterator.remove();
            }
        }
    }

    @Override
    public void removeReplica(Replica replica)
    {
        Preconditions.checkNotNull(replica);
        replicaSet.remove(replica);
    }

    @Override
    public int size()
    {
        return replicaSet.size();
    }

    @Override
    protected Collection<Replica> getUnmodifiableCollection()
    {
        return Collections.unmodifiableCollection(replicaSet);
    }

    @Override
    public Iterator<Replica> iterator()
    {
        return replicaSet.iterator();
    }

    public ReplicaSet differenceOnEndpoint(ReplicaCollection differenceOn)
    {
        if (Iterables.all(this, Replica::isFull) && Iterables.all(differenceOn, Replica::isFull))
        {
            Set<InetAddressAndPort> diffEndpoints = differenceOn.asEndpointSet();
            return new ReplicaSet(Replicas.filterOnEndpoints(this, e -> !diffEndpoints.contains(e)));
        }
        else
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    public ReplicaSet intersectOnEndoints(Set<InetAddressAndPort> endpoints)
    {
        ReplicaSet intersection = new ReplicaSet(size());
        for (Replica replica : this)
        {
            if (endpoints.contains(replica.getEndpoint()))
                intersection.add(replica);
        }
        return intersection;
    }

    public ReplicaSet filter(Predicate<Replica>... predicates)
    {
        return filter(predicates, ReplicaSet::new);
    }

    public boolean containsReplica(Replica replica)
    {
        return replicaSet.contains(replica);
    }

    @Override
    public Stream<Replica> stream()
    {
        return replicaSet.stream();
    }

    public static ReplicaSet immutableCopyOf(ReplicaSet from)
    {
        return new ReplicaSet(ImmutableSet.copyOf(from.replicaSet));
    }

    public static ReplicaSet immutableCopyOf(ReplicaCollection from)
    {
        return new ReplicaSet(ImmutableSet.<Replica>builder().addAll(from).build());
    }

    public static ReplicaSet of(Replica replica)
    {
        HashSet<Replica> set = Sets.newHashSetWithExpectedSize(1);
        set.add(replica);
        return new ReplicaSet(set);
    }

    public static ReplicaSet of(Replica... replicas)
    {
        ReplicaSet set = new ReplicaSet(Sets.newHashSetWithExpectedSize(replicas.length));
        for (Replica replica : replicas)
        {
            set.add(replica);
        }
        return set;
    }

    /**
     * Returns a ReplicaSet wrapping a LinkedHashSet that preserves order the same way LinkedHashSet does.
     * @return
     */
    public static ReplicaSet orderPreserving()
    {
        return new ReplicaSet(new LinkedHashSet<>());
    }
}

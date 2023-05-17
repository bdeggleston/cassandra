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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import accord.api.Data;
import accord.api.DataStore;
import accord.api.Query;
import accord.api.Read;
import accord.api.Update;
import accord.impl.AbstractFetchCoordinator;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.primitives.PartialTxn;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.service.accord.api.AccordRoutableKey;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.CastingSerializer;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.CollectionSerializers.deserializeMap;
import static org.apache.cassandra.utils.CollectionSerializers.serializeMap;
import static org.apache.cassandra.utils.CollectionSerializers.serializedMapSize;

public class AccordFetchCoordinator extends AbstractFetchCoordinator implements StreamManager.StreamListener
{
    private static final Query noopQuery = (txnId, executeAt, data, read, update) -> null;

    public static class StreamData implements Data
    {
        public static final IVersionedSerializer<StreamData> serializer = new IVersionedSerializer<StreamData>()
        {
            @Override
            public void serialize(StreamData data, DataOutputPlus out, int version) throws IOException
            {
                serializeMap(data.streams, out, version,
                             TokenRange.serializer, TimeUUID.Serializer.instance);
            }

            @Override
            public StreamData deserialize(DataInputPlus in, int version) throws IOException
            {

                return new StreamData(ImmutableMap.copyOf(deserializeMap(in, version,
                                                                         TokenRange.serializer,
                                                                         TimeUUID.Serializer.instance)));
            }

            @Override
            public long serializedSize(StreamData data, int version)
            {
                return serializedMapSize(data.streams, version, TokenRange.serializer, TimeUUID.Serializer.instance);
            }
        };

        private final ImmutableMap<TokenRange, TimeUUID> streams;

        public StreamData(ImmutableMap<TokenRange, TimeUUID> streams)
        {
            this.streams = streams;
        }

        public static StreamData of(TokenRange range, TimeUUID streamId)
        {
            return new StreamData(ImmutableMap.of(range, streamId));
        }

        @Override
        public Data merge(Data data)
        {
            StreamData that = (StreamData) data;
            ImmutableMap.Builder<TokenRange, TimeUUID> builder = ImmutableMap.builder();
            builder.putAll(this.streams);
            builder.putAll(that.streams);
            return new StreamData(builder.build());
        }
    }

    // needs to be externally synchronized
    private class IncomingStream
    {
        private final TimeUUID planId;
        private Range range;
        private Node.Id from;
        private StreamResultFuture future;

        public IncomingStream(TimeUUID planId)
        {
            this.planId = planId;
        }

        private void rangeReceived(Range range, Node.Id from)
        {
            Invariants.checkArgument(range != null);
            Invariants.checkArgument(from != null);
            Invariants.checkState(this.range == null);
            Invariants.checkState(this.from == null);
            this.range = range;
            this.from = from;
            maybeListen();
        }

        private void futureReceived(StreamResultFuture future)
        {
            Invariants.checkArgument(future != null);
            Invariants.checkState(this.future == null);
            this.future = future;
            maybeListen();
        }

        private void maybeListen()
        {
            if (range == null || future == null)
                return;

            Invariants.checkState(from != null);

            future.addCallback((state, fail) -> {
                if (fail == null) success(from, Ranges.of(range));
                else fail(from, Ranges.of(range), fail);
            });
        }
    }

    public static class StreamingRead implements Read
    {
        public static final IVersionedSerializer<StreamingRead> serializer = new IVersionedSerializer<StreamingRead>()
        {
            @Override
            public void serialize(StreamingRead read, DataOutputPlus out, int version) throws IOException
            {
                InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serialize(read.to, out, version);
                KeySerializers.ranges.serialize(read.ranges, out, version);
            }

            @Override
            public StreamingRead deserialize(DataInputPlus in, int version) throws IOException
            {
                return new StreamingRead(InetAddressAndPort.Serializer.inetAddressAndPortSerializer.deserialize(in, version),
                                         KeySerializers.ranges.deserialize(in, version));
            }

            @Override
            public long serializedSize(StreamingRead read, int version)
            {
                return InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serializedSize(read.to, version)
                       + KeySerializers.ranges.serializedSize(read.ranges, version);
            }
        };

        private final InetAddressAndPort to;
        private final Ranges ranges;

        public StreamingRead(InetAddressAndPort to, Ranges ranges)
        {
            this.to = to;
            this.ranges = ranges;
        }

        @Override
        public Seekables<?, ?> keys() { return ranges; }

        private static String keyspace(TokenRange range)
        {
            AccordRoutableKey start = (AccordRoutableKey) range.start();
            AccordRoutableKey end = (AccordRoutableKey) range.end();
            Invariants.checkArgument(start.keyspace().equals(end.keyspace()));
            return start.keyspace();
        }

        @Override
        public AsyncChain<Data> read(Seekable key, Txn.Kind kind, SafeCommandStore commandStore, Timestamp executeAt, DataStore store)
        {
            try
            {
                Invariants.checkArgument(key.domain() == Routable.Domain.Range);
                TokenRange range = (TokenRange) key;
                String keyspace = keyspace(range);

                // TODO: check epoch
                // FIXME: what if we need to stream from a table that's been dropped from the current schema?
                KeyspaceMetadata ksm = ClusterMetadata.current().schema.getKeyspaceMetadata(keyspace);
                Invariants.checkState(ksm != null, "Keyspace %s not found", keyspace);
                Invariants.checkState(ksm.tables.size() > 0, "Keyspace '%s' has no tables", keyspace);

                // FIXME: may also be relocation
                StreamPlan plan = new StreamPlan(StreamOperation.BOOTSTRAP, 1, false,
                                                 null, PreviewKind.NONE).flushBeforeTransfer(true);

                RangesAtEndpoint ranges = RangesAtEndpoint.toDummyList(Collections.singleton(range.toKeyspaceRange()));
                ksm.tables.forEach(table -> plan.transferRanges(to, table.keyspace, ranges, table.name));
                StreamResultFuture future = plan.execute();
                return AsyncChains.success(StreamData.of(range, future.planId));
            }
            catch (Throwable t)
            {
                return AsyncChains.failure(t);
            }
        }

        @Override
        public Read slice(Ranges ranges) { return new StreamingRead(to, this.ranges.slice(ranges)); }

        @Override
        public Read merge(Read other) { throw new UnsupportedOperationException(); }
    }

    public static class StreamingTxn
    {
        private static final IVersionedSerializer<Read> read = new CastingSerializer<>(Read.class,
                                                                                       StreamingRead.class,
                                                                                       StreamingRead.serializer);

        private static final IVersionedSerializer<Query> query = new IVersionedSerializer<Query>()
        {
            @Override
            public void serialize(Query t, DataOutputPlus out, int version) throws IOException
            {
                Invariants.checkArgument(t == noopQuery);
            }

            @Override
            public Query deserialize(DataInputPlus in, int version) throws IOException
            {
                return noopQuery;
            }

            @Override
            public long serializedSize(Query t, int version)
            {
                Invariants.checkArgument(t == noopQuery);
                return 0;
            }
        };

        private static final IVersionedSerializer<Update> update = new IVersionedSerializer<Update>()
        {
            @Override
            public void serialize(Update t, DataOutputPlus out, int version) throws IOException
            {
                Invariants.checkArgument(t == null);
            }

            @Override
            public Update deserialize(DataInputPlus in, int version) throws IOException
            {
                return null;
            }

            @Override
            public long serializedSize(Update t, int version)
            {
                Invariants.checkArgument(t == null);
                return 0;
            }
        };

        // TODO (desired): this could be serialized as an InetAddressAndPort and Ranges if we had a special case PartialTxn implementation
        public static final IVersionedSerializer<PartialTxn> serializer = new CommandSerializers.PartialTxnSerializer(read, query, update);
    }

    private final Map<TimeUUID, IncomingStream> streams = new HashMap<>();

    public AccordFetchCoordinator(Node node, Ranges ranges, SyncPoint syncPoint, DataStore.FetchRanges fetchRanges, CommandStore commandStore)
    {
        super(node, ranges, syncPoint, fetchRanges, commandStore);
    }

    @Override
    public void start()
    {
        StreamManager.instance.addListener(this);
        super.start();
    }

    private IncomingStream stream(TimeUUID id)
    {
        return streams.computeIfAbsent(id, IncomingStream::new);
    }

    @Override
    public synchronized void onRegister(StreamResultFuture result)
    {
        stream(result.planId).futureReceived(result);
    }

    @Override
    protected PartialTxn rangeReadTxn(Ranges ranges)
    {
        StreamingRead read = new StreamingRead(FBUtilities.getBroadcastAddressAndPort(), ranges);
        return new PartialTxn.InMemory(ranges, Txn.Kind.Read, ranges, read, noopQuery, null);
    }

    @Override
    protected synchronized void onReadOk(Node.Id from, CommandStore commandStore, Data data, Ranges received)
    {
        if (data == null)
            return;

        StreamData streamData = (StreamData) data;
        streamData.streams.forEach((range, planId) -> stream(planId).rangeReceived(range, from));
    }
}

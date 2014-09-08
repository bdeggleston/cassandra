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
package org.apache.cassandra.repair.messages;

import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.repair.RepairJobDesc;

/**
 * Body part of SYNC_REQUEST repair message.
 * Request {@code src} node to sync data with {@code dst} node for range {@code ranges}.
 *
 * @since 2.0
 */
public class SyncRequest extends RepairMessage
{
    public final InetAddress initiator;
    public final InetAddress src;
    public final InetAddress dst;
    public final Collection<Range<Token>> ranges;

    public SyncRequest(RepairJobDesc desc, InetAddress initiator, InetAddress src, InetAddress dst, Collection<Range<Token>> ranges, RepairMessage.Serializer serializer)
    {
        super(Type.SYNC_REQUEST, desc, serializer);
        this.initiator = initiator;
        this.src = src;
        this.dst = dst;
        this.ranges = ranges;
    }

    public static class Serializer implements MessageSerializer<SyncRequest>
    {
        private final AbstractBounds.Serializer abstractBoundsSerializer;
        private final RepairMessage.Serializer repairMessageSerializer;
        private final RepairJobDesc.RepairJobDescSerializer repairJobDescSerializer;

        public Serializer(AbstractBounds.Serializer abstractBoundsSerializer, RepairMessage.Serializer repairMessageSerializer, RepairJobDesc.RepairJobDescSerializer repairJobDescSerializer)
        {
            this.abstractBoundsSerializer = abstractBoundsSerializer;
            this.repairMessageSerializer = repairMessageSerializer;
            this.repairJobDescSerializer = repairJobDescSerializer;
        }

        public void serialize(SyncRequest message, DataOutputPlus out, int version) throws IOException
        {
            repairJobDescSerializer.serialize(message.desc, out, version);
            CompactEndpointSerializationHelper.serialize(message.initiator, out);
            CompactEndpointSerializationHelper.serialize(message.src, out);
            CompactEndpointSerializationHelper.serialize(message.dst, out);
            out.writeInt(message.ranges.size());
            for (Range<Token> range : message.ranges)
                abstractBoundsSerializer.serialize(range, out, version);
        }

        public SyncRequest deserialize(DataInput in, int version) throws IOException
        {
            RepairJobDesc desc = repairJobDescSerializer.deserialize(in, version);
            InetAddress owner = CompactEndpointSerializationHelper.deserialize(in);
            InetAddress src = CompactEndpointSerializationHelper.deserialize(in);
            InetAddress dst = CompactEndpointSerializationHelper.deserialize(in);
            int rangesCount = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>(rangesCount);
            for (int i = 0; i < rangesCount; ++i)
                ranges.add((Range<Token>) abstractBoundsSerializer.deserialize(in, version).toTokenBounds());
            return new SyncRequest(desc, owner, src, dst, ranges, repairMessageSerializer);
        }

        public long serializedSize(SyncRequest message, int version)
        {
            long size = repairJobDescSerializer.serializedSize(message.desc, version);
            size += 3 * CompactEndpointSerializationHelper.serializedSize(message.initiator);
            size += TypeSizes.NATIVE.sizeof(message.ranges.size());
            for (Range<Token> range : message.ranges)
                size += abstractBoundsSerializer.serializedSize(range, version);
            return size;
        }
    }
}

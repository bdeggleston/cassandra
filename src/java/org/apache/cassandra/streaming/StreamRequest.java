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
package org.apache.cassandra.streaming;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.LocatorConfig;

public class StreamRequest
{
    public final String keyspace;
    public final Collection<Range<Token>> ranges;
    public final Collection<String> columnFamilies = new HashSet<>();
    public final long repairedAt;
    private final IPartitioner partitioner;

    public StreamRequest(String keyspace, Collection<Range<Token>> ranges, Collection<String> columnFamilies, long repairedAt, IPartitioner p)
    {
        this.keyspace = keyspace;
        this.ranges = ranges;
        this.columnFamilies.addAll(columnFamilies);
        this.repairedAt = repairedAt;
        this.partitioner = p;
    }

    public static class Serializer implements IVersionedSerializer<StreamRequest>
    {

        private final IPartitioner partitioner;
        private final Token.Serializer tokenSerializer;

        public Serializer(IPartitioner partitioner, Token.Serializer tokenSerializer)
        {
            this.partitioner = partitioner;
            this.tokenSerializer = tokenSerializer;
        }

        public void serialize(StreamRequest request, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(request.keyspace);
            out.writeLong(request.repairedAt);
            out.writeInt(request.ranges.size());
            for (Range<Token> range : request.ranges)
            {
                tokenSerializer.serialize(range.left, out);
                tokenSerializer.serialize(range.right, out);
            }
            out.writeInt(request.columnFamilies.size());
            for (String cf : request.columnFamilies)
                out.writeUTF(cf);
        }

        public StreamRequest deserialize(DataInput in, int version) throws IOException
        {
            String keyspace = in.readUTF();
            long repairedAt = in.readLong();
            int rangeCount = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>(rangeCount);
            for (int i = 0; i < rangeCount; i++)
            {
                Token left = tokenSerializer.deserialize(in);
                Token right = tokenSerializer.deserialize(in);
                ranges.add(new Range<>(left, right, partitioner));
            }
            int cfCount = in.readInt();
            List<String> columnFamilies = new ArrayList<>(cfCount);
            for (int i = 0; i < cfCount; i++)
                columnFamilies.add(in.readUTF());
            return new StreamRequest(keyspace, ranges, columnFamilies, repairedAt, partitioner);
        }

        public long serializedSize(StreamRequest request, int version)
        {
            int size = TypeSizes.NATIVE.sizeof(request.keyspace);
            size += TypeSizes.NATIVE.sizeof(request.repairedAt);
            size += TypeSizes.NATIVE.sizeof(request.ranges.size());
            for (Range<Token> range : request.ranges)
            {
                size += tokenSerializer.serializedSize(range.left, TypeSizes.NATIVE);
                size += tokenSerializer.serializedSize(range.right, TypeSizes.NATIVE);
            }
            size += TypeSizes.NATIVE.sizeof(request.columnFamilies.size());
            for (String cf : request.columnFamilies)
                size += TypeSizes.NATIVE.sizeof(cf);
            return size;
        }
    }
}

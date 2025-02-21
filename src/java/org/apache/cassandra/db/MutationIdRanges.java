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
package org.apache.cassandra.db;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;

import java.io.IOException;
import java.util.Objects;

/**
 * range of mutation ids contained by sstable/stream/memtable
 *
 * currently just min max, but should expand to contain per-coordinator/shard id ranges
 */
public class MutationIdRanges
{
    public static final MutationIdRanges NONE = new MutationIdRanges(MutationId.none(), MutationId.none());
    public final MutationId minId;
    public final MutationId maxId;

    public MutationIdRanges(MutationId minId, MutationId maxId)
    {
        this.minId = minId;
        this.maxId = maxId;
    }

    @Override
    public String toString()
    {
        return "MutationIdRanges{" +
                "minId=" + minId +
                ", maxId=" + maxId +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        MutationIdRanges that = (MutationIdRanges) o;
        return Objects.equals(minId, that.minId) && Objects.equals(maxId, that.maxId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(minId, maxId);
    }

    public MutationIdRanges merge(MutationIdRanges that)
    {
        if (this == NONE)
            return that;
        if (that == NONE)
            return this;
        return new MutationIdRanges(MutationId.minNotNone(this.minId, that.minId), MutationId.max(this.maxId, that.maxId));
    }

    public MutationIdRanges subset(PartitionPosition from, PartitionPosition to)
    {
        return this;
    }

    public static MutationIdRanges merge(MutationIdRanges l, MutationIdRanges r)
    {
        return new MutationIdRanges(MutationId.minNotNone(l.minId, r.minId), MutationId.max(l.maxId, r.maxId));
    }

    public MutationIdRanges add(MutationId mutationId)
    {
        return new MutationIdRanges(MutationId.minNotNone(minId, mutationId), MutationId.max(maxId, mutationId));
    }

    public static MutationIdRanges fixme()
    {
        throw new RuntimeException("TODO");
    }

    public static final IVersionedSerializer<MutationIdRanges> serializer = new IVersionedSerializer<MutationIdRanges>()
    {
        @Override
        public void serialize(MutationIdRanges metadata, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_52)
                return;
            MutationId.serializer.serialize(metadata.minId, out, version);
            MutationId.serializer.serialize(metadata.maxId, out, version);
        }

        @Override
        public MutationIdRanges deserialize(DataInputPlus in, int version) throws IOException
        {
            if (version < MessagingService.VERSION_52)
                return MutationIdRanges.NONE;
            return new MutationIdRanges(MutationId.serializer.deserialize(in, version),
                                        MutationId.serializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(MutationIdRanges metadata, int version)
        {
            if (version < MessagingService.VERSION_52)
                return 0;
            return MutationId.serializer.serializedSize(metadata.minId, version)
                    + MutationId.serializer.serializedSize(metadata.maxId, version);
        }
    };
}

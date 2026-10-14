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
package org.apache.cassandra.replication;

import java.io.IOException;
import java.util.Objects;

import com.google.common.base.Preconditions;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IVerbHandler;

public class BroadcastNewLog
{
    public final CoordinatorLogId logId;
    public final ShardMetadata shardMetadata;

    public BroadcastNewLog(CoordinatorLogId logId, ShardMetadata shardMetadata)
    {
        this.logId = Preconditions.checkNotNull(logId);
        this.shardMetadata = Preconditions.checkNotNull(shardMetadata);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof BroadcastNewLog)) return false;
        BroadcastNewLog that = (BroadcastNewLog) o;
        return logId.equals(that.logId) && shardMetadata.equals(that.shardMetadata);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(logId, shardMetadata);
    }

    @Override
    public String toString()
    {
        return "BroadcastNewLog{" + logId + ", " + shardMetadata + '}';
    }

    public static final IVerbHandler<BroadcastNewLog> verbHandler = message -> {
        MutationTrackingService.ensureEnabled();
        BroadcastNewLog announcement = message.payload;
        // The implementation of registerRemoteLog will be completed in subtask 1.2
        MutationTrackingService.instance().registerRemoteLog(announcement.logId, announcement.shardMetadata);
    };

    public static final VersionedSerializer<BroadcastNewLog> serializer = new VersionedSerializer<>()
    {
        @Override
        public void serialize(BroadcastNewLog announcement, DataOutputPlus out, Version version) throws IOException
        {
            CoordinatorLogId.serializer.serialize(announcement.logId, out);
            ShardMetadata.serializer.serialize(announcement.shardMetadata, out, version);
        }

        @Override
        public BroadcastNewLog deserialize(DataInputPlus in, Version version) throws IOException
        {
            CoordinatorLogId logId = CoordinatorLogId.serializer.deserialize(in);
            ShardMetadata shardMetadata = ShardMetadata.serializer.deserialize(in, version);
            return new BroadcastNewLog(logId, shardMetadata);
        }

        @Override
        public long serializedSize(BroadcastNewLog announcement, Version version)
        {
            return CoordinatorLogId.serializer.serializedSize(announcement.logId)
                 + ShardMetadata.serializer.serializedSize(announcement.shardMetadata, version);
        }
    };
}

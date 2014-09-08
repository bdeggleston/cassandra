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

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.repair.NodePair;
import org.apache.cassandra.repair.RepairJobDesc;

/**
 *
 * @since 2.0
 */
public class SyncComplete extends RepairMessage
{
    /** nodes that involved in this sync */
    public final NodePair nodes;
    /** true if sync success, false otherwise */
    public final boolean success;

    public SyncComplete(RepairJobDesc desc, NodePair nodes, boolean success, RepairMessage.Serializer serializer)
    {
        super(Type.SYNC_COMPLETE, desc, serializer);
        this.nodes = nodes;
        this.success = success;
    }

    public SyncComplete(RepairJobDesc desc, InetAddress endpoint1, InetAddress endpoint2, boolean success, RepairMessage.Serializer serializer)
    {
        super(Type.SYNC_COMPLETE, desc, serializer);
        this.nodes = new NodePair(endpoint1, endpoint2);
        this.success = success;
    }

    public static class Serializer implements MessageSerializer<SyncComplete>
    {

        private final RepairMessage.Serializer repairMessageSerializer;
        private final RepairJobDesc.RepairJobDescSerializer repairJobDescSerializer;

        public Serializer(RepairMessage.Serializer repairMessageSerializer, RepairJobDesc.RepairJobDescSerializer repairJobDescSerializer)
        {
            this.repairMessageSerializer = repairMessageSerializer;
            this.repairJobDescSerializer = repairJobDescSerializer;
        }

        public void serialize(SyncComplete message, DataOutputPlus out, int version) throws IOException
        {
            repairJobDescSerializer.serialize(message.desc, out, version);
            NodePair.serializer.serialize(message.nodes, out, version);
            out.writeBoolean(message.success);
        }

        public SyncComplete deserialize(DataInput in, int version) throws IOException
        {
            RepairJobDesc desc = repairJobDescSerializer.deserialize(in, version);
            NodePair nodes = NodePair.serializer.deserialize(in, version);
            return new SyncComplete(desc, nodes, in.readBoolean(), repairMessageSerializer);
        }

        public long serializedSize(SyncComplete message, int version)
        {
            long size = repairJobDescSerializer.serializedSize(message.desc, version);
            size += NodePair.serializer.serializedSize(message.nodes, version);
            size += TypeSizes.NATIVE.sizeof(message.success);
            return size;
        }
    }
}

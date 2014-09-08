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

import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.repair.RepairJobDesc;

public class SnapshotMessage extends RepairMessage
{
    public SnapshotMessage(RepairJobDesc desc, RepairMessage.Serializer serializer)
    {
        super(Type.SNAPSHOT, desc, serializer);
    }

    public static class Serializer implements MessageSerializer<SnapshotMessage>
    {
        private final RepairMessage.Serializer repairMessageSerializer;
        private final RepairJobDesc.RepairJobDescSerializer repairJobDescSerializer;

        public Serializer(RepairMessage.Serializer repairMessageSerializer, RepairJobDesc.RepairJobDescSerializer repairJobDescSerializer)
        {
            this.repairMessageSerializer = repairMessageSerializer;
            this.repairJobDescSerializer = repairJobDescSerializer;
        }

        public void serialize(SnapshotMessage message, DataOutputPlus out, int version) throws IOException
        {
            repairJobDescSerializer.serialize(message.desc, out, version);
        }

        public SnapshotMessage deserialize(DataInput in, int version) throws IOException
        {
            RepairJobDesc desc = repairJobDescSerializer.deserialize(in, version);
            return new SnapshotMessage(desc, repairMessageSerializer);
        }

        public long serializedSize(SnapshotMessage message, int version)
        {
            return repairJobDescSerializer.serializedSize(message.desc, version);
        }
    }
}

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
import java.util.EnumMap;
import java.util.Map;

import org.apache.cassandra.db.DBConfig;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.RepairJobDesc;

/**
 * Base class of all repair related request/response messages.
 *
 * @since 2.0
 */
public abstract class RepairMessage
{
    public static interface MessageSerializer<T extends RepairMessage> extends IVersionedSerializer<T> {}

    public static enum Type
    {
        VALIDATION_REQUEST(0),
        VALIDATION_COMPLETE(1),
        SYNC_REQUEST(2),
        SYNC_COMPLETE(3),
        ANTICOMPACTION_REQUEST(4),
        PREPARE_MESSAGE(5),
        SNAPSHOT(6);

        private final byte type;

        private Type(int type)
        {
            this.type = (byte) type;
        }

        public static Type fromByte(byte b)
        {
            for (Type t : values())
            {
               if (t.type == b)
                   return t;
            }
            throw new IllegalArgumentException("Unknown RepairMessage.Type: " + b);
        }

        public static Map<Type, MessageSerializer> getSerializerMap(Serializer serializer, DBConfig dbConfig)
        {
            RepairJobDesc.RepairJobDescSerializer repairJobDescSerializer = new RepairJobDesc.RepairJobDescSerializer(dbConfig.boundsSerializer);
            Map <Type, MessageSerializer> serializers = new EnumMap<>(Type.class);
            serializers.put(VALIDATION_REQUEST, new ValidationRequest.Serializer(serializer, repairJobDescSerializer));
            serializers.put(VALIDATION_COMPLETE, new ValidationComplete.Serializer(serializer, dbConfig.merkleTreeSerializer, repairJobDescSerializer));
            serializers.put(SYNC_REQUEST, new SyncRequest.Serializer(dbConfig.boundsSerializer, serializer, repairJobDescSerializer));
            serializers.put(SYNC_COMPLETE, new SyncComplete.Serializer(serializer, repairJobDescSerializer));
            serializers.put(ANTICOMPACTION_REQUEST, new AnticompactionRequest.Serializer(serializer));
            serializers.put(PREPARE_MESSAGE, new PrepareMessage.Serializer(serializer, dbConfig.boundsSerializer));
            serializers.put(SNAPSHOT, new SnapshotMessage.Serializer(serializer, repairJobDescSerializer));

            return serializers;
        }
    }

    public final Type messageType;
    public final RepairJobDesc desc;
    protected final Serializer serializer;

    protected RepairMessage(Type messageType, RepairJobDesc desc, Serializer serializer)
    {
        this.messageType = messageType;
        this.desc = desc;
        this.serializer = serializer;
    }

    public MessageOut<RepairMessage> createMessage()
    {
        return new MessageOut<>(MessagingService.Verb.REPAIR_MESSAGE, this, serializer);
    }

    public static class Serializer implements IVersionedSerializer<RepairMessage>
    {
        private final Map<Type, MessageSerializer> serializers;

        public Serializer(DBConfig dbConfig)
        {
            this.serializers = Type.getSerializerMap(this, dbConfig);
        }

        @SuppressWarnings("unchecked")
        public void serialize(RepairMessage message, DataOutputPlus out, int version) throws IOException
        {
            out.write(message.messageType.type);
            MessageSerializer serializer = serializers.get(message.messageType);
            serializer.serialize(message, out, version);
        }

        public RepairMessage deserialize(DataInput in, int version) throws IOException
        {
            RepairMessage.Type messageType = RepairMessage.Type.fromByte(in.readByte());
            MessageSerializer serializer = serializers.get(messageType);
            return (RepairMessage) serializer.deserialize(in, version);
        }

        @SuppressWarnings("unchecked")
        public long serializedSize(RepairMessage message, int version)
        {
            long size = 1; // for messageType byte
            MessageSerializer serializer = serializers.get(message.messageType);
            size += serializer.serializedSize(message, version);
            return size;
        }
    }
}

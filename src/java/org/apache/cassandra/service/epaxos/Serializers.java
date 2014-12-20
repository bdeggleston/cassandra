package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class Serializers
{
    public static final IVersionedSerializer<Set<UUID>> uuidSets = new IVersionedSerializer<Set<UUID>>()
    {
        @Override
        public void serialize(Set<UUID> uuids, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(uuids.size());
            for (UUID id: uuids)
            {
                UUIDSerializer.serializer.serialize(id, out, version);
            }
        }

        @Override
        public Set<UUID> deserialize(DataInput in, int version) throws IOException
        {
            int num = in.readInt();
            Set<UUID> uuids = new HashSet<>(num);
            for (int i=0; i<num; i++)
            {
                uuids.add(UUIDSerializer.serializer.deserialize(in, version));
            }
            return uuids;
        }

        @Override
        public long serializedSize(Set<UUID> uuids, int version)
        {
            long size = 4;
            for (UUID id: uuids)
            {
                size += UUIDSerializer.serializer.serializedSize(id, version);
            }
            return size;
        }
    };
}

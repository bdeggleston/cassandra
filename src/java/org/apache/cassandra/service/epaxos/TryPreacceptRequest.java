package org.apache.cassandra.service.epaxos;

import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;

public class TryPreacceptRequest
{
    public static final IVersionedSerializer<TryPreacceptRequest> serializer = new Serializer();

    public final UUID iid;
    public final Set<UUID> dependencies;

    public TryPreacceptRequest(UUID iid, Set<UUID> dependencies)
    {
        this.iid = iid;
        this.dependencies = dependencies;
    }

    private static class Serializer implements IVersionedSerializer<TryPreacceptRequest>
    {
        @Override
        public void serialize(TryPreacceptRequest request, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(request.iid, out, version);
            out.writeInt(request.dependencies.size());
            for (UUID dep : request.dependencies)
                UUIDSerializer.serializer.serialize(dep, out, version);
        }

        @Override
        public TryPreacceptRequest deserialize(DataInput in, int version) throws IOException
        {
            UUID iid = UUIDSerializer.serializer.deserialize(in, version);

            UUID[] deps = new UUID[in.readInt()];
            for (int i=0; i<deps.length; i++)
                deps[i] = UUIDSerializer.serializer.deserialize(in, version);
            return new TryPreacceptRequest(iid, ImmutableSet.copyOf(deps));
        }

        @Override
        public long serializedSize(TryPreacceptRequest request, int version)
        {
            int size = 0;

            size += UUIDSerializer.serializer.serializedSize(request.iid, version);

            size += 4;  // deps.size
            for (UUID dep : request.dependencies)
                size += UUIDSerializer.serializer.serializedSize(dep, version);

            return size;
        }
    }
}

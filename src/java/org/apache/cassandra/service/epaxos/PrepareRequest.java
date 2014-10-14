package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.util.UUID;

public class PrepareRequest
{
    public static final IVersionedSerializer<PrepareRequest> serializer = new Serializer();

    public final UUID iid;
    public final int ballot;

    public PrepareRequest(Instance instance)
    {
        this(instance.getId(), instance.getBallot());
    }

    public PrepareRequest(UUID iid, int ballot)
    {
        this.iid = iid;
        this.ballot = ballot;
    }

    private static class Serializer implements IVersionedSerializer<PrepareRequest>
    {
        @Override
        public void serialize(PrepareRequest request, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(request.iid, out, version);
            out.writeInt(request.ballot);
        }

        @Override
        public PrepareRequest deserialize(DataInput in, int version) throws IOException
        {
            return new PrepareRequest(UUIDSerializer.serializer.deserialize(in, version), in.readInt());
        }

        @Override
        public long serializedSize(PrepareRequest request, int version)
        {
            return UUIDSerializer.serializer.serializedSize(request.iid, version) + 4;
        }
    }
}

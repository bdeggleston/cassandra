package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.util.UUID;

public class TryPreacceptResponse
{
    public static final IVersionedSerializer<TryPreacceptResponse> serializer = new Serializer();

    public final UUID iid;
    public final boolean success;

    public TryPreacceptResponse(UUID iid, boolean success)
    {
        this.iid = iid;
        this.success = success;
    }

    private static class Serializer implements IVersionedSerializer<TryPreacceptResponse>
    {
        @Override
        public void serialize(TryPreacceptResponse response, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(response.iid, out, version);
            out.writeBoolean(response.success);
        }

        @Override
        public TryPreacceptResponse deserialize(DataInput in, int version) throws IOException
        {
            return new TryPreacceptResponse(
                    UUIDSerializer.serializer.deserialize(in, version),
                    in.readBoolean());
        }

        @Override
        public long serializedSize(TryPreacceptResponse response, int version)
        {
            return UUIDSerializer.serializer.serializedSize(response.iid, version) + 1;
        }
    }
}

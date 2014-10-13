package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.util.UUID;

public class BallotResponse
{
    public static final Serializer serializer = new Serializer();

    public final UUID instanceId;
    public final int ballot;

    public BallotResponse(UUID instanceId, int ballot)
    {
        this.instanceId = instanceId;
        this.ballot = ballot;
    }

    public static class Serializer implements IVersionedSerializer<BallotResponse>
    {
        @Override
        public void serialize(BallotResponse response, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(response.instanceId, out, version);
            out.writeInt(response.ballot);
        }

        @Override
        public BallotResponse deserialize(DataInput in, int version) throws IOException
        {
            return new BallotResponse(UUIDSerializer.serializer.deserialize(in, version),
                                               in.readInt());
        }

        @Override
        public long serializedSize(BallotResponse response, int version)
        {
            return 4 + UUIDSerializer.serializer.serializedSize(response.instanceId, version);
        }
    }
}

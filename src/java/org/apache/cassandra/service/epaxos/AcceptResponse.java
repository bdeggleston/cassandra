package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

import java.io.DataInput;
import java.io.IOException;

public class AcceptResponse
{
    public static final IVersionedSerializer<AcceptResponse> serializer = new Serializer();
    public final boolean success;
    public final int ballot;
    // TODO: add missing instance request if this includes deps the receiving node hasn't seen

    public AcceptResponse(boolean success, int ballot)
    {
        this.success = success;
        this.ballot = ballot;
    }

    public static class Serializer implements IVersionedSerializer<AcceptResponse>
    {
        @Override
        public void serialize(AcceptResponse response, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(response.success);
            out.writeInt(response.ballot);
        }

        @Override
        public AcceptResponse deserialize(DataInput in, int version) throws IOException
        {
            return new AcceptResponse(in.readBoolean(), in.readInt());
        }

        @Override
        public long serializedSize(AcceptResponse response, int version)
        {
            return 1 + 4;
        }
    }
}

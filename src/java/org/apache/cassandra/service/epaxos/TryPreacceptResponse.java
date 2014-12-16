package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.util.UUID;

public class TryPreacceptResponse extends AbstractEpochMessage
{
    public static final IVersionedSerializer<TryPreacceptResponse> serializer = new Serializer();

    public final UUID iid;
    public final TryPreacceptDecision decision;
    public final boolean vetoed;

    public TryPreacceptResponse(Token token, long epoch, UUID iid, TryPreacceptDecision decision, boolean vetoed)
    {
        super(token, epoch);
        this.iid = iid;
        this.decision = decision;
        this.vetoed = vetoed;
    }

    private static class Serializer implements IVersionedSerializer<TryPreacceptResponse>
    {
        @Override
        public void serialize(TryPreacceptResponse response, DataOutputPlus out, int version) throws IOException
        {
            AbstractEpochMessage.serializer.serialize(response, out, version);
            UUIDSerializer.serializer.serialize(response.iid, out, version);
            out.writeInt(response.decision.ordinal());
            out.writeBoolean(response.vetoed);
        }

        @Override
        public TryPreacceptResponse deserialize(DataInput in, int version) throws IOException
        {
            AbstractEpochMessage epochInfo = AbstractEpochMessage.serializer.deserialize(in, version);
            return new TryPreacceptResponse(
                    epochInfo.token,
                    epochInfo.epoch,
                    UUIDSerializer.serializer.deserialize(in, version),
                    TryPreacceptDecision.values()[in.readInt()],
                    in.readBoolean());
        }

        @Override
        public long serializedSize(TryPreacceptResponse response, int version)
        {
            return AbstractEpochMessage.serializer.serializedSize(response, version)
                    + UUIDSerializer.serializer.serializedSize(response.iid, version) + 4 + 1;
        }
    }
}

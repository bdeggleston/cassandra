package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

import java.io.DataInput;
import java.io.IOException;

public class AcceptResponse extends AbstractEpochMessage
{
    public static final IVersionedSerializer<AcceptResponse> serializer = new Serializer();
    public final boolean success;
    public final int ballot;

    public AcceptResponse(Token token, long epoch, boolean success, int ballot)
    {
        super(token, epoch);
        this.success = success;
        this.ballot = ballot;
    }

    public MessageOut<AcceptResponse> getMessage()
    {
        return new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE, this, serializer);
    }

    public static class Serializer implements IVersionedSerializer<AcceptResponse>
    {
        @Override
        public void serialize(AcceptResponse response, DataOutputPlus out, int version) throws IOException
        {
            AbstractEpochMessage.serializer.serialize(response, out, version);
            out.writeBoolean(response.success);
            out.writeInt(response.ballot);
        }

        @Override
        public AcceptResponse deserialize(DataInput in, int version) throws IOException
        {
            AbstractEpochMessage epochInfo = AbstractEpochMessage.serializer.deserialize(in, version);
            return new AcceptResponse(epochInfo.token, epochInfo.epoch, in.readBoolean(), in.readInt());
        }

        @Override
        public long serializedSize(AcceptResponse response, int version)
        {
            return AbstractEpochMessage.serializer.serializedSize(response, version) + 1 + 4;
        }
    }
}

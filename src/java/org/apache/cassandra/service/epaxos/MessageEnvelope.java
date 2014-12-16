package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

import java.io.DataInput;
import java.io.IOException;

/**
 * Sends information used for failure detection
 * along with epaxos messages
 */
public class MessageEnvelope<T> implements IEpochMessage
{
    public final Token token;
    public final long epoch;
    public final T contents;

    public MessageEnvelope(Token token, long epoch, T contents)
    {
        this.token = token;
        this.epoch = epoch;
        this.contents = contents;
    }

    @Override
    public Token getToken()
    {
        return token;
    }

    @Override
    public long getEpoch()
    {
        return epoch;
    }

    public static <T> Serializer<T> getSerializer(IVersionedSerializer<T> payloadSerializer)
    {
        return new Serializer<>(payloadSerializer);
    }

    private static class Serializer<T> implements IVersionedSerializer<MessageEnvelope<T>>
    {
        private final IVersionedSerializer<T> payloadSerializer;

        private Serializer(IVersionedSerializer<T> payloadSerializer)
        {
            this.payloadSerializer = payloadSerializer;
        }

        @Override
        public void serialize(MessageEnvelope<T> envelope, DataOutputPlus out, int version) throws IOException
        {
            Token.serializer.serialize(envelope.token, out);
            out.writeLong(envelope.epoch);

            out.writeBoolean(envelope.contents != null);
            if (envelope.contents != null)
            {
                payloadSerializer.serialize(envelope.contents, out, version);
            }
        }

        @Override
        public MessageEnvelope<T> deserialize(DataInput in, int version) throws IOException
        {
            return new MessageEnvelope<>(Token.serializer.deserialize(in),
                                         in.readLong(),
                                         in.readBoolean() ? payloadSerializer.deserialize(in, version) : null);
        }

        @Override
        public long serializedSize(MessageEnvelope<T> envelope, int version)
        {
            long size = Token.serializer.serializedSize(envelope.token, TypeSizes.NATIVE);
            size += 8;  // envelope.epoch

            size += 1;
            if (envelope.contents != null)
            {
                size += payloadSerializer.serializedSize(envelope.contents, version);
            }
            return size;
        }
    }
}

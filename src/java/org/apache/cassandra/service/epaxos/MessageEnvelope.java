package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

import java.io.DataInput;
import java.io.IOException;

/**
 * Sends information used for failure detection
 * along with epaxos messages
 */
public class MessageEnvelope<T>
{
    public final T payload;

    // the expected epoch for new instances this should
    // consider unexecuted / uncommitted epoch instances
    public final long epoch;

    public MessageEnvelope(T payload, long epoch)
    {
        this.payload = payload;
        this.epoch = epoch;
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
            out.writeBoolean(envelope.payload != null);
            if (envelope.payload != null)
            {
                payloadSerializer.serialize(envelope.payload, out, version);
            }

            out.writeLong(envelope.epoch);
        }

        @Override
        public MessageEnvelope<T> deserialize(DataInput in, int version) throws IOException
        {
            T payload = null;
            if (in.readBoolean())
            {
                payload = payloadSerializer.deserialize(in, version);
            }

            return new MessageEnvelope<>(payload, in.readLong());
        }

        @Override
        public long serializedSize(MessageEnvelope<T> envelope, int version)
        {
            long size = 1;
            if (envelope.payload != null)
            {
                size += payloadSerializer.serializedSize(envelope.payload, version);
            }

            size += 8;  // envelope.epoch
            return size;
        }
    }
}

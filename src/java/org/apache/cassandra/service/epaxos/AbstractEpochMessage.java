package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.util.UUID;

/**
 * Message which contains epoch information
 */
public abstract class AbstractEpochMessage implements IEpochMessage
{
    public final Token token;
    public final UUID cfId;
    public final long epoch;

    public AbstractEpochMessage(Token token, UUID cfId, long epoch)
    {
        this.token = token;
        this.cfId = cfId;
        this.epoch = epoch;
    }

    protected AbstractEpochMessage(AbstractEpochMessage epochInfo)
    {
        this.token = epochInfo.token;
        this.cfId = epochInfo.cfId;
        this.epoch = epochInfo.epoch;
    }

    @Override
    public Token getToken()
    {
        return token;
    }

    @Override
    public UUID getCfId()
    {
        return cfId;
    }

    @Override
    public long getEpoch()
    {
        return epoch;
    }

    private static class EpochInfo extends AbstractEpochMessage
    {
        public EpochInfo(Token token, UUID cfId, long epoch)
        {
            super(token, cfId, epoch);
        }
    }

    protected static IVersionedSerializer<AbstractEpochMessage> serializer = new IVersionedSerializer<AbstractEpochMessage>()
    {
        @Override
        public void serialize(AbstractEpochMessage msg, DataOutputPlus out, int version) throws IOException
        {
            Token.serializer.serialize(msg.token, out);
            UUIDSerializer.serializer.serialize(msg.cfId, out, version);
            out.writeLong(msg.epoch);
        }

        @Override
        public AbstractEpochMessage deserialize(DataInput in, int version) throws IOException
        {
            return new EpochInfo(Token.serializer.deserialize(in),
                                 UUIDSerializer.serializer.deserialize(in, version),
                                 in.readLong());
        }

        @Override
        public long serializedSize(AbstractEpochMessage msg, int version)
        {
            long size = Token.serializer.serializedSize(msg.token, TypeSizes.NATIVE);
            size += UUIDSerializer.serializer.serializedSize(msg.cfId, version);
            size += 8;  // response.epoch
            return size;
        }
    };
}

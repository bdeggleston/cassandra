package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

import java.io.DataInput;
import java.io.IOException;

/**
 * Message which contains epoch information
 */
public abstract class AbstractEpochMessage implements IEpochMessage
{
    public final Token token;
    public final long epoch;

    public AbstractEpochMessage(Token token, long epoch)
    {
        this.token = token;
        this.epoch = epoch;
    }

    protected AbstractEpochMessage(AbstractEpochMessage epochInfo)
    {
        this.token = epochInfo.token;
        this.epoch = epochInfo.epoch;
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

    private static class EpochInfo extends AbstractEpochMessage
    {
        public EpochInfo(Token token, long epoch)
        {
            super(token, epoch);
        }
    }

    protected static IVersionedSerializer<AbstractEpochMessage> serializer = new IVersionedSerializer<AbstractEpochMessage>()
    {
        @Override
        public void serialize(AbstractEpochMessage msg, DataOutputPlus out, int version) throws IOException
        {
            Token.serializer.serialize(msg.token, out);
            out.writeLong(msg.epoch);
        }

        @Override
        public AbstractEpochMessage deserialize(DataInput in, int version) throws IOException
        {
            return new EpochInfo(Token.serializer.deserialize(in), in.readLong());
        }

        @Override
        public long serializedSize(AbstractEpochMessage msg, int version)
        {
            long size = Token.serializer.serializedSize(msg.token, TypeSizes.NATIVE);
            size += 8;  // response.epoch
            return size;
        }
    };
}

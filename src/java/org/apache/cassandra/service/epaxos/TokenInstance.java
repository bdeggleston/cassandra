package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;

public class TokenInstance extends Instance
{
    private final Token token;
    private final long epoch;

    public TokenInstance(InetAddress leader, Token token, long epoch)
    {
        super(leader);
        this.token = token;
        this.epoch = epoch;
    }

    public TokenInstance(UUID id, InetAddress leader, Token token, long epoch)
    {
        super(id, leader);
        this.token = token;
        this.epoch = epoch;
    }

    public TokenInstance(TokenInstance i)
    {
        super(i);
        this.token = i.token;
        this.epoch = i.epoch;
    }

    @Override
    public Instance copy()
    {
        return new TokenInstance(this);
    }

    @Override
    public Instance copyRemote()
    {
        Instance instance = new TokenInstance(this.id, this.leader, this.token, this.epoch);
        instance.ballot = ballot;
        instance.noop = noop;
        instance.successors = successors;
        instance.state = state;
        instance.dependencies = dependencies;
        return instance;
    }

    public Token getToken()
    {
        return token;
    }

    public long getEpoch()
    {
        return epoch;
    }

    @Override
    public Type getType()
    {
        return Type.TOKEN;
    }

    private static final IVersionedSerializer<TokenInstance> commonSerializer = new IVersionedSerializer<TokenInstance>()
    {
        @Override
        public void serialize(TokenInstance instance, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(instance.getId(), out, version);
            CompactEndpointSerializationHelper.serialize(instance.getLeader(), out);
            Token.serializer.serialize(instance.token, out);
            out.writeLong(instance.epoch);
        }

        @Override
        public TokenInstance deserialize(DataInput in, int version) throws IOException
        {
            return new TokenInstance(UUIDSerializer.serializer.deserialize(in, version),
                                     CompactEndpointSerializationHelper.deserialize(in),
                                     Token.serializer.deserialize(in),
                                     in.readLong());
        }

        @Override
        public long serializedSize(TokenInstance instance, int version)
        {
            long size = 0;
            size += UUIDSerializer.serializer.serializedSize(instance.getId(), version);
            size += CompactEndpointSerializationHelper.serializedSize(instance.getLeader());
            size += Token.serializer.serializedSize(instance.token, TypeSizes.NATIVE);
            size += 8;
            return size;
        }
    };

    public static final IVersionedSerializer<Instance> serializer = new IVersionedSerializer<Instance>()
    {
        private final ExternalSerializer baseSerializer = new ExternalSerializer();

        @Override
        public void serialize(Instance instance, DataOutputPlus out, int version) throws IOException
        {
            assert instance instanceof TokenInstance;
            commonSerializer.serialize((TokenInstance) instance, out, version);
            baseSerializer.serialize(instance, out, version);
        }

        @Override
        public Instance deserialize(DataInput in, int version) throws IOException
        {
            Instance instance = commonSerializer.deserialize(in, version);
            baseSerializer.deserialize(instance, in, version);
            return instance;
        }

        @Override
        public long serializedSize(Instance instance, int version)
        {
            assert instance instanceof TokenInstance;
            return commonSerializer.serializedSize((TokenInstance) instance, version)
                    + baseSerializer.serializedSize(instance, version);
        }
    };

    public static final IVersionedSerializer<Instance> internalSerializer = new IVersionedSerializer<Instance>()
    {
        private final InternalSerializer baseSerializer = new InternalSerializer();

        @Override
        public void serialize(Instance instance, DataOutputPlus out, int version) throws IOException
        {
            assert instance instanceof TokenInstance;
            commonSerializer.serialize((TokenInstance) instance, out, version);
            baseSerializer.serialize(instance, out, version);
        }

        @Override
        public Instance deserialize(DataInput in, int version) throws IOException
        {
            Instance instance = commonSerializer.deserialize(in, version);
            baseSerializer.deserialize(instance, in, version);
            return instance;
        }

        @Override
        public long serializedSize(Instance instance, int version)
        {
            assert instance instanceof TokenInstance;
            return commonSerializer.serializedSize((TokenInstance) instance, version)
                    + baseSerializer.serializedSize(instance, version);
        }
    };
}

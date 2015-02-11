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

/**
 * Adds new tokens to the token state manager
 */
public class TokenInstance extends AbstractTokenInstance
{
    public TokenInstance(InetAddress leader, UUID cfId, Token token)
    {
        super(leader, cfId, token);
    }

    public TokenInstance(UUID id, InetAddress leader, UUID cfId, Token token)
    {
        super(id, leader, cfId, token);
    }

    public TokenInstance(AbstractTokenInstance i)
    {
        super(i);
    }

    @Override
    public Instance copy()
    {
        return new TokenInstance(this);
    }

    @Override
    public Instance copyRemote()
    {
        return copy();
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
            UUIDSerializer.serializer.serialize(instance.cfId, out, version);
            Token.serializer.serialize(instance.token, out);
        }

        @Override
        public TokenInstance deserialize(DataInput in, int version) throws IOException
        {
            TokenInstance instance = new TokenInstance(UUIDSerializer.serializer.deserialize(in, version),
                                                       CompactEndpointSerializationHelper.deserialize(in),
                                                       UUIDSerializer.serializer.deserialize(in, version),
                                                       Token.serializer.deserialize(in));

            return instance;
        }

        @Override
        public long serializedSize(TokenInstance instance, int version)
        {
            long size = 0;
            size += UUIDSerializer.serializer.serializedSize(instance.getId(), version);
            size += CompactEndpointSerializationHelper.serializedSize(instance.getLeader());
            size += UUIDSerializer.serializer.serializedSize(instance.cfId, version);
            size += Token.serializer.serializedSize(instance.token, TypeSizes.NATIVE);
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

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
 * Dependency management, we may need to keep 3 epochs around. If an instance forms a
 * strongly connected component with an epoch increment instance, and there's no other
 * activity on that partition, the *next* epoch increment instance will also depend on it
 */
// TODO: rename to EpochInstance
public class TokenInstance extends Instance
{
    private final Token token;
    private final UUID cfId;
    private final long epoch;
    private volatile boolean vetoed;

    public TokenInstance(InetAddress leader, Token token, UUID cfId, long epoch)
    {
        super(leader);
        this.token = token;
        this.cfId = cfId;
        this.epoch = epoch;
    }

    public TokenInstance(UUID id, InetAddress leader, Token token, UUID cfId, long epoch)
    {
        super(id, leader);
        this.token = token;
        this.cfId = cfId;
        this.epoch = epoch;
    }

    public TokenInstance(TokenInstance i)
    {
        super(i);
        this.token = i.token;
        this.cfId = i.cfId;
        this.epoch = i.epoch;
        this.vetoed = i.vetoed;
    }

    @Override
    public Instance copy()
    {
        return new TokenInstance(this);
    }

    @Override
    public Instance copyRemote()
    {
        Instance instance = new TokenInstance(this.id, this.leader, this.token, this.cfId, this.epoch);
        instance.ballot = ballot;
        instance.noop = noop;
        instance.successors = successors;
        instance.state = state;
        instance.dependencies = dependencies;
        return instance;
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

    public long getEpoch()
    {
        return epoch;
    }

    @Override
    public Type getType()
    {
        return Type.TOKEN;
    }

    @Override
    public boolean getLeaderAttrsMatch()
    {
        return super.getLeaderAttrsMatch() && !isVetoed();
    }

    public boolean isVetoed()
    {
        return vetoed;
    }

    public void setVetoed(boolean vetoed)
    {
        this.vetoed = vetoed;
    }

    @Override
    public boolean skipExecution()
    {
        return super.skipExecution() || vetoed;
    }

    @Override
    public void applyRemote(Instance remote)
    {
        assert remote instanceof TokenInstance;
        super.applyRemote(remote);
        this.vetoed = ((TokenInstance) remote).vetoed;
    }

    private static final IVersionedSerializer<TokenInstance> commonSerializer = new IVersionedSerializer<TokenInstance>()
    {
        @Override
        public void serialize(TokenInstance instance, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(instance.getId(), out, version);
            CompactEndpointSerializationHelper.serialize(instance.getLeader(), out);
            Token.serializer.serialize(instance.token, out);
            UUIDSerializer.serializer.serialize(instance.cfId, out, version);
            out.writeLong(instance.epoch);
            out.writeBoolean(instance.vetoed);
        }

        @Override
        public TokenInstance deserialize(DataInput in, int version) throws IOException
        {
            TokenInstance instance = new TokenInstance(UUIDSerializer.serializer.deserialize(in, version),
                                                       CompactEndpointSerializationHelper.deserialize(in),
                                                       Token.serializer.deserialize(in),
                                                       UUIDSerializer.serializer.deserialize(in, version),
                                                       in.readLong());

            instance.vetoed = in.readBoolean();
            return instance;
        }

        @Override
        public long serializedSize(TokenInstance instance, int version)
        {
            long size = 0;
            size += UUIDSerializer.serializer.serializedSize(instance.getId(), version);
            size += CompactEndpointSerializationHelper.serializedSize(instance.getLeader());
            size += Token.serializer.serializedSize(instance.token, TypeSizes.NATIVE);
            size += UUIDSerializer.serializer.serializedSize(instance.cfId, version);
            size += 8;
            size += 1;
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

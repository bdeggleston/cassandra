package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Sets;

/**
 * Adds new tokens to the token state manager
 */
public class TokenInstance extends AbstractTokenInstance
{
    // the token range this instance is splitting
    // this is important because the range this instance
    // end up splitting may be different than the range
    // that exists when it's created
    private volatile Range<Token> splitRange;
    private volatile boolean leaderRangeMatched = true;

    public TokenInstance(InetAddress leader, UUID cfId, Token token, Range<Token> splitRange, boolean local)
    {
        super(leader, cfId, token, local);
        this.splitRange = splitRange;
    }

    public TokenInstance(UUID id, InetAddress leader, UUID cfId, Token token, Range<Token> splitRange, boolean local)
    {
        super(id, leader, cfId, token, local);
        this.splitRange = splitRange;
    }

    public TokenInstance(TokenInstance i)
    {
        super(i);
        this.splitRange = i.splitRange;
    }

    public Range<Token> getSplitRange()
    {
        return splitRange;
    }

    public void setSplitRange(Range<Token> splitRange)
    {
        this.splitRange = splitRange;
    }

    static Range<Token> mergeRanges(Range<Token> r1, Range<Token> r2)
    {
        if (!r1.intersects(r2))
        {
            throw new AssertionError(String.format("Ranges %s and %s do not intersect", r1, r2));
        }

        if (r1.equals(r2))
        {
            return r1;
        }

        boolean rewrap = r1.unwrap().size() > 1 || r2.unwrap().size() > 1;

        List<Range<Token>> normalized = Range.normalize(Sets.newHashSet(r1, r2));

        if (rewrap)
        {
            assert normalized.size() == 2;
            Range<Token> rangeRight = normalized.get(0);
            Range<Token> rangeLeft = normalized.get(1);

            Token minToken = DatabaseDescriptor.getPartitioner().minValue(r1.right.getClass());
            assert rangeLeft.right.equals(minToken);
            assert rangeRight.left.equals(minToken);
            return new Range<>(rangeLeft.left, rangeRight.right);
        }
        else
        {
            assert normalized.size() == 1;
            return normalized.get(0);
        }
    }

    /**
     * used during preaccept
     */
    public void mergeLocalSplitRange(Range<Token> range)
    {
        leaderRangeMatched = range.equals(splitRange);
        splitRange = mergeRanges(splitRange, range);
    }

    @Override
    public boolean getLeaderAttrsMatch()
    {
        return super.getLeaderAttrsMatch() && leaderRangeMatched;
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
            Token.serializer.serialize(instance.splitRange.left, out);
            Token.serializer.serialize(instance.splitRange.right, out);
            out.writeBoolean(instance.local);
        }

        @Override
        public TokenInstance deserialize(DataInput in, int version) throws IOException
        {
            TokenInstance instance = new TokenInstance(UUIDSerializer.serializer.deserialize(in, version),
                                                       CompactEndpointSerializationHelper.deserialize(in),
                                                       UUIDSerializer.serializer.deserialize(in, version),
                                                       Token.serializer.deserialize(in),
                                                       new Range<>(Token.serializer.deserialize(in), Token.serializer.deserialize(in)),
                                                       in.readBoolean());

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
            size += Token.serializer.serializedSize(instance.splitRange.left, TypeSizes.NATIVE);
            size += Token.serializer.serializedSize(instance.splitRange.right, TypeSizes.NATIVE);
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

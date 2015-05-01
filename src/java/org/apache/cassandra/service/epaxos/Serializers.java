package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class Serializers
{
    public static final IVersionedSerializer<Set<UUID>> uuidSets = new IVersionedSerializer<Set<UUID>>()
    {
        @Override
        public void serialize(Set<UUID> uuids, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(uuids != null);
            if (uuids == null)
                return;

            out.writeInt(uuids.size());
            for (UUID id: uuids)
            {
                UUIDSerializer.serializer.serialize(id, out, version);
            }
        }

        @Override
        public Set<UUID> deserialize(DataInput in, int version) throws IOException
        {
            if (!in.readBoolean())
                return null;
            int num = in.readInt();
            Set<UUID> uuids = new HashSet<>(num);
            for (int i=0; i<num; i++)
            {
                uuids.add(UUIDSerializer.serializer.deserialize(in, version));
            }
            return uuids;
        }

        @Override
        public long serializedSize(Set<UUID> uuids, int version)
        {
            if (uuids == null)
                return 1;

            long size = 4 + 1;
            for (UUID id: uuids)
            {
                size += UUIDSerializer.serializer.serializedSize(id, version);
            }
            return size;
        }
    };

    public static final IVersionedSerializer<Range<Token>> tokenRange = new IVersionedSerializer<Range<Token>>()
    {
        @Override
        public void serialize(Range<Token> range, DataOutputPlus out, int version) throws IOException
        {
            Token.serializer.serialize(range.left, out);
            Token.serializer.serialize(range.right, out);
        }

        @Override
        public Range<Token> deserialize(DataInput in, int version) throws IOException
        {
            return new Range<Token>(Token.serializer.deserialize(in), Token.serializer.deserialize(in));
        }

        @Override
        public long serializedSize(Range<Token> range, int version)
        {
            return Token.serializer.serializedSize(range.left, TypeSizes.NATIVE) +
                   Token.serializer.serializedSize(range.left, TypeSizes.NATIVE);
        }
    };

    public static final IVersionedSerializer<Range<Token>> nullableTokenRange = new IVersionedSerializer<Range<Token>>()
    {
        @Override
        public void serialize(Range<Token> range, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(range != null);
            if (range != null)
            {
                tokenRange.serialize(range, out, version);
            }
        }

        @Override
        public Range<Token> deserialize(DataInput in, int version) throws IOException
        {
            return in.readBoolean() ? tokenRange.deserialize(in, version) : null;
        }

        @Override
        public long serializedSize(Range<Token> range, int version)
        {
            return 1 + (range != null ? tokenRange.serializedSize(range, version) : 0);
        }
    };
}

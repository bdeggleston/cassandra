package org.apache.cassandra.streaming;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.util.UUID;

public class EpaxosSummary
{
    public final UUID taskId;
    public final UUID cfId;
    public final Range<Token> range;

    public EpaxosSummary(UUID taskId, UUID cfId, Range<Token> range)
    {
        this.taskId = taskId;
        this.cfId = cfId;
        this.range = range;
    }

    public static final IVersionedSerializer<EpaxosSummary> serializer = new IVersionedSerializer<EpaxosSummary>()
    {
        @Override
        public void serialize(EpaxosSummary request, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(request.taskId, out, version);
            UUIDSerializer.serializer.serialize(request.cfId, out, version);
            Token.serializer.serialize(request.range.left, out);
            Token.serializer.serialize(request.range.right, out);
        }

        @Override
        public EpaxosSummary deserialize(DataInput in, int version) throws IOException
        {
            return new EpaxosSummary(UUIDSerializer.serializer.deserialize(in, version),
                                             UUIDSerializer.serializer.deserialize(in, version),
                                             new Range<>(Token.serializer.deserialize(in), Token.serializer.deserialize(in)));
        }

        @Override
        public long serializedSize(EpaxosSummary request, int version)
        {
            long size = 0;
            size += UUIDSerializer.serializer.serializedSize(request.taskId, version);
            size += UUIDSerializer.serializer.serializedSize(request.cfId, version);
            size += Token.serializer.serializedSize(request.range.left, TypeSizes.NATIVE);
            size += Token.serializer.serializedSize(request.range.right, TypeSizes.NATIVE);
            return size;
        }
    };
}

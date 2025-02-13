package org.apache.cassandra.db;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;

import java.io.IOException;
import java.util.Objects;

/**
 * range of mutation ids contained by sstable/stream/memtable
 *
 * currently just min max, but should expand to contain per-coordinator/shard id ranges
 */
public class MutationIdMetadata
{
    // TODO: check usages in tests and replace with generated values
    public static final MutationIdMetadata NONE = new MutationIdMetadata(MutationId.none(), MutationId.none());
    public final MutationId minId;
    public final MutationId maxId;

    public MutationIdMetadata(MutationId minId, MutationId maxId)
    {
        this.minId = minId;
        this.maxId = maxId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        MutationIdMetadata that = (MutationIdMetadata) o;
        return Objects.equals(minId, that.minId) && Objects.equals(maxId, that.maxId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(minId, maxId);
    }

    public MutationIdMetadata merge(MutationIdMetadata that)
    {
        if (this == NONE)
            return that;
        if (that == NONE)
            return this;
        return new MutationIdMetadata(MutationId.minNotNone(this.minId, that.minId), MutationId.max(this.maxId, that.maxId));
    }

    public static MutationIdMetadata merge(MutationIdMetadata l, MutationIdMetadata r)
    {
        return new MutationIdMetadata(MutationId.minNotNone(l.minId, r.minId), MutationId.max(l.maxId, r.maxId));
    }

    public static MutationIdMetadata fixme()
    {
        throw new RuntimeException("TODO");
    }

    public static final IVersionedSerializer<MutationIdMetadata> serializer = new IVersionedSerializer<MutationIdMetadata>()
    {
        @Override
        public void serialize(MutationIdMetadata metadata, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_52)
                return;
            MutationId.serializer.serialize(metadata.minId, out, version);
            MutationId.serializer.serialize(metadata.maxId, out, version);
        }

        @Override
        public MutationIdMetadata deserialize(DataInputPlus in, int version) throws IOException
        {
            if (version < MessagingService.VERSION_52)
                return MutationIdMetadata.NONE;
            return new MutationIdMetadata(MutationId.serializer.deserialize(in, version),
                                          MutationId.serializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(MutationIdMetadata metadata, int version)
        {
            if (version < MessagingService.VERSION_52)
                return 0;
            return MutationId.serializer.serializedSize(metadata.minId, version)
                    + MutationId.serializer.serializedSize(metadata.maxId, version);
        }
    };
}

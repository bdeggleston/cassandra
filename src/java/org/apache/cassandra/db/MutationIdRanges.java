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
public class MutationIdRanges
{
    public static final MutationIdRanges NONE = new MutationIdRanges(MutationId.none(), MutationId.none());
    public final MutationId minId;
    public final MutationId maxId;

    public MutationIdRanges(MutationId minId, MutationId maxId)
    {
        this.minId = minId;
        this.maxId = maxId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        MutationIdRanges that = (MutationIdRanges) o;
        return Objects.equals(minId, that.minId) && Objects.equals(maxId, that.maxId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(minId, maxId);
    }

    public MutationIdRanges merge(MutationIdRanges that)
    {
        if (this == NONE)
            return that;
        if (that == NONE)
            return this;
        return new MutationIdRanges(MutationId.minNotNone(this.minId, that.minId), MutationId.max(this.maxId, that.maxId));
    }

    public MutationIdRanges subset(PartitionPosition from, PartitionPosition to)
    {
        return this;
    }

    public static MutationIdRanges merge(MutationIdRanges l, MutationIdRanges r)
    {
        return new MutationIdRanges(MutationId.minNotNone(l.minId, r.minId), MutationId.max(l.maxId, r.maxId));
    }

    public MutationIdRanges add(MutationId mutationId)
    {
        return new MutationIdRanges(MutationId.minNotNone(minId, mutationId), MutationId.max(maxId, mutationId));
    }

    public static MutationIdRanges fixme()
    {
        throw new RuntimeException("TODO");
    }

    public static final IVersionedSerializer<MutationIdRanges> serializer = new IVersionedSerializer<MutationIdRanges>()
    {
        @Override
        public void serialize(MutationIdRanges metadata, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_52)
                return;
            MutationId.serializer.serialize(metadata.minId, out, version);
            MutationId.serializer.serialize(metadata.maxId, out, version);
        }

        @Override
        public MutationIdRanges deserialize(DataInputPlus in, int version) throws IOException
        {
            if (version < MessagingService.VERSION_52)
                return MutationIdRanges.NONE;
            return new MutationIdRanges(MutationId.serializer.deserialize(in, version),
                                        MutationId.serializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(MutationIdRanges metadata, int version)
        {
            if (version < MessagingService.VERSION_52)
                return 0;
            return MutationId.serializer.serializedSize(metadata.minId, version)
                    + MutationId.serializer.serializedSize(metadata.maxId, version);
        }
    };
}

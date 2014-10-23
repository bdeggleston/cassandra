package org.apache.cassandra.service.epaxos;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.util.*;

/**
 * What if
 */
public class DependencyManager
{
    // TODO: add mechanism for old, implicitly added, entries to fall off over time
    static class Entry
    {
        private final UUID iid;

        boolean acknowledged = false;
        boolean executed = false;

        private Entry(UUID iid)
        {
            this.iid = iid;
        }

        private Entry(UUID iid, boolean acknowledged, boolean executed)
        {
            this.iid = iid;
            this.acknowledged = acknowledged;
            this.executed = executed;
        }

        private boolean canEvict()
        {
            return acknowledged && executed;
        }

        private static final IVersionedSerializer<Entry> serializer = new IVersionedSerializer<Entry>()
        {
            @Override
            public void serialize(Entry entry, DataOutputPlus out, int version) throws IOException
            {
                UUIDSerializer.serializer.serialize(entry.iid, out, version);
                out.writeBoolean(entry.acknowledged);
                out.writeBoolean(entry.executed);
            }

            @Override
            public Entry deserialize(DataInput in, int version) throws IOException
            {
                return new Entry(
                        UUIDSerializer.serializer.deserialize(in, version),
                        in.readBoolean(),
                        in.readBoolean());
            }

            @Override
            public long serializedSize(Entry entry, int version)
            {
                return UUIDSerializer.serializer.serializedSize(entry.iid, version) + 1 + 1 + 1;
            }
        };

    }

    private final Map<UUID, Entry> entries = new HashMap<>();

    @VisibleForTesting
    Entry get(UUID iid)
    {
        return entries.get(iid);
    }

    @VisibleForTesting
    Entry create(UUID iid)
    {
        Entry entry = new Entry(iid);
        entries.put(iid, entry);
        return entry;
    }

    private void maybeEvict(UUID iid)
    {
        Entry entry = entries.get(iid);
        if (entry == null)
            return;

        if (entry.canEvict())
            entries.remove(iid);
    }

    public Entry recordInstance(UUID iid)
    {
        return create(iid);
    }

    public Set<UUID> getDepsAndAdd(UUID iid)
    {
        Set<UUID> deps = new HashSet<>(entries.size());
        for (UUID depId: entries.keySet())
        {
            if (depId.equals(iid))
                continue;

            deps.add(depId);
        }
        for (UUID depId: deps)
        {
            maybeEvict(depId);
        }
        recordInstance(iid);
        return deps;
    }

    public void markAcknowledged(UUID iid)
    {
        Entry entry = get(iid);
        if (entry == null)
            return;
        entry.acknowledged = true;
    }

    public void markAcknowledged(Set<UUID> iids)
    {
        for (UUID iid: iids)
        {
            markAcknowledged(iid);
        }
    }

    public void markAcknowledged(Set<UUID> iids, UUID exclude)
    {
        for (UUID iid: iids)
        {
            if (iid.equals(exclude))
                continue;
            markAcknowledged(iid);
        }
    }

    public void markExecuted(UUID iid)
    {
        Entry entry = get(iid);
        if (entry == null)
            return;
        entry.executed = true;
    }

    public static final IVersionedSerializer<DependencyManager> serializer = new IVersionedSerializer<DependencyManager>()
    {
        @Override
        public void serialize(DependencyManager deps, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(deps.entries.size());
            for (Entry entry: deps.entries.values())
            {
                Entry.serializer.serialize(entry, out, version);
            }
        }

        @Override
        public DependencyManager deserialize(DataInput in, int version) throws IOException
        {
            DependencyManager deps = new DependencyManager();
            int size = in.readInt();
            for (int i=0; i<size; i++)
            {
                Entry entry = Entry.serializer.deserialize(in, version);
                deps.entries.put(entry.iid, entry);
            }
            return deps;
        }

        @Override
        public long serializedSize(DependencyManager deps, int version)
        {
            long size = 4;
            for (Entry entry: deps.entries.values())
                size += Entry.serializer.serializedSize(entry, version);
            return size;
        }
    };
}

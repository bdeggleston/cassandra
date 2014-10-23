package org.apache.cassandra.service.epaxos;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

import javax.annotation.Nullable;
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
        boolean addedExplicitly = false;

        private Entry(UUID iid)
        {
            this.iid = iid;
        }

        private Entry(UUID iid, boolean acknowledged, boolean executed, boolean addedExplicitly)
        {
            this.iid = iid;
            this.acknowledged = acknowledged;
            this.executed = executed;
            this.addedExplicitly = addedExplicitly;
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
                out.writeBoolean(entry.addedExplicitly);
            }

            @Override
            public Entry deserialize(DataInput in, int version) throws IOException
            {
                return new Entry(
                        UUIDSerializer.serializer.deserialize(in, version),
                        in.readBoolean(),
                        in.readBoolean(),
                        in.readBoolean());
            }

            @Override
            public long serializedSize(Entry entry, int version)
            {
                return UUIDSerializer.serializer.serializedSize(entry.iid, version) + 1 + 1 + 1;
            }
        };

        private static Predicate<Entry> explicitPredicate = new Predicate<Entry>()
        {
            @Override
            public boolean apply(@Nullable Entry entry)
            {
                return entry != null & entry.addedExplicitly;
            }
        };

        private static Function<Entry, UUID> getUUID = new Function<Entry, UUID>()
        {
            @Override
            public UUID apply(Entry entry)
            {
                return entry.iid;
            }
        };

    }

    private final Map<UUID, Entry> entries = new HashMap<>();

    @VisibleForTesting
    Entry getOrCreate(UUID iid)
    {
        Entry entry = entries.get(iid);
        if (entry == null)
        {
            entry = new Entry(iid);
            entries.put(iid, entry);
        }
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

    private Entry recordInstance(Instance instance)
    {
        UUID iid = instance.getId();
        Entry entry = getOrCreate(iid);
        entry.executed = instance.getState() == Instance.State.EXECUTED;
        entry.addedExplicitly = true;

        maybeEvict(iid);
        return entry;
    }

    @VisibleForTesting
    Set<UUID> getDeps()
    {
        // filter out entries that were acknowledged, but weren't explicitly added
        // this prevents executed + ack'd deps from coming back from the dead and becoming
        // dependencies on new instances
        return Sets.newHashSet(Iterables.transform(Iterables.filter(entries.values(), Entry.explicitPredicate), Entry.getUUID));
    }

    public Set<UUID> getDepsAndAdd(Instance instance)
    {
        Set<UUID> deps = new HashSet<>(entries.keySet());
        deps.remove(instance.getId());
        recordInstance(instance);
        return deps;
    }

    public void markAcknowledged(Set<UUID> iids)
    {
        for (UUID iid: iids)
        {
            Entry entry = getOrCreate(iid);
            entry.acknowledged = true;
            maybeEvict(iid);
        }
    }

    public void markExecuted(UUID iid)
    {
        Entry entry = getOrCreate(iid);
        entry.executed = true;
        maybeEvict(iid);
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

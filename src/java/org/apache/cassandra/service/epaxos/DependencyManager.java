package org.apache.cassandra.service.epaxos;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.util.*;

/**
 * Maintains the ids of 'active' instances, for determining dependencies during the preaccept phase.
 *
 * Since instances tend to leave the 'active' state in the same order they were created, modeling this
 * as a cql table basically creates a queue, and all the performance problems that come along with them,
 * so they are serialized as a big blob which is written out each time the dependencies change.
 *
 * An instance will remain in a partition's dependency manager until it's been both executed, and
 * acknowledged by other nodes in the cluster. An instance is considered acknowledged when it's a
 * dependency of another instance, which has been accepted or committed. This prevents situations where
 * a dependency graph becomes 'split', making the execution order ambiguous.
 */
// TODO: rename to KeyState, rename system table too
// TODO: move some functionality from EpaxosState to KeyStateManager
public class DependencyManager
{
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

    // active instances - instances that still need to be taken
    // as a dependency at least once
    private final Map<UUID, Entry> entries = new HashMap<>();

    // ids of executed instances, divided into execution epochs:w
    private final Map<Long, Set<UUID>> epochs = new HashMap<>();

    private long epoch;
    private long executionCount;

    public DependencyManager(long epoch)
    {
        this(epoch, 0);
    }

    public DependencyManager(long epoch, long executionCount)
    {
        this.epoch = epoch;
        this.executionCount = executionCount;
    }

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

    public Set<UUID> getDeps()
    {
        // TODO: make defensive copy?
        return entries.keySet();
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

    // TODO: can we evict if instance has already been acknowledged?
    public void markExecuted(UUID iid)
    {
        Entry entry = get(iid);
        if (entry != null)
        {
            entry.executed = true;
        }
         /*
         we can't evict even if the instance has been acknowledged.
         If the instance is part of a strongly connected component,
         they could all acknowledge each other, and would terminate
         the execution chain.
          */

        Set<UUID> epochSet = epochs.get(epoch);
        if (epochSet == null)
        {
            epochSet = new HashSet<>();
            epochs.put(epoch, epochSet);
        }
        if (!epochSet.contains(iid))
        {
            executionCount++;
            epochSet.add(iid);
        }
    }

    public long getEpoch()
    {
        return epoch;
    }

    @VisibleForTesting
    Map<Long, Set<UUID>> getEpochExecutions()
    {
        return epochs;
    }

    public void setEpoch(long epoch)
    {
        if (epoch < this.epoch)
        {
            throw new RuntimeException(String.format("Cannot decrement epoch. %s -> %s", this.epoch, epoch));
        }
        else if (epoch == this.epoch)
        {
            return;
        }
        this.epoch = epoch;
        executionCount = 0;
    }

    public long getExecutionCount()
    {
        return executionCount;
    }

    public static final IVersionedSerializer<DependencyManager> serializer = new IVersionedSerializer<DependencyManager>()
    {
        @Override
        public void serialize(DependencyManager deps, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(deps.epoch);
            out.writeLong(deps.executionCount);

            out.writeInt(deps.entries.size());
            for (Entry entry: deps.entries.values())
            {
                Entry.serializer.serialize(entry, out, version);
            }

            out.writeInt(deps.epochs.size());
            for (Map.Entry<Long, Set<UUID>> entry: deps.epochs.entrySet())
            {
                out.writeLong(entry.getKey());
                Set<UUID> ids = entry.getValue();
                out.writeInt(ids.size());
                for (UUID id: ids)
                {
                    UUIDSerializer.serializer.serialize(id, out, version);
                }
            }
        }

        @Override
        public DependencyManager deserialize(DataInput in, int version) throws IOException
        {
            DependencyManager deps = new DependencyManager(in.readLong(), in.readLong());
            int size = in.readInt();
            for (int i=0; i<size; i++)
            {
                Entry entry = Entry.serializer.deserialize(in, version);
                deps.entries.put(entry.iid, entry);
            }

            int numEpochs = in.readInt();
            for (int i=0; i<numEpochs; i++)
            {
                long epoch = in.readLong();
                Set<UUID> ids = new HashSet<>();
                int numIds = in.readInt();
                for (int j=0; j<numIds; j++)
                {
                    ids.add(UUIDSerializer.serializer.deserialize(in, version));
                }
                deps.epochs.put(epoch, ids);
            }
            return deps;
        }

        @Override
        public long serializedSize(DependencyManager deps, int version)
        {
            long size = 8 + 8 + 4;
            for (Entry entry: deps.entries.values())
                size += Entry.serializer.serializedSize(entry, version);

            size += 4; //out.writeInt(deps.epochs.size());
            for (Map.Entry<Long, Set<UUID>> entry: deps.epochs.entrySet())
            {
                size += 8; //out.writeLong(entry.getKey());
                Set<UUID> ids = entry.getValue();
                size += 4; //out.writeInt(ids.size());
                for (UUID id: ids)
                {
                    size += UUIDSerializer.serializer.serializedSize(id, version);
                }
            }
            return size;
        }
    };
}

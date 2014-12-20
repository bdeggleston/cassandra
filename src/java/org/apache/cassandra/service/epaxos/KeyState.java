package org.apache.cassandra.service.epaxos;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.util.*;

/**
 * Maintains the ids of 'active' instances for a given key, for determining dependencies during the
 * preaccept phase.
 *
 * Since instances tend to leave the 'active' state in the same order they were created, modeling this
 * as a cql table basically creates a queue, and all the performance problems that come along with them,
 * so they are serialized as a big blob which is written out each time the dependencies change.
 *
 * An instance will remain in a keys's set of active instances until it's been both  acknowledged by other
 * nodes in the cluster. An instance is considered acknowledged when it's a dependency of another instance,
 * which has been accepted or committed. Doing this prevents the dependency graph from becoming 'split',
 * makeing the execution order ambiguous.
 *
 * A special case is strongly connected components in the dependency graph. Since every instance in a
 * strongly connected component will trigger an acknowledgement of every other instance in that component,
 * allowing the instance of strongly connected components to acknowledge other instances will basically end
 * the execution chain, causing ambiguous execution ordering.
 *
 * To prevent the problems created by strongly connected components, acknowledgements from other instances
 * in the component are allowed to evict instances in the component, with the exception of the last instance
 * in the sorted instances. This instance must be acknowledged by an instance outside of the component.
 *
 * A side effect of needing to know what's in a strongly connected component is that instances must be
 * executed themselves before they can be evicted.
 */
public class KeyState
{
    static class Entry
    {
        private final UUID iid;

        final Set<UUID> acknowledged;
        boolean executed = false;
        Set<UUID> stronglyConnected = null;
        private boolean isSccTerminator = false;

        private Entry(UUID iid)
        {
            this.iid = iid;
            this.acknowledged = Sets.newHashSet();
        }

        private Entry(UUID iid, Set<UUID> acknowledged, boolean executed)
        {
            this.iid = iid;
            this.acknowledged = acknowledged;
            this.executed = executed;
        }

        // if an instance has been acknowledged by an instance that
        // isn't part of it's strongly connected component, we can evict
        private boolean canEvict()
        {

            if (acknowledged.size() == 0 || stronglyConnected == null)
                return false;

            if (isSccTerminator)
            {
                return Sets.difference(acknowledged, stronglyConnected).size() > 0;
            }
            else
            {
                return acknowledged.size() > 0;
            }
        }

        private void setStronglyConnected(Set<UUID> scc)
        {
            assert stronglyConnected == null || stronglyConnected.equals(scc);

            stronglyConnected = scc != null ? Sets.newHashSet(scc) : Sets.<UUID>newHashSet();

            if (stronglyConnected.size() > 0)
            {
                List<UUID> sorted = Lists.newArrayList(stronglyConnected);
                Collections.sort(sorted, DependencyGraph.comparator);
                UUID componentTerminator = sorted.get(sorted.size() - 1);
                isSccTerminator = componentTerminator.equals(iid);
                stronglyConnected.remove(iid);
            }
        }

        private static final IVersionedSerializer<Entry> serializer = new IVersionedSerializer<Entry>()
        {
            @Override
            public void serialize(Entry entry, DataOutputPlus out, int version) throws IOException
            {
                UUIDSerializer.serializer.serialize(entry.iid, out, version);

                Serializers.uuidSets.serialize(entry.acknowledged, out, version);

                out.writeBoolean(entry.executed);
            }

            @Override
            public Entry deserialize(DataInput in, int version) throws IOException
            {
                return new Entry(
                        UUIDSerializer.serializer.deserialize(in, version),
                        Serializers.uuidSets.deserialize(in, version),
                        in.readBoolean());
            }

            @Override
            public long serializedSize(Entry entry, int version)
            {
                return UUIDSerializer.serializer.serializedSize(entry.iid, version)
                        + Serializers.uuidSets.serializedSize(entry.acknowledged, version) + 1 + 1;
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

    private final SetMultimap<UUID, UUID> pendingAcknowledgements = HashMultimap.create();
    private final SetMultimap<Long, UUID> pendingEpochAcknowledgements = HashMultimap.create(); // for gc'ing pendingEpochAcknowledgements

    public KeyState(long epoch)
    {
        this(epoch, 0);
    }

    public KeyState(long epoch, long executionCount)
    {
        this.epoch = epoch;
        this.executionCount = executionCount;
    }

    @VisibleForTesting
    Entry get(UUID iid)
    {
        return entries.get(iid);
    }

    private void maybeEvict(UUID iid)
    {
        Entry entry = entries.get(iid);
        if (entry == null)
        {
            return;
        }

        if (entry.canEvict())
        {
            entries.remove(iid);
        }
    }

    public Entry recordInstance(UUID iid)
    {
        Entry entry = new Entry(iid);
        entries.put(iid, entry);

        entry.acknowledged.addAll(pendingAcknowledgements.removeAll(iid));

        return entry;
    }

    @VisibleForTesting
    Set<UUID> getDeps()
    {
        return ImmutableSet.copyOf(entries.keySet());
    }

    public Set<UUID> getDepsAndAdd(UUID iid)
    {
        Set<UUID> deps = new HashSet<>(entries.size());
        for (UUID depId : entries.keySet())
        {
            if (depId.equals(iid))
                continue;

            deps.add(depId);
        }
        recordInstance(iid);
        return deps;
    }

    private void markAcknowledged(UUID id, UUID by)
    {
        Entry entry = get(id);
        if (entry != null)
        {
            entry.acknowledged.add(by);
        }
        else
        {
            pendingAcknowledgements.put(id, by);
            pendingEpochAcknowledgements.put(epoch, id);
        }
    }

    public void markAcknowledged(Set<UUID> iids, UUID by)
    {
        for (UUID iid: iids)
        {
            if (iid.equals(by))
                continue;
            markAcknowledged(iid, by);
            maybeEvict(iid);
        }
    }

    public void markExecuted(UUID iid, Set<UUID> stronglyConnected)
    {
        Entry entry = get(iid);
        if (entry != null)
        {
            entry.executed = true;
            entry.setStronglyConnected(stronglyConnected);
        }
        // we can't evict even if the instance has been acknowledged.
        // If the instance is part of a strongly connected component,
        // they could all acknowledge each other, and would terminate
        // the execution chain once executed.

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

        maybeEvict(iid);
    }

    public long getEpoch()
    {
        return epoch;
    }

    @VisibleForTesting
    public Map<Long, Set<UUID>> getEpochExecutions()
    {
        return epochs;
    }

    public void setEpoch(long epoch)
    {
        if (epoch < this.epoch)
        {
            throw new IllegalArgumentException(String.format("Cannot decrement epoch. %s -> %s", this.epoch, epoch));
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

    /**
     * returns a map of epochs older than the given value, and
     * the instances contained in them. Since this is used for
     * garbage collection, an exception is thrown if removing
     * epochs older than the given value would leave the KeyState
     * in an illegal state.
     */
    public Map<Long, Set<UUID>> getEpochsOlderThan(Long e)
    {
        if (e >= epoch - 1)
        {
            throw new IllegalArgumentException("Can't GC instances for epochs >= current epoch - 1");
        }
        return getEpochsOlderThanUnsafe(e);
    }

    private Map<Long, Set<UUID>> getEpochsOlderThanUnsafe(Long e)
    {
        Map<Long, Set<UUID>> rmap = new HashMap<>(epochs.size());
        for (Map.Entry<Long, Set<UUID>> entry: epochs.entrySet())
        {
            if (entry.getKey() < e)
            {
                rmap.put(entry.getKey(), entry.getValue());
            }
        }
        return rmap;
    }

    /**
     * Checks that none of the key managers owned by the given token state
     * have any active instances in any epochs but the current one
     */
    public boolean canIncrementToEpoch(long targetEpoch)
    {
        if (targetEpoch == epoch)
        {
            // duplicate message
            return true;
        }

        for (Map.Entry<Long, Set<UUID>> entry: getEpochsOlderThanUnsafe(epoch).entrySet())
        {
            if (Sets.intersection(entries.keySet(), entry.getValue()).size() > 0)
            {
                return false;
            }

        }
        return true;
    }

    public void removeEpoch(Long e)
    {
        Set<UUID> ids = epochs.get(e);
        assert ids == null || Sets.intersection(entries.keySet(), ids).size() == 0;
        epochs.remove(e);

        // clean up pending acks
        for (UUID id: pendingEpochAcknowledgements.get(e))
        {
            pendingAcknowledgements.removeAll(id);
        }
    }

    public static final IVersionedSerializer<KeyState> serializer = new IVersionedSerializer<KeyState>()
    {
        @Override
        public void serialize(KeyState deps, DataOutputPlus out, int version) throws IOException
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

            // pending acks
            Set<UUID> pendingKeys = deps.pendingAcknowledgements.keySet();
            out.writeInt(pendingKeys.size());
            for (UUID id: pendingKeys)
            {
                UUIDSerializer.serializer.serialize(id, out, version);
                Serializers.uuidSets.serialize(deps.pendingAcknowledgements.get(id), out, version);
            }

            // pending epoch acks
            Set<Long> pendingEpochKeys = deps.pendingEpochAcknowledgements.keySet();
            out.writeInt(pendingKeys.size());
            for (Long epoch: pendingEpochKeys)
            {
                out.writeLong(epoch);
                Serializers.uuidSets.serialize(deps.pendingEpochAcknowledgements.get(epoch), out, version);
            }


        }

        @Override
        public KeyState deserialize(DataInput in, int version) throws IOException
        {
            KeyState deps = new KeyState(in.readLong(), in.readLong());
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

            // pending acks
            int numPending = in.readInt();
            for (int i=0; i<numPending; i++)
            {
                UUID id = UUIDSerializer.serializer.deserialize(in, version);
                deps.pendingAcknowledgements.putAll(id, Serializers.uuidSets.deserialize(in, version));
            }

            // pending epoch acks
            int numPendingEpoch = in.readInt();
            for (int i=0; i<numPendingEpoch; i++)
            {
                Long epoch = in.readLong();
                deps.pendingEpochAcknowledgements.putAll(epoch, Serializers.uuidSets.deserialize(in, version));
            }

            return deps;
        }

        @Override
        public long serializedSize(KeyState deps, int version)
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

            // pending acks
            size += 4;
            for (UUID id: deps.pendingAcknowledgements.keySet())
            {
                size += UUIDSerializer.serializer.serializedSize(id, version);
                size += Serializers.uuidSets.serializedSize(deps.pendingAcknowledgements.get(id), version);
            }

            // pending epoch acks
            size += 4;
            for (Long epoch: deps.pendingEpochAcknowledgements.keySet())
            {
                size += 8;
                size += Serializers.uuidSets.serializedSize(deps.pendingEpochAcknowledgements.get(epoch), version);
            }

            return size;
        }
    };
}

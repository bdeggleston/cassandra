package org.apache.cassandra.service.epaxos;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * in the sorted component. This instance must be acknowledged by an instance outside of the component.
 *
 * A side effect of needing to know what's in a strongly connected component is that instances must be
 * executed themselves before they can be evicted.
 */
public class KeyState
{
    private static final Logger logger = LoggerFactory.getLogger(KeyState.class);

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
    private final Map<Long, EpochExecutionInfo> epochs = new HashMap<>();

    private long epoch;
    private long executionCount;

    /**
     * If a row was streamed to this node containing a mutation from an instance
     * that wasn't yet locally executed, the execution position of the remote node
     * will be recorded here, and all instances up to and including this position
     * will be no-oped to guard against inconsistencies.
     */
    private ExecutionInfo futureExecution = null;

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
    Set<UUID> getActiveInstanceIds()
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
    public void markExecuted(UUID iid, Set<UUID> stronglyConnected, ReplayPosition position)
    {
        markExecuted(iid, stronglyConnected, epoch, position);
    }

    void markExecuted(UUID iid, Set<UUID> stronglyConnected, long execEpoch, ReplayPosition position)
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

        EpochExecutionInfo info = epochs.get(execEpoch);
        if (info == null)
        {
            info = new EpochExecutionInfo();
            epochs.put(execEpoch, info);
        }
        if (!info.contains(iid))
        {
            executionCount++;
            info.add(iid, position);
        }

        maybeEvict(iid);

        if (futureExecution != null)
        {
            if (new ExecutionInfo(execEpoch, executionCount).compareTo(futureExecution) >= 0)
            {
                futureExecution = null;
            }
        }

        logger.debug("Instance {} marked executed at rp={}, epoch={}, execNum={}", iid, position, execEpoch, executionCount);
    }

    /**
     * Sets the execution position in local storage. This is for keeping
     * track of data from repairs that include mutations from instances
     * that haven't been executed yet.
     */
    public boolean setFutureExecution(ExecutionInfo info)
    {
        if (futureExecution != null && info.compareTo(futureExecution) <= 0)
        {
            // don't set smaller max execution
            return false;
        }

        futureExecution = info;
        return true;
    }

    /**
     * Returns false if the streamed sstables have data from a mutation
     * not executed locally yet
     */
    public boolean canExecute()
    {
        if (futureExecution == null)
            return true;

        ExecutionInfo localPosition = new ExecutionInfo(epoch, executionCount);
        boolean okToExecute = localPosition.compareTo(futureExecution) >= 0;
        if (!okToExecute && logger.isDebugEnabled())
        {
            logger.debug("Skipping execution locally. {} < {}", localPosition, futureExecution);
        }
        return okToExecute;
    }

    /**
     * Return the latest possible local execution point for the given replay position
     */
    public ExecutionInfo getExecutionInfoAtPosition(ReplayPosition rp)
    {
        List<Long> recordedEpochs = Lists.newArrayList(epochs.keySet());
        Collections.sort(recordedEpochs);
        recordedEpochs = Lists.reverse(recordedEpochs);

        ExecutionInfo position = new ExecutionInfo(getEpoch(), getExecutionCount());
        for (long ep: recordedEpochs)
        {
            EpochExecutionInfo executionInfo = epochs.get(ep);

            ReplayPosition min = executionInfo.min;
            ReplayPosition max = executionInfo.min;

            if (min == null)
            {
                // no mutations for this epoch
                continue;
            }

            if (min.compareTo(rp) <= 0)
            {
                // this epoch's min replay position is less than the
                // one we're looking for. Since we're going through the
                // epochs backwards, the one we're looking for is in here
                Iterator<Pair<UUID, ReplayPosition>> iter = executionInfo.executed.iterator();
                long execPos = 0;
                while (iter.hasNext())
                {
                    ReplayPosition here = iter.next().right;
                    if (here != null && here.compareTo(rp) > 0)
                    {
                        break;
                    }
                    execPos++;
                }
                return new ExecutionInfo(ep, execPos);
            }
            else
            {
                // if the next epoch has no mutations, we consider the
                // replay position to match the beginning of this epoch
                position = new ExecutionInfo(ep, 0);
            }
        }

        return position;
    }

    public long getEpoch()
    {
        return epoch;
    }

    @VisibleForTesting
    public Map<Long, Set<UUID>> getEpochExecutions()
    {
        Map<Long, Set<UUID>> m = new HashMap<>();
        for (Map.Entry<Long, EpochExecutionInfo> entry: epochs.entrySet())
        {
            m.put(entry.getKey(), entry.getValue().idSet);
        }
        return m;
    }

    public Map<Long, List<UUID>> getOrderedEpochExecutions()
    {
        Map<Long, List<UUID>> m = new HashMap<>();
        for (Map.Entry<Long, EpochExecutionInfo> entry: epochs.entrySet())
        {
            List<UUID> executed = new ArrayList<>(entry.getValue().executed.size());
            for (Pair<UUID, ReplayPosition> info: entry.getValue().executed)
            {
                executed.add(info.left);
            }
            m.put(entry.getKey(), executed);
        }
        return m;
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

    // for testing only
    @VisibleForTesting
    void setExecutionCount(long executionCount)
    {
        this.executionCount = executionCount;
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
        for (Map.Entry<Long, EpochExecutionInfo> entry: epochs.entrySet())
        {
            if (entry.getKey() < e)
            {
                rmap.put(entry.getKey(), entry.getValue().idSet);
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
        EpochExecutionInfo ids = epochs.get(e);
        assert ids == null || Sets.intersection(entries.keySet(), ids.idSet).size() == 0;
        epochs.remove(e);

        // clean up pending acks
        for (UUID id: pendingEpochAcknowledgements.get(e))
        {
            pendingAcknowledgements.removeAll(id);
        }
    }

    private static class EpochExecutionInfo
    {
        public transient ReplayPosition min = null;
        public transient ReplayPosition max = null;
        public final LinkedList<Pair<UUID, ReplayPosition>> executed = new LinkedList<>();
        public final Set<UUID> idSet = new HashSet<>();

        private void add(UUID id, ReplayPosition position)
        {
            if (position != null)
            {
                if (min == null)
                {
                    min = position;
                }
                max = position;
            }
            executed.add(Pair.create(id, position));
            idSet.add(id);
        }

        private boolean contains(UUID id)
        {
            return idSet.contains(id);
        }

        public static final IVersionedSerializer<EpochExecutionInfo> serializer = new IVersionedSerializer<EpochExecutionInfo>()
        {
            @Override
            public void serialize(EpochExecutionInfo info, DataOutputPlus out, int version) throws IOException
            {
                out.writeInt(info.executed.size());
                for (Pair<UUID, ReplayPosition> entry: info.executed)
                {
                    UUIDSerializer.serializer.serialize(entry.left, out, version);
                    out.writeBoolean(entry.right != null);
                    if (entry.right != null)
                    {
                        ReplayPosition.serializer.serialize(entry.right, out);
                    }
                }
            }

            @Override
            public EpochExecutionInfo deserialize(DataInput in, int version) throws IOException
            {
                int numEntries = in.readInt();
                EpochExecutionInfo info = new EpochExecutionInfo();
                for (int i=0; i<numEntries; i++)
                {
                    UUID id = UUIDSerializer.serializer.deserialize(in, version);
                    ReplayPosition rp = in.readBoolean() ? ReplayPosition.serializer.deserialize(in) : null;
                    info.add(id, rp);
                }
                return info;
            }

            @Override
            public long serializedSize(EpochExecutionInfo info, int version)
            {
                long size = 0;
                size += 4; // out.writeInt(info.executed.size());
                for (Pair<UUID, ReplayPosition> entry: info.executed)
                {
                    size += UUIDSerializer.serializer.serializedSize(entry.left, version);
                    size += 1; // out.writeBoolean(entry.right != null);
                    if (entry.right != null)
                    {
                        size += ReplayPosition.serializer.serializedSize(entry.right, TypeSizes.NATIVE);
                    }
                }
                return size;
            }
        };
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
            for (Map.Entry<Long, EpochExecutionInfo> entry: deps.epochs.entrySet())
            {
                out.writeLong(entry.getKey());
                EpochExecutionInfo.serializer.serialize(entry.getValue(), out, version);
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

            boolean hasMaxExecution = deps.futureExecution != null;
            out.writeBoolean(hasMaxExecution);
            if (hasMaxExecution)
            {
                ExecutionInfo.serializer.serialize(deps.futureExecution, out, version);
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
                deps.epochs.put(in.readLong(), EpochExecutionInfo.serializer.deserialize(in, version));
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

            if (in.readBoolean())
            {
                deps.setFutureExecution(ExecutionInfo.serializer.deserialize(in, version));
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
            for (Map.Entry<Long, EpochExecutionInfo> entry: deps.epochs.entrySet())
            {
                size += 8; //out.writeLong(entry.getKey());
                size += EpochExecutionInfo.serializer.serializedSize(entry.getValue(), version);
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

            // futureExecution
            size += 1;
            if (deps.futureExecution != null)
            {
                size += ExecutionInfo.serializer.serializedSize(deps.futureExecution, version);
            }

            return size;
        }
    };
}

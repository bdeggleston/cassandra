package org.apache.cassandra.service.epaxos;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.service.epaxos.exceptions.BallotException;
import org.apache.cassandra.service.epaxos.exceptions.InvalidInstanceStateChange;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.UUIDSerializer;

import javax.annotation.Nullable;
import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * An Epaxos instance
 * Instances are not thread-safe, it's the responsibility of the user
 * to synchronize access
 */
public class Instance
{
    public static final IVersionedSerializer<Instance> serializer = new ExternalSerializer();
    static final IVersionedSerializer<Instance> internalSerializer = new InternalSerializer();

    public static enum State
    {
        // order matters
        INITIALIZED(0),
        PREACCEPTED(1),
        ACCEPTED(2),
        COMMITTED(3),
        EXECUTED(4);

        State(int expectedOrdinal)
        {
            assert ordinal() == expectedOrdinal;
        }

        public boolean isLegalPromotion(State state)
        {
            return state.ordinal() >= this.ordinal();
        }

        public boolean isCommitted()
        {
            return atLeast(COMMITTED);
        }

        public boolean atLeast(State state)
        {
            return ordinal() >= state.ordinal();
        }

    }

    private final UUID id;
    private final SerializedRequest query;
    private final InetAddress leader;
    private volatile State state = State.INITIALIZED;
    private volatile int ballot = 0;
    private volatile boolean noop;
    private volatile boolean fastPathImpossible;
    private volatile Set<UUID> dependencies = null;
    private volatile boolean leaderDepsMatch = false;
    private volatile List<InetAddress> successors = null;

    // fields not transmitted to other nodes
    private volatile boolean placeholder = false;
    private volatile boolean acknowledged = false;
    private volatile boolean acknowledgedChanged = false;
    private volatile Set<UUID> stronglyConnected = null;

    private class DependencyFilter implements Predicate<UUID>
    {
        @Override
        public boolean apply(@Nullable UUID uuid)
        {
            return uuid != null && !uuid.equals(id);
        }
    }

    private final DependencyFilter dependencyFilter;

    Instance(SerializedRequest query, InetAddress leader)
    {
        this(UUIDGen.getTimeUUID(), query, leader);
    }

    Instance(UUID id, SerializedRequest query, InetAddress leader)
    {
        this.id = id;
        this.dependencyFilter = new DependencyFilter();
        this.query = query;
        this.leader = leader;
    }

    private Instance(Instance i)
    {
        this(i.id, i.query, i.leader);
        state = i.state;
        ballot = i.ballot;
        noop = i.noop;
        fastPathImpossible = i.fastPathImpossible;
        placeholder = i.placeholder;
        dependencies = i.dependencies;
        successors = i.successors;
        leaderDepsMatch = i.leaderDepsMatch;
    }

    public UUID getId()
    {
        return id;
    }

    public SerializedRequest getQuery()
    {
        return query;
    }

    public State getState()
    {
        return state;
    }

    public Set<UUID> getDependencies()
    {
        return dependencies;
    }

    public boolean getLeaderDepsMatch()
    {
        return leaderDepsMatch;
    }

    public int getBallot()
    {
        return ballot;
    }

    public void incrementBallot()
    {
        ballot++;
    }

    public int updateBallot(int ballot)
    {
        this.ballot = Math.max(this.ballot, ballot);
        return this.ballot;
    }

    public void checkBallot(int ballot) throws BallotException
    {
        if (ballot <= this.ballot)
            throw new BallotException(this, ballot);
        updateBallot(ballot);
    }

    public InetAddress getLeader()
    {
        return leader;
    }

    public boolean isFastPathImpossible()
    {
        return fastPathImpossible;
    }

    public void setFastPathImpossible(boolean fastPathImpossible)
    {
        this.fastPathImpossible = fastPathImpossible;
    }

    public void setNoop(boolean noop)
    {
        this.noop = noop;
    }

    public boolean isNoop()
    {
        return noop;
    }

    public void setPlaceholder(boolean placeholder)
    {
        this.placeholder = placeholder;
    }

    public boolean isPlaceholder()
    {
        return (!state.atLeast(State.ACCEPTED)) && placeholder;
    }

    public void setAcknowledged()
    {
        setAcknowledged(true);
    }

    public void setAcknowledged(boolean acknowledged)
    {
        acknowledgedChanged |= acknowledged != this.acknowledged;
        this.acknowledged = acknowledged;
    }

    public boolean isAcknowledged()
    {
        return acknowledged;
    }

    public boolean isAcknowledgedChanged()
    {
        return acknowledgedChanged;
    }

    public void setSuccessors(List<InetAddress> successors)
    {
        this.successors = successors;
    }

    public List<InetAddress> getSuccessors()
    {
        return successors;
    }

    public Set<UUID> getStronglyConnected()
    {
        return stronglyConnected;
    }

    public void setStronglyConnected(Set<UUID> stronglyConnected)
    {
        this.stronglyConnected = ImmutableSet.copyOf(stronglyConnected);
    }

    @VisibleForTesting
    void setState(State state) throws InvalidInstanceStateChange
    {
        if (!this.state.isLegalPromotion(state))
            throw new InvalidInstanceStateChange(this, state);
        this.state = state;

        if (state.atLeast(State.ACCEPTED))
            placeholder = false;
    }

    @VisibleForTesting
    void setDependencies(Set<UUID> dependencies)
    {
        this.dependencies = dependencies != null
                ? ImmutableSet.copyOf(Iterables.filter(dependencies, dependencyFilter))
                : null;
    }

    public void preaccept(Set<UUID> dependencies) throws InvalidInstanceStateChange
    {
        preaccept(dependencies, null);
    }

    public void preaccept(Set<UUID> dependencies, Set<UUID> leaderDependencies) throws InvalidInstanceStateChange
    {
        setState(State.PREACCEPTED);
        setDependencies(dependencies);

        if (leaderDependencies != null)
            leaderDepsMatch = this.dependencies.equals(leaderDependencies);
        placeholder = false;
    }

    public void accept() throws InvalidInstanceStateChange
    {
        accept(dependencies);
    }

    public void accept(Set<UUID> dependencies) throws InvalidInstanceStateChange
    {
        setState(State.ACCEPTED);
        setDependencies(dependencies);
        placeholder = false;
    }

    public void commit() throws InvalidInstanceStateChange
    {
        commit(dependencies);
    }

    public void commit(Set<UUID> dependencies) throws InvalidInstanceStateChange
    {
        setState(State.COMMITTED);
        setDependencies(dependencies);
        placeholder = false;
    }

    public void setExecuted()
    {
        try
        {
            setState(State.EXECUTED);
        }
        catch (InvalidInstanceStateChange e)
        {
            throw new AssertionError();
        }
    }

    public ColumnFamily execute() throws ReadTimeoutException, WriteTimeoutException
    {
        return query.execute();
    }

    public Instance copy()
    {
        return new Instance(this);
    }

    public Instance copyRemote()
    {
        Instance instance = new Instance(this.id, this.query, this.leader);
        instance.ballot = ballot;
        instance.noop = noop;
        instance.successors = successors;
        return instance;
    }

    /**
     * Applies mutable non-dependency attributes from remote instance copies
     */
    public void applyRemote(Instance remote)
    {
        this.noop = remote.noop;
    }

    /**
     * Serialization logic shared by internal and external serializers
     */
    public static abstract class Serializer implements IVersionedSerializer<Instance>
    {
        @Override
        public void serialize(Instance instance, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(instance.id, out, version);
            SerializedRequest.serializer.serialize(instance.getQuery(), out, version);
            CompactEndpointSerializationHelper.serialize(instance.leader, out);
            out.writeInt(instance.state.ordinal());
            out.writeInt(instance.ballot);
            out.writeBoolean(instance.noop);
            out.writeBoolean(instance.fastPathImpossible);
            out.writeBoolean(instance.dependencies != null);
            if (instance.dependencies != null)
            {
                Set<UUID> deps = instance.dependencies;
                out.writeInt(deps.size());
                for (UUID dep : deps)
                    UUIDSerializer.serializer.serialize(dep, out, version);
            }
            out.writeBoolean(instance.leaderDepsMatch);

            // there should never be a null successor list at this point
            out.writeInt(instance.successors.size());
            for (InetAddress endpoint: instance.successors)
                CompactEndpointSerializationHelper.serialize(endpoint, out);
        }

        @Override
        public Instance deserialize(DataInput in, int version) throws IOException
        {
            Instance instance = new Instance(
                    UUIDSerializer.serializer.deserialize(in, version),
                    SerializedRequest.serializer.deserialize(in, version),
                    CompactEndpointSerializationHelper.deserialize(in));

            try
            {
                instance.state = State.values()[in.readInt()];
            }
            catch (IllegalArgumentException e)
            {
                throw new IOException(e);
            }

            instance.ballot = in.readInt();
            instance.noop = in.readBoolean();
            instance.fastPathImpossible = in.readBoolean();

            if (in.readBoolean())
            {
                UUID[] deps = new UUID[in.readInt()];
                for (int i=0; i<deps.length; i++)
                    deps[i] = UUIDSerializer.serializer.deserialize(in, version);
                instance.dependencies = ImmutableSet.copyOf(deps);
            }
            else
            {
                instance.dependencies = null;
            }

            instance.leaderDepsMatch = in.readBoolean();

            InetAddress[] successors = new InetAddress[in.readInt()];
            for (int i=0; i<successors.length; i++)
                successors[i] = CompactEndpointSerializationHelper.deserialize(in);
            instance.successors = Lists.newArrayList(successors);

            return instance;
        }

        @Override
        public long serializedSize(Instance instance, int version)
        {
            int size = 0;
            size += UUIDSerializer.serializer.serializedSize(instance.id, version);
            size += SerializedRequest.serializer.serializedSize(instance.getQuery(), version);
            size += CompactEndpointSerializationHelper.serializedSize(instance.leader);
            size += 4;  // instance.state.code
            size += 4;  // instance.ballot
            size += 1;  // instance.noop
            size += 1;  // instance.fastPathImpossible
            size += 1;  // instance.dependencies != null
            if (instance.dependencies != null)
            {
                size += 4;  // deps.size
                for (UUID dep : instance.dependencies)
                    size += UUIDSerializer.serializer.serializedSize(dep, version);
            }
            size += 1;  // instance.leaderDepsMatch

            size += 4;  // instance.successors.size
            for (InetAddress successor: instance.successors)
                size += CompactEndpointSerializationHelper.serializedSize(successor);

            return size;
        }
    }

    /**
     * Serialization used to communicate instances to other nodes
     */
    private static class ExternalSerializer extends Serializer
    {
        @Override
        public void serialize(Instance instance, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(instance != null);
            if (instance != null)
            {
                if (instance.dependencies == null || instance.isPlaceholder())
                    throw new AssertionError("cannot transmit placeholder instances");
                super.serialize(instance, out, version);
            }
        }

        @Override
        public Instance deserialize(DataInput in, int version) throws IOException
        {
            if (!in.readBoolean())
                return null;
            return super.deserialize(in, version);
        }

        @Override
        public long serializedSize(Instance instance, int version)
        {
            if (instance == null)
                return 1;
            if (instance.dependencies == null || instance.isPlaceholder())
                throw new AssertionError("cannot transmit placeholder instances");
            return super.serializedSize(instance, version) + 1;
        }
    }

    /**
     * Serialization used for local instance persistence
     */
    private static class InternalSerializer extends Serializer
    {
        @Override
        public void serialize(Instance instance, DataOutputPlus out, int version) throws IOException
        {
            super.serialize(instance, out, version);
            out.writeBoolean(instance.placeholder);
            out.writeBoolean(instance.acknowledged);
            out.writeBoolean(instance.stronglyConnected != null);
            if (instance.stronglyConnected != null)
            {
                out.writeInt(instance.stronglyConnected.size());
                for (UUID iid: instance.stronglyConnected)
                    UUIDSerializer.serializer.serialize(iid, out, version);
            }
        }

        @Override
        public Instance deserialize(DataInput in, int version) throws IOException
        {
            Instance instance = super.deserialize(in, version);
            instance.placeholder = in.readBoolean();
            instance.acknowledged = in.readBoolean();
            if (in.readBoolean())
            {
                UUID[] scc = new UUID[in.readInt()];
                for (int i=0; i<scc.length; i++)
                    scc[i] = UUIDSerializer.serializer.deserialize(in, version);
                instance.stronglyConnected = ImmutableSet.copyOf(scc);
            }
            return instance;
        }

        @Override
        public long serializedSize(Instance instance, int version)
        {
            long size = super.serializedSize(instance, version);
            size += 1;  // instance.placeholder
            size += 1;  // instance.acknowledged
            size += 1;  // instance.stronglyConnected != null
            if (instance.stronglyConnected != null)
            {
                size += 4;  // stronglyConnected.size
                for (UUID iid : instance.stronglyConnected)
                    size += UUIDSerializer.serializer.serializedSize(iid, version);
            }
            return size;
        }
    }
}

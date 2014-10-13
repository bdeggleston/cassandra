package org.apache.cassandra.service.epaxos;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.epaxos.exceptions.BallotException;
import org.apache.cassandra.service.epaxos.exceptions.InvalidInstanceStateChange;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.UUIDSerializer;

import javax.annotation.Nullable;
import java.io.DataInput;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;

/**
 * An Epaxos instance
 * Instances are not thread-safe, it's the responsibility of the user
 * to synchronize access
 */
public class Instance
{
    public static final Serializer serializer = new Serializer();
    public static enum State
    {
        INITIALIZED(0),
        PREACCEPTED(1),
        ACCEPTED(2),
        COMMITTED(3),
        EXECUTED(4);

        private final int code;

        private State(int code)
        {
            this.code = code;
        }

        public boolean isLegalPromotion(State state)
        {
            return state.code > this.code;
        }

        public static State fromCode(int code)
        {
            for (State s: values())
                if (code == s.code)
                    return s;
            throw new IllegalArgumentException(String.format("Unknown state: %s", code));
        }
    }

    private final UUID id;
    private final SerializedRequest query;
    private volatile State state = State.INITIALIZED;
    private volatile int ballot = 0;
    private volatile Set<UUID> dependencies = null;
    private volatile boolean leaderDepsMatch = false;
    private volatile Set<UUID> stronglyConnected = null;

    private transient ColumnFamily result;

    private class DependencyFilter implements Predicate<UUID>
    {
        @Override
        public boolean apply(@Nullable UUID uuid)
        {
            return uuid != null && !uuid.equals(id);
        }
    }

    private final DependencyFilter dependencyFilter;

    Instance(SerializedRequest query)
    {
        this(UUIDGen.getTimeUUID(), query);
    }

    Instance(UUID id, SerializedRequest query)
    {
        this.id = id;
        this.dependencyFilter = new DependencyFilter();
        this.query = query;
    }

    private Instance(Instance i)
    {
        this(i.id, i.query);
        state = i.state;
        ballot = i.ballot;
        dependencies = i.dependencies;
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
    }

    public void accept() throws InvalidInstanceStateChange
    {
        accept(dependencies);
    }

    public void accept(Set<UUID> dependencies) throws InvalidInstanceStateChange
    {
        setState(State.ACCEPTED);
        setDependencies(dependencies);
    }

    public void commit() throws InvalidInstanceStateChange
    {
        commit(dependencies);
    }

    public void commit(Set<UUID> dependencies) throws InvalidInstanceStateChange
    {
        setState(State.COMMITTED);
        setDependencies(dependencies);
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

    public Instance copy()
    {
        return new Instance(this);
    }

    public Instance copyRemote()
    {
        Instance instance = new Instance(this.id, this.query);
        instance.ballot = ballot;
        return instance;
    }

    public static class Serializer implements IVersionedSerializer<Instance>
    {
        @Override
        public void serialize(Instance instance, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(instance.id, out, version);
            SerializedRequest.serializer.serialize(instance.getQuery(), out, version);
            out.writeInt(instance.state.code);
            out.writeInt(instance.ballot);
            Set<UUID> deps = instance.dependencies;
            out.writeInt(deps.size());
            for (UUID dep : deps)
                UUIDSerializer.serializer.serialize(dep, out, version);
            out.writeBoolean(instance.leaderDepsMatch);
        }

        @Override
        public Instance deserialize(DataInput in, int version) throws IOException
        {
            Instance instance = new Instance(
                    UUIDSerializer.serializer.deserialize(in, version),
                    SerializedRequest.serializer.deserialize(in, version));

            try
            {
                instance.state = State.fromCode(in.readInt());
            }
            catch (IllegalArgumentException e)
            {
                throw new IOException(e);
            }

            instance.ballot = in.readInt();

            UUID[] deps = new UUID[in.readInt()];
            for (int i=0; i<deps.length; i++)
                deps[i] = UUIDSerializer.serializer.deserialize(in, version);
            instance.dependencies = ImmutableSet.copyOf(deps);

            instance.leaderDepsMatch = in.readBoolean();

            return instance;
        }

        @Override
        public long serializedSize(Instance instance, int version)
        {
            int size = 0;

            size += UUIDSerializer.serializer.serializedSize(instance.id, version);
            size += SerializedRequest.serializer.serializedSize(instance.getQuery(), version);
            size += 4;  //out.writeInt(instance.state.code);
            size += 4;  //out.writeInt(instance.ballot);
            size += 4;  //out.writeInt(deps.size());
            for (UUID dep : instance.dependencies)
                size += UUIDSerializer.serializer.serializedSize(dep, version);
            size += 1;  //out.writeBoolean(instance.leaderDepsMatch);
            return size;
        }
    }
}

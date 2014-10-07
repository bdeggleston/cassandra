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

        private final int number;

        private State(int number)
        {
            this.number = number;
        }

        public boolean isLegalPromotion(State state)
        {
            return state.number > this.number;
        }
    }

    private final UUID id;
    private final SerializedRequest query;
    private volatile State state = State.INITIALIZED;
    private volatile int ballot = 0;
    private volatile Set<UUID> dependencies = null;
    private volatile boolean leaderDepsMatch = false;

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
        accept(null);
    }

    public void accept(Set<UUID> dependencies) throws InvalidInstanceStateChange
    {
        setState(State.ACCEPTED);
        setDependencies(dependencies);
    }

    public void commit() throws InvalidInstanceStateChange
    {
        commit(null);
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

    public static class Serializer implements IVersionedSerializer<Instance>
    {
        @Override
        public void serialize(Instance instance, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(instance.id, out, version);
            out.writeInt(instance.state.number);
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

            return null;
        }

        @Override
        public long serializedSize(Instance instance, int version)
        {
            return 0;
        }
    }
}

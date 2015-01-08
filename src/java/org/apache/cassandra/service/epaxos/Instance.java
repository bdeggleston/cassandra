package org.apache.cassandra.service.epaxos;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.epaxos.exceptions.BallotException;
import org.apache.cassandra.service.epaxos.exceptions.InvalidInstanceStateChange;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.UUIDSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public abstract class Instance
{
    private static final Logger logger = LoggerFactory.getLogger(EpaxosState.class);

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

    public static enum Type
    {
        QUERY(0, QueryInstance.serializer, QueryInstance.internalSerializer),
        TOKEN(1, TokenInstance.serializer, TokenInstance.internalSerializer);

        public final IVersionedSerializer<Instance> serializer;
        public final IVersionedSerializer<Instance> internalSerializer;

        Type(int o, IVersionedSerializer<Instance> serializer, IVersionedSerializer<Instance> internalSerializer)
        {
            assert o == ordinal();
            this.serializer = serializer;
            this.internalSerializer = internalSerializer;
        }

        public static Type fromCode(int code)
        {
            return Type.values()[code];
        }
    }

    protected final UUID id;
    protected final InetAddress leader;
    protected volatile State state = State.INITIALIZED;
    protected volatile int ballot = 0;
    protected volatile boolean noop;
    protected volatile boolean fastPathImpossible; // TODO: remove
    protected volatile Set<UUID> dependencies = null;
    protected volatile boolean leaderAttrsMatch = false;
    protected volatile List<InetAddress> successors = null;

    // fields not transmitted to other nodes
    private volatile boolean placeholder = false;
    private volatile Set<UUID> stronglyConnected = null;
    private volatile long lastUpdated = System.currentTimeMillis();

    private class DependencyFilter implements Predicate<UUID>
    {
        @Override
        public boolean apply(@Nullable UUID uuid)
        {
            return uuid != null && !uuid.equals(id);
        }
    }

    private final DependencyFilter dependencyFilter;

    Instance(InetAddress leader)
    {
        this(UUIDGen.getTimeUUID(), leader);
    }

    Instance(UUID id, InetAddress leader)
    {
        this.id = id;
        this.dependencyFilter = new DependencyFilter();
        this.leader = leader;
    }

    protected Instance(Instance i)
    {
        this(i.id, i.leader);
        state = i.state;
        ballot = i.ballot;
        noop = i.noop;
        fastPathImpossible = i.fastPathImpossible;
        dependencies = i.dependencies;
        leaderAttrsMatch = i.leaderAttrsMatch;
        successors = i.successors;
        placeholder = i.placeholder;
        stronglyConnected = i.stronglyConnected;
        lastUpdated = i.lastUpdated;
    }

    public UUID getId()
    {
        return id;
    }

    public State getState()
    {
        return state;
    }

    public Set<UUID> getDependencies()
    {
        return dependencies;
    }

    public boolean getLeaderAttrsMatch()
    {
        return leaderAttrsMatch;
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

    public boolean skipExecution()
    {
        return noop;
    }

    public void setPlaceholder(boolean placeholder)
    {
        this.placeholder = placeholder;
    }

    /**
     * When we're notified of instances that have been preaccepted, but they haven't been
     * seen locally, it's useful to record them for failure recovery, but they can't be
     * used for a lot of things in their preaccepted state.
     *
     * Setting them as a placeholder instance prevents them from being included in preaccept
     * dependencies, or prepare responses
     */
    // TODO: are these added to the deps manager after coming out of placeholder mode?
    public void makePlacehoder()
    {
        placeholder = true;
        dependencies = null;
        ballot = 0;
    }

    public boolean isPlaceholder()
    {
        return (!state.atLeast(State.ACCEPTED)) && placeholder;
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

    public long getLastUpdated()
    {
        return lastUpdated;
    }

    public void setLastUpdated()
    {
        setLastUpdated(System.currentTimeMillis());
    }

    public void setLastUpdated(long lastUpdated)
    {
        this.lastUpdated = lastUpdated;
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
            leaderAttrsMatch = this.dependencies.equals(leaderDependencies);
        placeholder = false;
        logger.debug("preaccepted: {}", this);
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
        logger.debug("accepted: {}", this);
    }

    public void commit() throws InvalidInstanceStateChange
    {
        commit(dependencies);
    }

    public void commit(Set<UUID> dependencies) throws InvalidInstanceStateChange
    {
        if (dependencies.size() > 50)
            logger.warn("committing instance with {} dependencies", dependencies.size());
        setDependencies(dependencies);
        setState(State.COMMITTED);
        placeholder = false;
        logger.debug("committed: {}", this);
    }

    /**
     * sets an executed remote instance to committed. Only used when adding missing instances
     */
    public void commitRemote()
    {
        if (state.atLeast(State.EXECUTED))
        {
            state = State.COMMITTED;
        }
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

    /**
     * Returns an exact copy of this instance for internal use
     */
    public abstract Instance copy();

    public abstract Instance copyRemote();
    public abstract Type getType();
    public abstract Token getToken();
    public abstract UUID getCfId();

    /**
     * Applies mutable non-dependency attributes from remote instance copies
     */
    public void applyRemote(Instance remote)
    {
        assert remote.getId().equals(getId());
        this.noop = remote.noop;
        this.fastPathImpossible = remote.fastPathImpossible;
    }

    public MessageOut<MessageEnvelope<Instance>> getMessage(MessagingService.Verb verb, long epoch)
    {
        return new MessageOut<>(verb, new MessageEnvelope<>(getToken(), getCfId(), epoch, this), envelopeSerializer);
    }

    @Deprecated
    public MessageOut<Instance> getMessage(MessagingService.Verb verb)
    {
        return new MessageOut<>(verb, this, serializer);
    }

    /**
     * Serialization logic shared by composable internal and external serializers
     */
    static abstract class BaseSerializer
    {
        public void serialize(Instance instance, DataOutputPlus out, int version) throws IOException
        {
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
            out.writeBoolean(instance.leaderAttrsMatch);

            // there should never be a null successor list at this point
            out.writeInt(instance.successors.size());
            for (InetAddress endpoint: instance.successors)
                CompactEndpointSerializationHelper.serialize(endpoint, out);
        }

        public Instance deserialize(Instance instance, DataInput in, int version) throws IOException
        {
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

            instance.leaderAttrsMatch = in.readBoolean();

            InetAddress[] successors = new InetAddress[in.readInt()];
            for (int i=0; i<successors.length; i++)
                successors[i] = CompactEndpointSerializationHelper.deserialize(in);
            instance.successors = Lists.newArrayList(successors);

            return instance;
        }

        public long serializedSize(Instance instance, int version)
        {
            int size = 0;
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
            size += 1;  // instance.leaderAttrsMatch

            size += 4;  // instance.successors.size
            for (InetAddress successor: instance.successors)
                size += CompactEndpointSerializationHelper.serializedSize(successor);

            return size;
        }
    }

    /**
     * Serialization used to communicate instances to other nodes
     */
    static class ExternalSerializer extends BaseSerializer
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
        public Instance deserialize(Instance instance, DataInput in, int version) throws IOException
        {
            if (!in.readBoolean())
                return null;
            return super.deserialize(instance, in, version);
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
    static class InternalSerializer extends BaseSerializer
    {
        @Override
        public void serialize(Instance instance, DataOutputPlus out, int version) throws IOException
        {
            super.serialize(instance, out, version);
            out.writeBoolean(instance.placeholder);
            out.writeLong(instance.lastUpdated);
            out.writeBoolean(instance.stronglyConnected != null);
            if (instance.stronglyConnected != null)
            {
                out.writeInt(instance.stronglyConnected.size());
                for (UUID iid: instance.stronglyConnected)
                    UUIDSerializer.serializer.serialize(iid, out, version);
            }
        }

        @Override
        public Instance deserialize(Instance instance, DataInput in, int version) throws IOException
        {
            super.deserialize(instance, in, version);
            instance.placeholder = in.readBoolean();
            instance.lastUpdated = in.readLong();
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
            size += 8;  // instance.lastUpdated
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

    public static final IVersionedSerializer<Instance> serializer = new IVersionedSerializer<Instance>()
    {
        @Override
        public void serialize(Instance instance, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(instance != null);
            if (instance != null)
            {
                if (instance.dependencies == null || instance.isPlaceholder())
                    throw new AssertionError("cannot transmit placeholder instances");

                Type type = instance.getType();
                out.writeInt(type.ordinal());
                type.serializer.serialize(instance, out, version);
            }
        }

        @Override
        public Instance deserialize(DataInput in, int version) throws IOException
        {
            if (!in.readBoolean())
                return null;
            Type type = Type.fromCode(in.readInt());
            return type.serializer.deserialize(in, version);
        }

        @Override
        public long serializedSize(Instance instance, int version)
        {
            if (instance == null)
                return 1;
            if (instance.dependencies == null || instance.isPlaceholder())
                throw new AssertionError("cannot transmit placeholder instances");
            return instance.getType().serializer.serializedSize(instance, version) + 1 + 4;
        }
    };

    public static final IVersionedSerializer<MessageEnvelope<Instance>> envelopeSerializer = MessageEnvelope.getSerializer(serializer);

    public static final IVersionedSerializer<Instance> internalSerializer = new IVersionedSerializer<Instance>()
    {
        @Override
        public void serialize(Instance instance, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(instance.getType().ordinal());
            instance.getType().internalSerializer.serialize(instance, out, version);
        }

        @Override
        public Instance deserialize(DataInput in, int version) throws IOException
        {
            Type type = Type.fromCode(in.readInt());
            return type.internalSerializer.deserialize(in, version);
        }

        @Override
        public long serializedSize(Instance instance, int version)
        {
            return instance.getType().internalSerializer.serializedSize(instance, version) + 4;
        }
    };

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "{" +
                "id=" + id +
                ", leader=" + leader +
                ", state=" + state +
                ", dependencies=" + (dependencies != null ? dependencies.size() : null)  +
                '}';
    }

    static Predicate<Instance> skipPlaceholderPredicate = new Predicate<Instance>()
    {
        @Override
        public boolean apply(Instance instance)
        {
            return !instance.isPlaceholder();
        }
    };
}

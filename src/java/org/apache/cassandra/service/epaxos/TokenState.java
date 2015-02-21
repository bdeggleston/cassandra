package org.apache.cassandra.service.epaxos;

import com.google.common.collect.*;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The epoch state for a given token range
 */
public class TokenState
{
    // TODO: record if there are any SERIAL instances executed against this token state

    private final Token token;
    private final UUID cfId;

    // the current epoch used in recording
    // execution epochs
    private long epoch;

    // the highest epoch instance seen so far
    // this is the epoch we expect new instances
    // to be executed in
    // determines whether an epoch increment is already in the works
    // FIXME: this is currently being used to detect if an epoch increment is in progress. Will this work across DCs?
    private long highEpoch;

    private final AtomicInteger executions;

    // the number of failure recovery streams are open
    // instances cannot be gc'd unless this is 0
    private final AtomicInteger recoveryStreams = new AtomicInteger(0);

    public static enum State {

        INITIALIZING(false, false),
        NORMAL(true, true),
        RECOVERY_REQUIRED(false, false),
        PRE_RECOVERY(false, false),
        RECOVERING_INSTANCES(false, false, true),
        RECOVERING_DATA(true, false);

        // this node can participate in epaxos rounds
        private final boolean okToParticipate;
        // this node can execute instances
        private final boolean okToExecute;

        // this node can't execute or participate, but should
        // passively record accept and committed instances
        private final boolean passiveRecord;

        State(boolean okToParticipate, boolean okToExecute)
        {
            this(okToParticipate, okToExecute, false);
        }

        State(boolean okToParticipate, boolean okToExecute, boolean passiveRecord)
        {
            this.okToParticipate = okToParticipate;
            this.okToExecute = okToExecute;
            this.passiveRecord = passiveRecord;
        }

        public boolean isOkToParticipate()
        {
            return okToParticipate;
        }

        public boolean isOkToExecute()
        {
            return okToExecute;
        }

        public boolean isPassiveRecord()
        {
            return passiveRecord;
        }
    }

    private volatile State state;  // local only
    private final SetMultimap<Long, UUID> epochInstances = HashMultimap.create();
    private final SetMultimap<Token, UUID> tokenInstances = HashMultimap.create();

    private transient volatile int lastPersistedExecutionCount = 0;

    // fair to give priority to token mutations
    public final ReadWriteLock rwLock = new ReentrantReadWriteLock(true);

    public TokenState(Token token, UUID cfId, long epoch, long highEpoch, int executions)
    {
        this(token, cfId, epoch, highEpoch, executions, State.NORMAL);
    }

    public TokenState(Token token, UUID cfId, long epoch, long highEpoch, int executions, State state)
    {
        this.token = token;
        this.cfId = cfId;
        this.epoch = epoch;
        this.highEpoch = highEpoch;
        this.executions = new AtomicInteger(executions);
        lastPersistedExecutionCount = executions;
        this.state = state;
    }

    public Token getToken()
    {
        return token;
    }

    public UUID getCfId()
    {
        return cfId;
    }

    public long getEpoch()
    {
        return epoch;
    }

    public void setEpoch(long epoch)
    {
        assert epoch >= this.epoch;
        this.epoch = epoch;
        recordHighEpoch(epoch);

        executions.set(0);
        resetUnrecordedExecutions();
        cleanEpochInstances();
    }

    public synchronized boolean recordHighEpoch(long epoch)
    {
        if (epoch > highEpoch)
        {
            highEpoch = epoch;
            return true;
        }
        else
        {
            return false;
        }
    }

    public long getHighEpoch()
    {
        return highEpoch;
    }

    public void recordExecution()
    {
        executions.incrementAndGet();
    }

    public int getExecutions()
    {
        return executions.get();
    }

    public int getNumUnrecordedExecutions()
    {
        return executions.get() - lastPersistedExecutionCount;
    }

    private void resetUnrecordedExecutions()
    {
        lastPersistedExecutionCount = executions.get();
    }

    public State getState()
    {
        return state;
    }

    public void setState(State state)
    {
        this.state = state;
    }

    void onSave()
    {
        resetUnrecordedExecutions();
    }

    public void recordEpochInstance(EpochInstance instance)
    {
        recordEpochInstance(instance.getEpoch(), instance.getId());
    }

    public void recordTokenInstance(TokenInstance instance)
    {
        recordTokenInstance(instance.getToken(), instance.getId());
    }

    /**
     * Moves the given token instance from the token instance collection, into
     * the epoch instances collection as part of the given epoch.
     *
     * TODO: is it possible for token instances to not be removed?
     */
    public void recordTokenInstanceExecution(TokenInstance instance)
    {
        tokenInstances.remove(instance.getToken(), instance.getId());
        recordEpochInstance(epoch, instance.getId());
    }

    public void lockGc()
    {
        long current = recoveryStreams.incrementAndGet();
        assert current > 0;
    }

    public void unlockGc()
    {
        long current = recoveryStreams.decrementAndGet();
        assert current >= 0;
    }

    public boolean canGc()
    {
        return recoveryStreams.get() < 1;
    }

    void recordEpochInstance(long epoch, UUID id)
    {
        if (epoch < this.epoch)
        {
            return;
        }

        epochInstances.put(epoch, id);
    }

    void recordTokenInstance(Token token, UUID id)
    {
        tokenInstances.put(token, id);
    }

    private void cleanEpochInstances()
    {
        Set<Long> keys = Sets.newHashSet(epochInstances.keySet());
        for (long key : keys)
        {
            if (key < epoch)
            {
                epochInstances.removeAll(key);
            }
        }
    }

    public Set<UUID> getCurrentEpochInstances()
    {
        return ImmutableSet.copyOf(epochInstances.values());
    }

    /**
     * return token instances for tokens that are > the given token
     * @param token
     * @return
     */
    public Set<UUID> getCurrentTokenInstances(Token token)
    {
        Set<UUID> ids = Sets.newHashSet();
        for (Token splitToken: tokenInstances.keySet())
        {
            // exclude greater tokens
            if (splitToken.compareTo(token) < 1)
            {
                ids.addAll(tokenInstances.get(splitToken));
            }
        }
        return ids;
    }

    /**
     * If this is the token state for the given token, the token
     * instance ids are moved into the current epoch instances. Otherwise,
     * they're removed
     * @return true if there are changes to be saved
     */
    public Map<Token, Set<UUID>> splitTokenInstances(Token splitToken)
    {
        Map<Token, Set<UUID>> deps = Maps.newHashMap();
        for (Token depToken: Sets.newHashSet(tokenInstances.keySet()))
        {
            if (depToken.compareTo(splitToken) < 1)
            {
                deps.put(depToken, tokenInstances.removeAll(depToken));
            }
        }
        return deps;
    }

    public EpochDecision evaluateMessageEpoch(IEpochMessage message)
    {
        long remoteEpoch = message.getEpoch();
        return new EpochDecision(EpochDecision.evaluate(epoch, remoteEpoch),
                                 message.getToken(),
                                 epoch,
                                 remoteEpoch);
    }

    /**
     * returns true if every instance for the current and last epoch used
     * the LOCAL_SERIAL consistency level
     */
    public boolean localOnly()
    {
        // TODO: this
        return false;
    }

    public static final IVersionedSerializer<TokenState> serializer = new IVersionedSerializer<TokenState>()
    {
        @Override
        public void serialize(TokenState tokenState, DataOutputPlus out, int version) throws IOException
        {
            Token.serializer.serialize(tokenState.token, out);
            UUIDSerializer.serializer.serialize(tokenState.cfId, out, version);
            out.writeLong(tokenState.epoch);
            out.writeLong(tokenState.highEpoch);
            out.writeInt(tokenState.executions.get());
            out.writeInt(tokenState.state.ordinal());

            // epoch instances
            Set<Long> keys = tokenState.epochInstances.keySet();
            out.writeInt(keys.size());
            for (Long epoch: keys)
            {
                out.writeLong(epoch);
                Serializers.uuidSets.serialize(tokenState.epochInstances.get(epoch), out, version);
            }
        }

        @Override
        public TokenState deserialize(DataInput in, int version) throws IOException
        {
            TokenState ts = new TokenState(Token.serializer.deserialize(in),
                                           UUIDSerializer.serializer.deserialize(in, version),
                                           in.readLong(),
                                           in.readLong(),
                                           in.readInt(),
                                           State.values()[in.readInt()]);

            int numEpochInstanceKeys = in.readInt();
            for (int i=0; i<numEpochInstanceKeys; i++)
            {
                Long epoch = in.readLong();
                ts.epochInstances.putAll(epoch, Serializers.uuidSets.deserialize(in, version));
            }

            return ts;
        }

        @Override
        public long serializedSize(TokenState tokenState, int version)
        {
            long size = Token.serializer.serializedSize(tokenState.token, TypeSizes.NATIVE);
            size += UUIDSerializer.serializer.serializedSize(tokenState.cfId, version);
            size += 8 + 8 + 4 + 4;

            // epoch instances
            size += 4;
            for (Long epoch: tokenState.epochInstances.keySet())
            {
                size += 8;
                size += Serializers.uuidSets.serializedSize(tokenState.epochInstances.get(epoch), version);
            }

            return size;
        }
    };

    @Override
    public String toString()
    {
        return "TokenState{" +
                "token=" + token +
                ", cfId=" + cfId +
                ", epoch=" + epoch +
                ", highEpoch=" + highEpoch +
                ", executions=" + executions +
                ", state=" + state +
                '}';
    }
}

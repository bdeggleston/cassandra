package org.apache.cassandra.service.epaxos;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The epoch state for a given token range
 */
// TODO: persist
public class TokenState
{

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

    public static enum State { NORMAL, RECOVERING }
    private volatile State state;  // local only
    private final SetMultimap<Long, UUID> tokenInstances = HashMultimap.create();

    private transient volatile int lastPersistedExecutionCount = 0;

    // TODO: is a lock even needed? The execution algorithm should handle serialization by itself
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

        executions.set(0);
        resetUnrecordedExecutions();
        cleanTokenInstances();
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

    public void recordTokenInstance(TokenInstance instance)
    {
        recordTokenInstance(instance.getEpoch(), instance.getId());
    }

    void recordTokenInstance(long epoch, UUID id)
    {
        if (epoch < this.epoch)
        {
            return;
        }

        tokenInstances.put(epoch, id);
    }

    private void cleanTokenInstances()
    {
        Set<Long> keys = Sets.newHashSet(tokenInstances.keySet());
        for (long key : keys)
        {
            if (key < epoch)
            {
                tokenInstances.removeAll(key);
            }
        }
    }

    public Set<UUID> getCurrentTokenInstances()
    {
        return ImmutableSet.copyOf(tokenInstances.values());
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
            Set<Long> keys = tokenState.tokenInstances.keySet();
            out.writeInt(keys.size());
            for (Long epoch: keys)
            {
                out.writeLong(epoch);
                Serializers.uuidSets.serialize(tokenState.tokenInstances.get(epoch), out, version);
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
                ts.tokenInstances.putAll(epoch, Serializers.uuidSets.deserialize(in, version));
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
            for (Long epoch: tokenState.tokenInstances.keySet())
            {
                size += 8;
                size += Serializers.uuidSets.serializedSize(tokenState.tokenInstances.get(epoch), version);
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

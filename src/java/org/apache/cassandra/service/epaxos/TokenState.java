package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Maps;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

import java.io.DataInput;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
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
    // the current epoch used in recording
    // execution epochs
    private long epoch;

    // the highest epoch instance seen so far
    // this is the epoch we expect new instances
    // to be executed in
    // FIXME: this is currently being used to detect if an epoch increment is in progress. Will this work across DCs?
    private long highEpoch;

    private final ConcurrentMap<Long, Set<UUID>> epochInstances = Maps.newConcurrentMap();

    private final AtomicInteger executions;
    private transient volatile int lastPersistedExecutionCount = 0;

    public final KeyState keyState;

    // TODO: is a lock even needed? The execution algorithm should handle serialization by itself
    // fair to give priority to token mutations
    public final ReadWriteLock rwLock = new ReentrantReadWriteLock(true);

    public TokenState(Token token, long epoch, long highEpoch, int executions)
    {
        this(token, epoch, highEpoch, executions, new KeyState(epoch));
    }

    public TokenState(Token token, long epoch, long highEpoch, int executions, KeyState keyState)
    {
        this.token = token;
        this.epoch = epoch;
        this.highEpoch = highEpoch;
        this.executions = new AtomicInteger(executions);
        lastPersistedExecutionCount = executions;
        this.keyState = keyState;
    }

    public Token getToken()
    {
        return token;
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

    void onSave()
    {
        resetUnrecordedExecutions();
    }

    // TODO: persist high epoch and execution count
    public static final IVersionedSerializer<TokenState> serializer = new IVersionedSerializer<TokenState>()
    {
        @Override
        public void serialize(TokenState tokenState, DataOutputPlus out, int version) throws IOException
        {
            Token.serializer.serialize(tokenState.token, out);
            out.writeLong(tokenState.epoch);
            out.writeLong(tokenState.highEpoch);
            out.writeInt(tokenState.executions.get());
        }

        @Override
        public TokenState deserialize(DataInput in, int version) throws IOException
        {
            return new TokenState(Token.serializer.deserialize(in), in.readLong(), in.readLong(), in.readInt());
        }

        @Override
        public long serializedSize(TokenState tokenState, int version)
        {
            long size = Token.serializer.serializedSize(tokenState.token, TypeSizes.NATIVE);
            return size + 8 + 8 + 4;
        }
    };

    @Override
    public String toString()
    {
        return "TokenState{" +
                "token=" + token +
                ", epoch=" + epoch +
                ", highEpoch=" + highEpoch +
                ", executions=" + executions +
                '}';
    }
}

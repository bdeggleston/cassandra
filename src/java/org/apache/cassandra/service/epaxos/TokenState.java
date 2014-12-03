package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

import java.io.DataInput;
import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
* Created by beggleston on 11/30/14.
*/ // TODO: persist
public class TokenState
{
    private long epoch;

    public final KeyState keyState;

    // TODO: is a lock even needed? The execution algorithm should handle serialization by itself
    // fair to give priority to token mutations
    public final ReadWriteLock rwLock = new ReentrantReadWriteLock(true);

    public TokenState(long epoch)
    {
        this(epoch, new KeyState(epoch));
    }

    public TokenState(long epoch, KeyState keyState)
    {
        this.epoch = epoch;
        this.keyState = keyState;
    }

    public long getEpoch()
    {
        return epoch;
    }

    public void setEpoch(long epoch)
    {
        this.epoch = epoch;
    }

    public static final IVersionedSerializer<TokenState> serializer = new IVersionedSerializer<TokenState>()
    {
        @Override
        public void serialize(TokenState tokenState, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(tokenState.epoch);

        }

        @Override
        public TokenState deserialize(DataInput in, int version) throws IOException
        {
            return new TokenState(in.readLong());
        }

        @Override
        public long serializedSize(TokenState tokenState, int version)
        {
            return 8;
        }
    };
}

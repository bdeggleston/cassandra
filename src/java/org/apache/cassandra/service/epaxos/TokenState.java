package org.apache.cassandra.service.epaxos;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
* Created by beggleston on 11/30/14.
*/ // TODO: persist
public class TokenState
{
    long epoch;

    public final KeyState keyState;

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
}

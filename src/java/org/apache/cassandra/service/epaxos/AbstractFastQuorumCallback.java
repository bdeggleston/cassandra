package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.service.epaxos.Instance;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractFastQuorumCallback<T> implements IAsyncCallback<T>
{
    private static AtomicInteger responseCount = new AtomicInteger();
}

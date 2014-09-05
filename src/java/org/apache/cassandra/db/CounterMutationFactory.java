package org.apache.cassandra.db;

import com.google.common.util.concurrent.Striped;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.tracing.Tracing;

import java.util.concurrent.locks.Lock;

public class CounterMutationFactory
{
    public static final CounterMutationFactory instance = new CounterMutationFactory();

    public final CounterMutation.Serializer serializer;
    private final Striped<Lock> locks;

    public CounterMutationFactory()
    {
        serializer = new CounterMutation.Serializer(MutationFactory.instance);
        locks = Striped.lazyWeakLock(DatabaseDescriptor.instance.getConcurrentCounterWriters() * 1024);
    }

    public CounterMutation create(Mutation mutation, ConsistencyLevel consistency)
    {
        return new CounterMutation(mutation,
                                   consistency,
                                   DatabaseDescriptor.instance,
                                   CacheService.instance,
                                   KeyspaceManager.instance,
                                   MutationFactory.instance,
                                   SystemKeyspace.instance,
                                   Tracing.instance,
                                   serializer,
                                   locks);
    }
}

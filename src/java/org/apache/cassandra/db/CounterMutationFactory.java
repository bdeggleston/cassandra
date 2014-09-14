package org.apache.cassandra.db;

import com.google.common.util.concurrent.Striped;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.locator.LocatorConfig;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.tracing.Tracing;

import java.util.concurrent.locks.Lock;

public class CounterMutationFactory
{
    public static final CounterMutationFactory instance = new CounterMutationFactory(DatabaseDescriptor.instance, MutationFactory.instance);

    public final CounterMutation.Serializer serializer;
    private final Striped<Lock> locks;

    private final DatabaseDescriptor databaseDescriptor;

    public CounterMutationFactory(DatabaseDescriptor databaseDescriptor, MutationFactory mutationFactory)
    {
        assert mutationFactory != null;
        serializer = new CounterMutation.Serializer(mutationFactory, this);
        locks = Striped.lazyWeakLock(databaseDescriptor.getConcurrentCounterWriters() * 1024);

        this.databaseDescriptor = databaseDescriptor;
    }

    public CounterMutation create(Mutation mutation, ConsistencyLevel consistency)
    {
        return new CounterMutation(mutation,
                                   consistency,
                                   databaseDescriptor,
                                   databaseDescriptor.getSchema(),
                                   databaseDescriptor.getCacheService(),
                                   databaseDescriptor.getKeyspaceManager(),
                                   databaseDescriptor.getMutationFactory(),
                                   databaseDescriptor.getSystemKeyspace(),
                                   databaseDescriptor.getMessagingService(),
                                   databaseDescriptor.getTracing(),
                                   databaseDescriptor.getDBConfig(),
                                   databaseDescriptor.getLocatorConfig().getPartitioner(),
                                   serializer,
                                   locks);
    }
}

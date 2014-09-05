package org.apache.cassandra.db;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class MutationFactory
{
    public static final MutationFactory instance = new MutationFactory();



    public final Mutation.Serializer serializer;

    public MutationFactory()
    {
        assert Schema.instance != null;
        assert KeyspaceManager.instance != null;
        assert DBConfig.instance != null;
        serializer = new Mutation.Serializer(DatabaseDescriptor.instance.getWriteRpcTimeout(),
                                             Schema.instance,
                                             KeyspaceManager.instance,
                                             DBConfig.instance);
    }

    public Mutation create(String keyspaceName, ByteBuffer key)
    {
        return new Mutation(keyspaceName,
                            key,
                            new HashMap<UUID, ColumnFamily>(),
                            DatabaseDescriptor.instance.getWriteRpcTimeout(),
                            Schema.instance,
                            KeyspaceManager.instance,
                            serializer,
                            DBConfig.instance);
    }

    public Mutation create(String keyspaceName, ByteBuffer key, ColumnFamily cf)
    {
        return new Mutation(keyspaceName,
                            key,
                            Collections.singletonMap(cf.id(), cf),
                            DatabaseDescriptor.instance.getWriteRpcTimeout(),
                            Schema.instance,
                            KeyspaceManager.instance,
                            serializer,
                            DBConfig.instance);
    }

    public Mutation create(String keyspaceName, Row row)
    {
        return new Mutation(keyspaceName,
                            row.key.getKey(),
                            row.cf,
                            DatabaseDescriptor.instance.getWriteRpcTimeout(),
                            Schema.instance,
                            KeyspaceManager.instance,
                            serializer,
                            DBConfig.instance);
    }

    protected Mutation create(String keyspaceName, ByteBuffer key, Map<UUID, ColumnFamily> modifications)
    {
        return new Mutation(keyspaceName,
                            key,
                            modifications,
                            DatabaseDescriptor.instance.getWriteRpcTimeout(),
                            Schema.instance,
                            KeyspaceManager.instance,
                            serializer,
                            DBConfig.instance);
    }

    public Mutation create(ByteBuffer key, ColumnFamily cf)
    {
        return new Mutation(cf.metadata().ksName,
                            key,
                            cf,
                            DatabaseDescriptor.instance.getWriteRpcTimeout(),
                            Schema.instance,
                            KeyspaceManager.instance,
                            serializer,
                            DBConfig.instance);
    }
}

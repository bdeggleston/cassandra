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
    public static final MutationFactory instance = new MutationFactory(DatabaseDescriptor.instance, DatabaseDescriptor.instance.getSchema(), KeyspaceManager.instance, DBConfig.instance);

    public final Mutation.Serializer serializer;

    private final DatabaseDescriptor databaseDescriptor;

    public MutationFactory(DatabaseDescriptor databaseDescriptor, Schema schema, KeyspaceManager keyspaceManager, DBConfig dbConfig)
    {
        assert schema != null;
        assert keyspaceManager != null;
        assert dbConfig != null;
        serializer = new Mutation.Serializer(databaseDescriptor.getWriteRpcTimeout(),
                                             schema,
                                             keyspaceManager,
                                             dbConfig);

        this.databaseDescriptor = databaseDescriptor;
    }

    public Mutation create(String keyspaceName, ByteBuffer key)
    {
        return new Mutation(keyspaceName,
                            key,
                            new HashMap<UUID, ColumnFamily>(),
                            databaseDescriptor.getWriteRpcTimeout(),
                            databaseDescriptor.getSchema(),
                            databaseDescriptor.getKeyspaceManager(),
                            serializer,
                            databaseDescriptor.getDBConfig());
    }

    public Mutation create(String keyspaceName, ByteBuffer key, ColumnFamily cf)
    {
        return new Mutation(keyspaceName,
                            key,
                            Collections.singletonMap(cf.id(), cf),
                            databaseDescriptor.getWriteRpcTimeout(),
                            databaseDescriptor.getSchema(),
                            databaseDescriptor.getKeyspaceManager(),
                            serializer,
                            databaseDescriptor.getDBConfig());
    }

    public Mutation create(String keyspaceName, Row row)
    {
        return new Mutation(keyspaceName,
                            row.key.getKey(),
                            row.cf,
                            databaseDescriptor.getWriteRpcTimeout(),
                            databaseDescriptor.getSchema(),
                            databaseDescriptor.getKeyspaceManager(),
                            serializer,
                            databaseDescriptor.getDBConfig());
    }

    protected Mutation create(String keyspaceName, ByteBuffer key, Map<UUID, ColumnFamily> modifications)
    {
        return new Mutation(keyspaceName,
                            key,
                            modifications,
                            databaseDescriptor.getWriteRpcTimeout(),
                            databaseDescriptor.getSchema(),
                            databaseDescriptor.getKeyspaceManager(),
                            serializer,
                            databaseDescriptor.getDBConfig());
    }

    public Mutation create(ByteBuffer key, ColumnFamily cf)
    {
        return new Mutation(cf.metadata().ksName,
                            key,
                            cf,
                            databaseDescriptor.getWriteRpcTimeout(),
                            databaseDescriptor.getSchema(),
                            databaseDescriptor.getKeyspaceManager(),
                            serializer,
                            databaseDescriptor.getDBConfig());
    }
}

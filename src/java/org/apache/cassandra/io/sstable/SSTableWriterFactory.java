package org.apache.cassandra.io.sstable;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.DBConfig;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.service.FileCacheService;

public class SSTableWriterFactory
{
    public static final SSTableWriterFactory instance = new SSTableWriterFactory(DatabaseDescriptor.instance);

    private final DatabaseDescriptor databaseDescriptor;

    public SSTableWriterFactory(DatabaseDescriptor databaseDescriptor)
    {
        this.databaseDescriptor = databaseDescriptor;
    }

    public SSTableWriter create(String filename, long keyCount, long repairedAt)
    {
        return new SSTableWriter(filename,
                                 keyCount,
                                 repairedAt,
                                 databaseDescriptor.getSchema(),
                                 databaseDescriptor.getFileCacheService(),
                                 databaseDescriptor.getSSTableReaderFactory(),
                                 databaseDescriptor.getDBConfig());
    }

    public SSTableWriter create(String filename,
                                long keyCount,
                                long repairedAt,
                                CFMetaData metadata,
                                IPartitioner<?> partitioner,
                                MetadataCollector sstableMetadataCollector)
    {
        return new SSTableWriter(filename,
                                 keyCount,
                                 repairedAt,
                                 metadata,
                                 partitioner,
                                 sstableMetadataCollector,
                                 databaseDescriptor.getFileCacheService(),
                                 databaseDescriptor.getSSTableReaderFactory(),
                                 databaseDescriptor.getDBConfig());
    }
}

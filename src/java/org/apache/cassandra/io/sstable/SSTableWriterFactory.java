package org.apache.cassandra.io.sstable;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;

public class SSTableWriterFactory
{
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

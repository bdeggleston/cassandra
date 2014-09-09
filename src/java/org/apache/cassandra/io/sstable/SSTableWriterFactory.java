package org.apache.cassandra.io.sstable;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.DBConfig;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.service.FileCacheService;

public class SSTableWriterFactory
{
    public static final SSTableWriterFactory instance = new SSTableWriterFactory();

    public SSTableWriter create(String filename, long keyCount, long repairedAt)
    {
        return new SSTableWriter(filename,
                                 keyCount,
                                 repairedAt,
                                 Schema.instance,
                                 FileCacheService.instance,
                                 SSTableReaderFactory.instance,
                                 DBConfig.instance);
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
                                 FileCacheService.instance,
                                 SSTableReaderFactory.instance,
                                 DBConfig.instance);
    }
}

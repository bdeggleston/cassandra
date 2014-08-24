package org.apache.cassandra.io.sstable;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.service.ClusterState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSTableWriterFactory
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableWriter.class);

    public static final SSTableWriterFactory instance = new SSTableWriterFactory(DatabaseDescriptor.instance,
                                                                                 Schema.instance,
                                                                                 ClusterState.instance,
                                                                                 SSTableReaderFactory.instance);

    private final DatabaseDescriptor databaseDescriptor;
    private final Schema schema;
    private final ClusterState clusterState;
    private final SSTableReaderFactory ssTableReaderFactory;

    public SSTableWriterFactory(DatabaseDescriptor databaseDescriptor, Schema schema, ClusterState clusterState, SSTableReaderFactory ssTableReaderFactory)
    {
        assert databaseDescriptor != null;
        assert schema != null;
        assert clusterState != null;
        assert ssTableReaderFactory != null;

        this.databaseDescriptor = databaseDescriptor;
        this.schema = schema;
        this.clusterState = clusterState;
        this.ssTableReaderFactory = ssTableReaderFactory;
    }

    public SSTableWriter create(String filename, long keyCount, long repairedAt)
    {
        return create(filename,
                      keyCount,
                      repairedAt,
                      schema.getCFMetaData(Descriptor.fromFilename(filename)),
                      clusterState.getPartitioner(),
                      new MetadataCollector(schema.getCFMetaData(Descriptor.fromFilename(filename)).comparator));
    }

    public SSTableWriter create(String filename, long keyCount, long repairedAt, CFMetaData metadata, IPartitioner<?> partitioner, MetadataCollector sstableMetadataCollector)
    {
        return new SSTableWriter(filename,
                                 keyCount,
                                 repairedAt,
                                 metadata,
                                 partitioner,
                                 sstableMetadataCollector,
                                 databaseDescriptor,
                                 ssTableReaderFactory);
    }
}

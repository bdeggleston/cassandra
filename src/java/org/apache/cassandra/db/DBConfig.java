package org.apache.cassandra.db;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.LocalByPartionerType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LongToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.io.sstable.SSTableReaderFactory;
import org.apache.cassandra.io.sstable.SSTableWriterFactory;
import org.apache.cassandra.io.util.IAllocator;
import org.apache.cassandra.locator.LocatorConfig;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.Murmur3BloomFilter;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.memory.MemtablePool;

import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DBConfig
{
    public static final DBConfig instance = new DBConfig(DatabaseDescriptor.instance, Tracing.instance, DatabaseDescriptor.instance.getSchema(), LocatorConfig.instance);

    public final IAllocator offHeapAllocator;
    private final MemtablePool memtablePool;
    private final long memtableRowOverhead;

    public final IndexSummary.Serializer indexSummarySerializer;
    public final ColumnFamilySerializer columnFamilySerializer;
    public final Row.RowSerializer rowSerializer;
    public final Token.Serializer tokenSerializer;
    public final MerkleTree.Hashable.HashableSerializer hashableSerializer;
    public final MerkleTree.Inner.InnerSerializer innerSerializer;
    public final RowPosition.Serializer rowPositionSerializer;
    public final AbstractBounds.Serializer boundsSerializer;
    public final MerkleTree.Serializer merkleTreeSerializer;
    public final Murmur3BloomFilter.Serializer murmur3BloomFilterSerializer;

    public final long preemptiveOpenInterval;

    public final AbstractType<?> keyComparator;

    private final DatabaseDescriptor databaseDescriptor;

    public DBConfig(DatabaseDescriptor databaseDescriptor, Tracing tracing, Schema schema, LocatorConfig locatorConfig)
    {
        assert tracing != null;
        assert schema != null;
        assert locatorConfig != null;

        this.databaseDescriptor = databaseDescriptor;

        offHeapAllocator = databaseDescriptor.getoffHeapMemoryAllocator();
        memtablePool = databaseDescriptor.getMemtableAllocatorPool();
        memtableRowOverhead = estimateRowOverhead(
                Integer.valueOf(System.getProperty("cassandra.memtable_row_overhead_computation_step", "100000")));

        indexSummarySerializer = new IndexSummary.Serializer(offHeapAllocator);
        columnFamilySerializer = new ColumnFamilySerializer(databaseDescriptor, tracing, schema, this);
        // don't change the partitioner after these are instantiated
        rowSerializer = new Row.RowSerializer(locatorConfig.getPartitioner(), columnFamilySerializer);
        tokenSerializer = new Token.Serializer(locatorConfig.getPartitioner());
        rowPositionSerializer = new RowPosition.Serializer(locatorConfig.getPartitioner(), tokenSerializer);
        boundsSerializer = new AbstractBounds.Serializer(locatorConfig.getPartitioner(), tokenSerializer, rowPositionSerializer);
        hashableSerializer = new MerkleTree.Hashable.HashableSerializer(tokenSerializer);
        innerSerializer = new MerkleTree.Inner.InnerSerializer(tokenSerializer, hashableSerializer);
        merkleTreeSerializer = new MerkleTree.Serializer(tokenSerializer, hashableSerializer, locatorConfig);
        murmur3BloomFilterSerializer = new Murmur3BloomFilter.Serializer(offHeapAllocator);

        preemptiveOpenInterval = calculatePreemptiveOpenInterval(databaseDescriptor.getSSTablePreempiveOpenIntervalInMB());
        keyComparator = getPartitioner().preservesOrder() ? BytesType.instance : new LocalByPartionerType(getPartitioner());
    }

    private int estimateRowOverhead(int count)
    {
        // calculate row overhead
        final OpOrder.Group group = new OpOrder().start();
        int rowOverhead;
        MemtableAllocator allocator = memtablePool.newAllocator();
        ConcurrentNavigableMap<RowPosition, Object> rows = new ConcurrentSkipListMap<>();
        final Object val = new Object();
        for (int i = 0 ; i < count ; i++)
            rows.put(allocator.clone(new BufferDecoratedKey(new LongToken((long) i, getLocatorConfig().getPartitioner()), ByteBufferUtil.EMPTY_BYTE_BUFFER), group), val);
        double avgSize = ObjectSizes.measureDeep(rows) / (double) count;
        rowOverhead = (int) ((avgSize - Math.floor(avgSize)) < 0.05 ? Math.floor(avgSize) : Math.ceil(avgSize));
        rowOverhead -= ObjectSizes.measureDeep(new LongToken((long) 0, null));
        rowOverhead += AtomicBTreeColumns.EMPTY_SIZE;
        allocator.setDiscarding();
        allocator.setDiscarded();
        return rowOverhead;
    }

    private long calculatePreemptiveOpenInterval(long ssTablePreemptiveOpenIntervalInMB)
    {
        long interval = ssTablePreemptiveOpenIntervalInMB * (1L << 20);
        if (interval < 0)
            interval = Long.MAX_VALUE;
        return interval;
    }

    public MemtablePool getMemtablePool()
    {
        return memtablePool;
    }

    public long getMemtableRowOverhead()
    {
        return memtableRowOverhead;
    }

    public boolean isIncrementalBackupsEnabled()
    {
        return databaseDescriptor.isIncrementalBackupsEnabled();
    }

    public CompactionManager getCompactionManager()
    {
        return databaseDescriptor.getCompactionManager();
    }

    public LocatorConfig getLocatorConfig()
    {
        return databaseDescriptor.getLocatorConfig();
    }

    public IPartitioner getPartitioner()
    {
        return databaseDescriptor.getLocatorConfig().getPartitioner();
    }

    public DatabaseDescriptor getDatabaseDescriptor()
    {
        return databaseDescriptor;
    }

    public SystemKeyspace getSystemKeyspace()
    {
        return databaseDescriptor.getSystemKeyspace();
    }

    public StorageService getStorageService()
    {
        return databaseDescriptor.getStorageService();
    }

    public SSTableReaderFactory getSSTableReaderFactory()
    {
        return databaseDescriptor.getSSTableReaderFactory();
    }

    public SSTableWriterFactory getSSTableWriterFactory()
    {
        return databaseDescriptor.getSSTableWriterFactory();
    }
}

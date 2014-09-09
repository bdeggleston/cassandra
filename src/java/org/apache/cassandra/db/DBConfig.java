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
import org.apache.cassandra.io.sstable.SSTableReaderFactory;
import org.apache.cassandra.io.sstable.SSTableWriterFactory;
import org.apache.cassandra.locator.LocatorConfig;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.memory.MemtablePool;

import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DBConfig
{
    public static final DBConfig instance = new DBConfig();

    private final MemtablePool memtablePool;
    private final long memtableRowOverhead;

    public final ColumnFamilySerializer columnFamilySerializer;
    public final Row.RowSerializer rowSerializer;
    public final Token.Serializer tokenSerializer;
    public final MerkleTree.Hashable.HashableSerializer hashableSerializer;
    public final MerkleTree.Inner.InnerSerializer innerSerializer;
    public final RowPosition.Serializer rowPositionSerializer;
    public final AbstractBounds.Serializer boundsSerializer;
    public final MerkleTree.Serializer merkleTreeSerializer;

    public final long preemptiveOpenInterval;

    public final AbstractType<?> keyComparator;

    public DBConfig()
    {
        memtablePool = DatabaseDescriptor.instance.getMemtableAllocatorPool();
        memtableRowOverhead = estimateRowOverhead(
                Integer.valueOf(System.getProperty("cassandra.memtable_row_overhead_computation_step", "100000")));

        columnFamilySerializer = new ColumnFamilySerializer(DatabaseDescriptor.instance, Tracing.instance, Schema.instance, this);
        // don't change the partitioner after these are instantiated
        rowSerializer = new Row.RowSerializer(LocatorConfig.instance.getPartitioner(), columnFamilySerializer);
        tokenSerializer = new Token.Serializer(LocatorConfig.instance.getPartitioner());
        rowPositionSerializer = new RowPosition.Serializer(LocatorConfig.instance.getPartitioner(), tokenSerializer);
        boundsSerializer = new AbstractBounds.Serializer(tokenSerializer, rowPositionSerializer);
        hashableSerializer = new MerkleTree.Hashable.HashableSerializer(tokenSerializer);
        innerSerializer = new MerkleTree.Inner.InnerSerializer(tokenSerializer, hashableSerializer);
        merkleTreeSerializer = new MerkleTree.Serializer(tokenSerializer, hashableSerializer);

        preemptiveOpenInterval = calculatePreemptiveOpenInterval();
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

    private long calculatePreemptiveOpenInterval()
    {
        long interval = DatabaseDescriptor.instance.getSSTablePreempiveOpenIntervalInMB() * (1L << 20);
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
        return DatabaseDescriptor.instance.isIncrementalBackupsEnabled();
    }

    public CompactionManager getCompactionManager()
    {
        return CompactionManager.instance;
    }

    public LocatorConfig getLocatorConfig()
    {
        return LocatorConfig.instance;
    }

    public IPartitioner getPartitioner()
    {
        return LocatorConfig.instance.getPartitioner();
    }

    public DatabaseDescriptor getDatabaseDescriptor()
    {
        return DatabaseDescriptor.instance;
    }

    public SystemKeyspace getSystemKeyspace()
    {
        return SystemKeyspace.instance;
    }

    public StorageService getStorageService()
    {
        return StorageService.instance;
    }

    public SSTableReaderFactory getSSTableReaderFactory()
    {
        return SSTableReaderFactory.instance;
    }

    public SSTableWriterFactory getSSTableWriterFactory()
    {
        return SSTableWriterFactory.instance;
    }
}

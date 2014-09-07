package org.apache.cassandra.db;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.LongToken;
import org.apache.cassandra.locator.LocatorConfig;
import org.apache.cassandra.utils.ByteBufferUtil;
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

    public DBConfig()
    {
        memtablePool = DatabaseDescriptor.instance.getMemtableAllocatorPool();
        memtableRowOverhead = estimateRowOverhead(
                Integer.valueOf(System.getProperty("cassandra.memtable_row_overhead_computation_step", "100000")));

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
            rows.put(allocator.clone(new BufferDecoratedKey(new LongToken((long) i, LocatorConfig.instance.getPartitioner()), ByteBufferUtil.EMPTY_BYTE_BUFFER), group), val);
        double avgSize = ObjectSizes.measureDeep(rows) / (double) count;
        rowOverhead = (int) ((avgSize - Math.floor(avgSize)) < 0.05 ? Math.floor(avgSize) : Math.ceil(avgSize));
        rowOverhead -= ObjectSizes.measureDeep(new LongToken((long) 0, LocatorConfig.instance.getPartitioner()));
        rowOverhead += AtomicBTreeColumns.EMPTY_SIZE;
        allocator.setDiscarding();
        allocator.setDiscarded();
        return rowOverhead;
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
}

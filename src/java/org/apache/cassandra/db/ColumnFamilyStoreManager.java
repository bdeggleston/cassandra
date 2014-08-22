package org.apache.cassandra.db;

import com.google.common.collect.Iterables;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.ClusterState;
import org.apache.cassandra.streaming.StreamLockfile;
import org.apache.cassandra.tracing.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class ColumnFamilyStoreManager
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyStoreManager.class);

    public static final ColumnFamilyStoreManager instance = new ColumnFamilyStoreManager(
            Schema.instance,
            ClusterState.instance,
            SystemKeyspace.instance,
            CompactionManager.instance,
            CacheService.instance
    );

    private final Schema schema;
    private final ClusterState clusterState;
    private final SystemKeyspace systemKeyspace;
    private final CompactionManager compactionManager;
    private final CacheService cacheService;
    private volatile Tracing tracing;
    public final TaskExecutors taskExecutors;

    public ColumnFamilyStoreManager(Schema schema, ClusterState clusterState, SystemKeyspace systemKeyspace, CompactionManager compactionManager, CacheService cacheService)
    {
        assert schema != null;
        assert clusterState != null;
        assert systemKeyspace != null;
        assert compactionManager != null;
        assert cacheService != null;

        this.schema = schema;
        this.clusterState = clusterState;
        this.systemKeyspace = systemKeyspace;
        this.compactionManager = compactionManager;
        this.cacheService = cacheService;
        this.taskExecutors = new TaskExecutors();
    }

    public void setTracing(Tracing tracing)
    {
        assert this.tracing == null;
        assert tracing != null;
        this.tracing = tracing;
    }

    public void rebuildSecondaryIndex(String ksName, String cfName, String... idxNames)
    {
        ColumnFamilyStore cfs = KeyspaceManager.instance.open(ksName).getColumnFamilyStore(cfName);

        Set<String> indexes = new HashSet<String>(Arrays.asList(idxNames));

        Collection<SSTableReader> sstables = cfs.getSSTables();
        try
        {
            cfs.indexManager.setIndexRemoved(indexes);
            SSTableReader.acquireReferences(sstables);
            logger.info(String.format("User Requested secondary index re-build for %s/%s indexes", ksName, cfName));
            cfs.indexManager.maybeBuildSecondaryIndexes(sstables, indexes);
            cfs.indexManager.setIndexBuilt(indexes);
        }
        finally
        {
            SSTableReader.releaseReferences(sstables);
        }
    }

    public Iterable<ColumnFamilyStore> all()
    {
        List<Iterable<ColumnFamilyStore>> stores = new ArrayList<Iterable<ColumnFamilyStore>>(schema.getKeyspaces().size());
        for (Keyspace keyspace : KeyspaceManager.instance.all())
        {
            stores.add(keyspace.getColumnFamilyStores());
        }
        return Iterables.concat(stores);
    }

    public ColumnFamilyStore createColumnFamilyStore(Keyspace keyspace, String columnFamily, boolean loadSSTables)
    {
        return createColumnFamilyStore(keyspace, columnFamily, clusterState.getPartitioner(), schema.getCFMetaData(keyspace.getName(), columnFamily), loadSSTables);
    }

    public ColumnFamilyStore createColumnFamilyStore(Keyspace keyspace, String columnFamily, IPartitioner partitioner, CFMetaData metadata)
    {
        return createColumnFamilyStore(keyspace, columnFamily, partitioner, metadata, true);
    }

    private synchronized ColumnFamilyStore createColumnFamilyStore(Keyspace keyspace,
                                                                          String columnFamily,
                                                                          IPartitioner partitioner,
                                                                          CFMetaData metadata,
                                                                          boolean loadSSTables)
    {
        // get the max generation number, to prevent generation conflicts
        Directories directories = new Directories(metadata);
        Directories.SSTableLister lister = directories.sstableLister().includeBackups(true);
        List<Integer> generations = new ArrayList<Integer>();
        for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet())
        {
            Descriptor desc = entry.getKey();
            generations.add(desc.generation);
            if (!desc.isCompatible())
                throw new RuntimeException(String.format("Incompatible SSTable found. Current version %s is unable to read file: %s. Please run upgradesstables.",
                        Descriptor.Version.CURRENT, desc));
        }
        Collections.sort(generations);
        int value = (generations.size() > 0) ? (generations.get(generations.size() - 1)) : 0;

        UUID cfId = schema.getId(keyspace.getName(), columnFamily);
        assert tracing != null;
        return new ColumnFamilyStore(keyspace,
                                     columnFamily,
                                     partitioner,
                                     value,
                                     metadata,
                                     directories,
                                     loadSSTables,
                                     cfId,
                                     clusterState,
                                     systemKeyspace,
                                     compactionManager,
                                     cacheService,
                                     tracing,
                                     taskExecutors);
    }

    /**
     * Removes unnecessary files from the cf directory at startup: these include temp files, orphans, zero-length files
     * and compacted sstables. Files that cannot be recognized will be ignored.
     */
    public void scrubDataDirectories(CFMetaData metadata)
    {
        Directories directories = new Directories(metadata);

        // remove any left-behind SSTables from failed/stalled streaming
        FileFilter filter = new FileFilter()
        {
            public boolean accept(File pathname)
            {
                return pathname.getPath().endsWith(StreamLockfile.FILE_EXT);
            }
        };
        for (File dir : directories.getCFDirectories())
        {
            File[] lockfiles = dir.listFiles(filter);
            // lock files can be null if I/O error happens
            if (lockfiles == null || lockfiles.length == 0)
                continue;
            logger.info("Removing SSTables from failed streaming session. Found {} files to cleanup.", lockfiles.length);

            for (File lockfile : lockfiles)
            {
                StreamLockfile streamLockfile = new StreamLockfile(lockfile);
                streamLockfile.cleanup();
                streamLockfile.delete();
            }
        }

        logger.debug("Removing compacted SSTable files from {} (see http://wiki.apache.org/cassandra/MemtableSSTable)", metadata.cfName);

        for (Map.Entry<Descriptor,Set<Component>> sstableFiles : directories.sstableLister().list().entrySet())
        {
            Descriptor desc = sstableFiles.getKey();
            Set<Component> components = sstableFiles.getValue();

            if (desc.type.isTemporary)
            {
                SSTable.delete(desc, components);
                continue;
            }

            File dataFile = new File(desc.filenameFor(Component.DATA));
            if (components.contains(Component.DATA) && dataFile.length() > 0)
                // everything appears to be in order... moving on.
                continue;

            // missing the DATA file! all components are orphaned
            logger.warn("Removing orphans for {}: {}", desc, components);
            for (Component component : components)
            {
                FileUtils.deleteWithConfirm(desc.filenameFor(component));
            }
        }

        // cleanup incomplete saved caches
        Pattern tmpCacheFilePattern = Pattern.compile(metadata.ksName + "-" + metadata.cfName + "-(Key|Row)Cache.*\\.tmp$");
        File dir = new File(clusterState.getSavedCachesLocation());

        if (dir.exists())
        {
            assert dir.isDirectory();
            for (File file : dir.listFiles())
                if (tmpCacheFilePattern.matcher(file.getName()).matches())
                    if (!file.delete())
                        logger.warn("could not delete {}", file.getAbsolutePath());
        }

        // also clean out any index leftovers.
        for (ColumnDefinition def : metadata.allColumns())
        {
            if (def.isIndexed())
            {
                CellNameType indexComparator = SecondaryIndex.getIndexComparator(metadata, def);
                if (indexComparator != null)
                {
                    CFMetaData indexMetadata = CFMetaData.newIndexMetadata(metadata, def, indexComparator);
                    scrubDataDirectories(indexMetadata);
                }
            }
        }


    }

    /**
     * See #{@code StorageService.loadNewSSTables(String, String)} for more info
     *
     * @param ksName The keyspace name
     * @param cfName The columnFamily name
     */
    public synchronized void loadNewSSTables(String ksName, String cfName, KeyspaceManager keyspaceManager)
    {
        /** ks/cf existence checks will be done by open and getCFS methods for us */
        Keyspace keyspace = keyspaceManager.open(ksName);
        keyspace.getColumnFamilyStore(cfName).loadNewSSTables();
    }

    public class TaskExecutors
    {
        public final ExecutorService flushExecutor = new JMXEnabledThreadPoolExecutor(ClusterState.instance.getFlushWriters(),
                                                                                       StageManager.KEEPALIVE,
                                                                                       TimeUnit.SECONDS,
                                                                                       new LinkedBlockingQueue<Runnable>(),
                                                                                       new NamedThreadFactory("MemtableFlushWriter"),
                                                                                       "internal");

        // post-flush executor is single threaded to provide guarantee that any flush Future on a CF will never return until prior flushes have completed
        public final ExecutorService postFlushExecutor = new JMXEnabledThreadPoolExecutor(1,
                                                                                          StageManager.KEEPALIVE,
                                                                                          TimeUnit.SECONDS,
                                                                                          new LinkedBlockingQueue<Runnable>(),
                                                                                          new NamedThreadFactory("MemtablePostFlush"),
                                                                                          "internal");

        public final ExecutorService reclaimExecutor = new JMXEnabledThreadPoolExecutor(1,
                                                                                        StageManager.KEEPALIVE,
                                                                                        TimeUnit.SECONDS,
                                                                                        new LinkedBlockingQueue<Runnable>(),
                                                                                        new NamedThreadFactory("MemtableReclaimMemory"),
                                                                                        "internal");
    }

}

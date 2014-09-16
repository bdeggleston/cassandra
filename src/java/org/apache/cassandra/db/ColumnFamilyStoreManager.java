package org.apache.cassandra.db;

import com.google.common.collect.Iterables;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.tracing.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ColumnFamilyStoreManager
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyStore.class);

    private final DatabaseDescriptor databaseDescriptor;

    public final TaskExecutors taskExecutors;
    public final Directories.DataDirectory[] dataDirectories;

    public ColumnFamilyStoreManager(DatabaseDescriptor databaseDescriptor, Tracing tracing)
    {
        assert tracing != null;
        this.databaseDescriptor = databaseDescriptor;
        String[] locations = databaseDescriptor.getAllDataFileLocations();
        dataDirectories = new Directories.DataDirectory[locations.length];
        for (int i = 0; i < locations.length; ++i)
            dataDirectories[i] = new Directories.DataDirectory(new File(locations[i]));
        taskExecutors = new TaskExecutors(databaseDescriptor, tracing);
    }

    public ColumnFamilyStore createColumnFamilyStore(Keyspace keyspace, String columnFamily, boolean loadSSTables)
    {
        return createColumnFamilyStore(keyspace, columnFamily, databaseDescriptor.getLocatorConfig().getPartitioner(), databaseDescriptor.getSchema().getCFMetaData(keyspace.getName(), columnFamily), loadSSTables);
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
        Directories directories = new Directories(metadata, databaseDescriptor, databaseDescriptor.getStorageService(), databaseDescriptor.getKeyspaceManager(), dataDirectories);
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

        return new ColumnFamilyStore(keyspace,
                                     columnFamily,
                                     partitioner,
                                     value,
                                     metadata,
                                     directories,
                                     loadSSTables,
                                     taskExecutors,
                                     databaseDescriptor.getSchema(),
                                     databaseDescriptor.getTracing(),
                                     databaseDescriptor.getCFMetaDataFactory(),
                                     this,
                                     databaseDescriptor.getKeyspaceManager(),
                                     databaseDescriptor.getDBConfig(),
                                     databaseDescriptor.getCommitLog(),
                                     databaseDescriptor.getCacheService(),
                                     databaseDescriptor.getStorageServiceExecutors(),
                                     databaseDescriptor.getMutationFactory());
    }

    /**
     * Replacing compacted sstables is atomic as far as observers of DataTracker are concerned, but not on the
     * filesystem: first the new sstables are renamed to "live" status (i.e., the tmp marker is removed), then
     * their ancestors are removed.
     *
     * If an unclean shutdown happens at the right time, we can thus end up with both the new ones and their
     * ancestors "live" in the system.  This is harmless for normal data, but for counters it can cause overcounts.
     *
     * To prevent this, we record sstables being compacted in the system keyspace.  If we find unfinished
     * compactions, we remove the new ones (since those may be incomplete -- under LCS, we may create multiple
     * sstables from any given ancestor).
     */
    public void removeUnfinishedCompactionLeftovers(CFMetaData metadata, Map<Integer, UUID> unfinishedCompactions)
    {
        Directories directories = new Directories(metadata, databaseDescriptor, databaseDescriptor.getStorageService(), databaseDescriptor.getKeyspaceManager(), dataDirectories);

        Set<Integer> allGenerations = new HashSet<>();
        for (Descriptor desc : directories.sstableLister().list().keySet())
            allGenerations.add(desc.generation);

        // sanity-check unfinishedCompactions
        Set<Integer> unfinishedGenerations = unfinishedCompactions.keySet();
        if (!allGenerations.containsAll(unfinishedGenerations))
        {
            HashSet<Integer> missingGenerations = new HashSet<>(unfinishedGenerations);
            missingGenerations.removeAll(allGenerations);
            logger.debug("Unfinished compactions of {}.{} reference missing sstables of generations {}",
                         metadata.ksName, metadata.cfName, missingGenerations);
        }

        // remove new sstables from compactions that didn't complete, and compute
        // set of ancestors that shouldn't exist anymore
        Set<Integer> completedAncestors = new HashSet<>();
        for (Map.Entry<Descriptor, Set<Component>> sstableFiles : directories.sstableLister().skipTemporary(true).list().entrySet())
        {
            Descriptor desc = sstableFiles.getKey();

            Set<Integer> ancestors;
            try
            {
                CompactionMetadata compactionMetadata = (CompactionMetadata) desc.getMetadataSerializer().deserialize(desc, MetadataType.COMPACTION);
                ancestors = compactionMetadata.ancestors;
            }
            catch (IOException e)
            {
                throw new FSReadError(e, desc.filenameFor(Component.STATS));
            }

            if (!ancestors.isEmpty()
                    && unfinishedGenerations.containsAll(ancestors)
                    && allGenerations.containsAll(ancestors))
            {
                // any of the ancestors would work, so we'll just lookup the compaction task ID with the first one
                UUID compactionTaskID = unfinishedCompactions.get(ancestors.iterator().next());
                assert compactionTaskID != null;
                logger.debug("Going to delete unfinished compaction product {}", desc);
                SSTable.delete(desc, sstableFiles.getValue());
                databaseDescriptor.getSystemKeyspace().finishCompaction(compactionTaskID);
            }
            else
            {
                completedAncestors.addAll(ancestors);
            }
        }

        // remove old sstables from compactions that did complete
        for (Map.Entry<Descriptor, Set<Component>> sstableFiles : directories.sstableLister().list().entrySet())
        {
            Descriptor desc = sstableFiles.getKey();
            if (completedAncestors.contains(desc.generation))
            {
                // if any of the ancestors were participating in a compaction, finish that compaction
                logger.debug("Going to delete leftover compaction ancestor {}", desc);
                SSTable.delete(desc, sstableFiles.getValue());
                UUID compactionTaskID = unfinishedCompactions.get(desc.generation);
                if (compactionTaskID != null)
                    databaseDescriptor.getSystemKeyspace().finishCompaction(unfinishedCompactions.get(desc.generation));
            }
        }
    }

    /**
     * See #{@code StorageService.loadNewSSTables(String, String)} for more info
     *
     * @param ksName The keyspace name
     * @param cfName The columnFamily name
     */
    public synchronized void loadNewSSTables(String ksName, String cfName)
    {
        /** ks/cf existence checks will be done by open and getCFS methods for us */
        Keyspace keyspace = databaseDescriptor.getKeyspaceManager().open(ksName);
        keyspace.getColumnFamilyStore(cfName).loadNewSSTables();
    }

    public void rebuildSecondaryIndex(String ksName, String cfName, String... idxNames)
    {
        ColumnFamilyStore cfs = databaseDescriptor.getKeyspaceManager().open(ksName).getColumnFamilyStore(cfName);

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
        List<Iterable<ColumnFamilyStore>> stores = new ArrayList<Iterable<ColumnFamilyStore>>(databaseDescriptor.getSchema().getKeyspaces().size());
        for (Keyspace keyspace : databaseDescriptor.getKeyspaceManager().all())
        {
            stores.add(keyspace.getColumnFamilyStores());
        }
        return Iterables.concat(stores);
    }

    public void shutdownPostFlushExecutor() throws InterruptedException
    {
        taskExecutors.postFlushExecutor.shutdown();
        taskExecutors.postFlushExecutor.awaitTermination(60, TimeUnit.SECONDS);
    }

    public class TaskExecutors
    {
        public final ExecutorService flushExecutor;
        // post-flush executor is single threaded to provide guarantee that any flush Future on a CF will never return until prior flushes have completed
        public final ExecutorService postFlushExecutor;
        public final ExecutorService reclaimExecutor;


        public TaskExecutors(DatabaseDescriptor databaseDescriptor, Tracing tracing)
        {

            flushExecutor = new JMXEnabledThreadPoolExecutor(databaseDescriptor.getFlushWriters(),
                                                             StageManager.KEEPALIVE,
                                                             TimeUnit.SECONDS,
                                                             new LinkedBlockingQueue<Runnable>(),
                                                             new NamedThreadFactory("MemtableFlushWriter"),
                                                             "internal",
                                                             tracing,
                                                             databaseDescriptor.shouldInitializeJMX());
            postFlushExecutor = new JMXEnabledThreadPoolExecutor(1,
                                                                 StageManager.KEEPALIVE,
                                                                 TimeUnit.SECONDS,
                                                                 new LinkedBlockingQueue<Runnable>(),
                                                                 new NamedThreadFactory("MemtablePostFlush"),
                                                                 "internal",
                                                                 tracing,
                                                                 databaseDescriptor.shouldInitializeJMX());
            reclaimExecutor = new JMXEnabledThreadPoolExecutor(1, StageManager.KEEPALIVE,
                                                               TimeUnit.SECONDS,
                                                               new LinkedBlockingQueue<Runnable>(),
                                                               new NamedThreadFactory("MemtableReclaimMemory"),
                                                               "internal",
                                                               tracing,
                                                               databaseDescriptor.shouldInitializeJMX());
        }
    }

}

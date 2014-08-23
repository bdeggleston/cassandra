/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.config;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Longs;
import org.apache.cassandra.auth.*;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.service.*;
import org.apache.cassandra.sink.SinkManager;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.triggers.TriggerExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.config.Config.RequestSchedulerId;
import org.apache.cassandra.config.EncryptionOptions.ClientEncryptionOptions;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.IAllocator;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.EndpointSnitchInfo;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.SeedProvider;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.scheduler.IRequestScheduler;
import org.apache.cassandra.scheduler.NoScheduler;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.NativePool;
import org.apache.cassandra.utils.memory.MemtablePool;
import org.apache.cassandra.utils.memory.SlabPool;

public class DatabaseDescriptor
{
    private static final Logger logger = LoggerFactory.getLogger(DatabaseDescriptor.class);

    /**
     * Tokens are serialized in a Gossip VersionedValue String.  VV are restricted to 64KB
     * when we send them over the wire, which works out to about 1700 tokens.
     */
    private static final int MAX_NUM_TOKENS = 1536;

    public static volatile DatabaseDescriptor instance;

    static
    {
        instance = create();
        CommitLog commitLog = CommitLog.instance;
        QueryProcessor queryProcessor = QueryProcessor.instance;
        TriggerExecutor triggerExecutor = TriggerExecutor.instance;

        SinkManager sinkManager = SinkManager.instance;
        StreamManager streamManager = StreamManager.instance;
        StorageServiceTasks storageServiceTasks = StorageServiceTasks.instance;

        Schema schema = Schema.instance;
        SystemKeyspace systemKeyspace = SystemKeyspace.instance;
        DefsTables defsTables = DefsTables.instance;

        StageManager stageManager = StageManager.instance;
        MessagingService messagingService = MessagingService.instance;
        Gossiper gossiper = Gossiper.instance;
        LoadBroadcaster loadBroadcaster = LoadBroadcaster.instance;

        IFailureDetector failureDetector = FailureDetector.instance;
        MigrationManager migrationManager = MigrationManager.instance;
        instance.loadSnitch();

        ClusterState clusterState = ClusterState.instance;
        ActiveRepairService activeRepairService = ActiveRepairService.instance;
        CompactionManager compactionManager = CompactionManager.instance;
        defsTables.init(migrationManager, compactionManager);

        HintedHandOffManager hintedHandOffManager = HintedHandOffManager.instance;
        StorageProxy storageProxy = StorageProxy.instance;
        Tracing tracing = Tracing.instance;

        CacheService cacheService = CacheService.instance;
        ColumnFamilyStoreManager columnFamilyStoreManager = ColumnFamilyStoreManager.instance;

        KeyspaceManager keyspaceManager = KeyspaceManager.instance;
        systemKeyspace.setKeyspaceManager(keyspaceManager);

        Auth auth = Auth.instance;
        BatchlogManager batchlogManager = BatchlogManager.instance;

        StorageService storageService = StorageService.instance;
        Gossiper.instance.init(FailureDetector.instance, StorageService.instance);

        PendingRangeCalculatorService pendingRangeCalculatorService = PendingRangeCalculatorService.instance;

        // ghetto workaround for the above not being visible to other threads
        new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    // this sucks, but until we can clean up after previous runs before starting everything
                    // it keeps tests from randomly failing spectacularly, and having to hack up the storage system
                    int initWait = Integer.parseInt(System.getProperty("descriptor.start.wait", "100"));
                    Thread.sleep(initWait);
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }

                CommitLog.instance.init(StorageService.instance, KeyspaceManager.instance);
                HintedHandOffManager.instance.loadHintStore(KeyspaceManager.instance);
            }
        }).start();
    }

    public static DatabaseDescriptor init()
    {
        return instance;
    }

    private final boolean clientMode;
    private IEndpointSnitch snitch;
    private InetAddress listenAddress; // leave null so we can fall through to getLocalHost
    private InetAddress broadcastAddress;
    private InetAddress rpcAddress;
    private InetAddress broadcastRpcAddress;
    private SeedProvider seedProvider;
    private IInternodeAuthenticator internodeAuthenticator;

    /* Hashing strategy Random or OPHF */
    private IPartitioner<?> partitioner;
    private String paritionerName;

    private Config.DiskAccessMode indexAccessMode;

    private final Config conf;

    private IAuthenticator authenticator = new AllowAllAuthenticator();
    private IAuthorizer authorizer = new AllowAllAuthorizer();

    private IRequestScheduler requestScheduler;
    private RequestSchedulerId requestSchedulerId;
    private RequestSchedulerOptions requestSchedulerOptions;

    private long keyCacheSizeInMB;
    private long counterCacheSizeInMB;
    private IAllocator memoryAllocator;
    private long indexSummaryCapacityInMB;

    private String localDC;
    private Comparator<InetAddress> localComparator;

    @Deprecated
    public static DatabaseDescriptor create()
    {
        return create(Config.isClientMode());
    }

    public static DatabaseDescriptor create(boolean clientMode)
    {
        DatabaseDescriptor dd = new DatabaseDescriptor(clientMode);

        return dd;
    }

    private DatabaseDescriptor(boolean clientMode)
    {
        // In client mode, we use a default configuration. Note that the fields of this class will be
        // left unconfigured however (the partitioner or localDC will be null for instance) so this
        // should be used with care.
        try
        {
            this.clientMode = clientMode;
            if (this.clientMode)
            {
                conf = new Config();
                // at least we have to set memoryAllocator to open SSTable in client mode
                memoryAllocator = FBUtilities.newOffHeapAllocator(conf.memory_allocator);
            } else
            {
                conf = loadConfig();
                applyConfig(conf);
            }
        }
        catch (ConfigurationException e)
        {
            logger.error("Fatal configuration error", e);
            System.err.println(e.getMessage() + "\nFatal configuration error; unable to start. See log for stacktrace.");
            throw new IllegalStateException(e);
        }
        catch (Exception e)
        {
            logger.error("Fatal error during configuration loading", e);
            System.err.println(e.getMessage() + "\nFatal error during configuration loading; unable to start. See log for stacktrace.");
            throw new IllegalStateException(e);
        }
    }

    @VisibleForTesting
    public Config loadConfig() throws ConfigurationException
    {
        String loaderClass = System.getProperty("cassandra.config.loader");
        ConfigurationLoader loader = loaderClass == null
                                   ? new YamlConfigurationLoader()
                                   : FBUtilities.<ConfigurationLoader>construct(loaderClass, "configuration loading");
        return loader.loadConfig();
    }

    private void applyConfig(Config config) throws ConfigurationException
    {
        if (conf.commitlog_sync == null)
        {
            throw new ConfigurationException("Missing required directive CommitLogSync");
        }

        if (conf.commitlog_sync == Config.CommitLogSync.batch)
        {
            if (conf.commitlog_sync_batch_window_in_ms == null)
            {
                throw new ConfigurationException("Missing value for commitlog_sync_batch_window_in_ms: Double expected.");
            }
            else if (conf.commitlog_sync_period_in_ms != null)
            {
                throw new ConfigurationException("Batch sync specified, but commitlog_sync_period_in_ms found. Only specify commitlog_sync_batch_window_in_ms when using batch sync");
            }
            logger.debug("Syncing log with a batch window of {}", conf.commitlog_sync_batch_window_in_ms);
        }
        else
        {
            if (conf.commitlog_sync_period_in_ms == null)
            {
                throw new ConfigurationException("Missing value for commitlog_sync_period_in_ms: Integer expected");
            }
            else if (conf.commitlog_sync_batch_window_in_ms != null)
            {
                throw new ConfigurationException("commitlog_sync_period_in_ms specified, but commitlog_sync_batch_window_in_ms found.  Only specify commitlog_sync_period_in_ms when using periodic sync.");
            }
            logger.debug("Syncing log with a period of {}", conf.commitlog_sync_period_in_ms);
        }

        if (conf.commitlog_total_space_in_mb == null)
            conf.commitlog_total_space_in_mb = hasLargeAddressSpace() ? 8192 : 32;

        if (FBUtilities.isUnix())
        {
            /* evaluate the DiskAccessMode Config directive, which also affects indexAccessMode selection */
            if (conf.disk_access_mode == Config.DiskAccessMode.auto)
            {
                conf.disk_access_mode = hasLargeAddressSpace() ? Config.DiskAccessMode.mmap : Config.DiskAccessMode.standard;
                indexAccessMode = conf.disk_access_mode;
                logger.info("DiskAccessMode 'auto' determined to be {}, indexAccessMode is {}", conf.disk_access_mode, indexAccessMode);
            }
            else if (conf.disk_access_mode == Config.DiskAccessMode.mmap_index_only)
            {
                conf.disk_access_mode = Config.DiskAccessMode.standard;
                indexAccessMode = Config.DiskAccessMode.mmap;
                logger.info("DiskAccessMode is {}, indexAccessMode is {}", conf.disk_access_mode, indexAccessMode);
            }
            else
            {
                indexAccessMode = conf.disk_access_mode;
                logger.info("DiskAccessMode is {}, indexAccessMode is {}", conf.disk_access_mode, indexAccessMode);
            }
        }
        // Always force standard mode access on Windows - CASSANDRA-6993. Windows won't allow deletion of hard-links to files that
        // are memory-mapped which causes trouble with snapshots.
        else
        {
            conf.disk_access_mode = Config.DiskAccessMode.standard;
            indexAccessMode = conf.disk_access_mode;
            logger.info("Non-unix environment detected.  DiskAccessMode set to {}, indexAccessMode {}", conf.disk_access_mode, indexAccessMode);
        }

        /* Authentication and authorization backend, implementing IAuthenticator and IAuthorizer */
        if (conf.authenticator != null)
            authenticator = FBUtilities.newAuthenticator(conf.authenticator);

        if (conf.authorizer != null)
            authorizer = FBUtilities.newAuthorizer(conf.authorizer);

        if (authenticator instanceof AllowAllAuthenticator && !(authorizer instanceof AllowAllAuthorizer))
            throw new ConfigurationException("AllowAllAuthenticator can't be used with " +  conf.authorizer);

        if (conf.internode_authenticator != null)
            internodeAuthenticator = FBUtilities.construct(conf.internode_authenticator, "internode_authenticator");
        else
            internodeAuthenticator = new AllowAllInternodeAuthenticator();

        authenticator.validateConfiguration();
        authorizer.validateConfiguration();
        internodeAuthenticator.validateConfiguration();

        /* Hashing strategy */
        if (conf.partitioner == null)
        {
            throw new ConfigurationException("Missing directive: partitioner");
        }
        try
        {
            partitioner = FBUtilities.newPartitioner(System.getProperty("cassandra.partitioner", conf.partitioner));
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Invalid partitioner class " + conf.partitioner);
        }
        paritionerName = partitioner.getClass().getCanonicalName();

        if (conf.max_hint_window_in_ms == null)
        {
            throw new ConfigurationException("max_hint_window_in_ms cannot be set to null");
        }

        /* phi convict threshold for FailureDetector */
        if (conf.phi_convict_threshold < 5 || conf.phi_convict_threshold > 16)
        {
            throw new ConfigurationException("phi_convict_threshold must be between 5 and 16");
        }

        /* Thread per pool */
        if (conf.concurrent_reads != null && conf.concurrent_reads < 2)
        {
            throw new ConfigurationException("concurrent_reads must be at least 2");
        }

        if (conf.concurrent_writes != null && conf.concurrent_writes < 2)
        {
            throw new ConfigurationException("concurrent_writes must be at least 2");
        }

        if (conf.concurrent_counter_writes != null && conf.concurrent_counter_writes < 2)
            throw new ConfigurationException("concurrent_counter_writes must be at least 2");

        if (conf.concurrent_replicates != null)
            logger.warn("concurrent_replicates has been deprecated and should be removed from cassandra.yaml");

        if (conf.file_cache_size_in_mb == null)
            conf.file_cache_size_in_mb = Math.min(512, (int) (Runtime.getRuntime().maxMemory() / (4 * 1048576)));

        if (conf.memtable_offheap_space_in_mb == null)
            conf.memtable_offheap_space_in_mb = (int) (Runtime.getRuntime().maxMemory() / (4 * 1048576));
        if (conf.memtable_offheap_space_in_mb < 0)
            throw new ConfigurationException("memtable_offheap_space_in_mb must be positive");
        // for the moment, we default to twice as much on-heap space as off-heap, as heap overhead is very large
        if (conf.memtable_heap_space_in_mb == null)
            conf.memtable_heap_space_in_mb = (int) (Runtime.getRuntime().maxMemory() / (4 * 1048576));
        if (conf.memtable_heap_space_in_mb <= 0)
            throw new ConfigurationException("memtable_heap_space_in_mb must be positive");
        logger.info("Global memtable on-heap threshold is enabled at {}MB", conf.memtable_heap_space_in_mb);
        if (conf.memtable_offheap_space_in_mb == 0)
            logger.info("Global memtable off-heap threshold is disabled, HeapAllocator will be used instead");
        else
            logger.info("Global memtable off-heap threshold is enabled at {}MB", conf.memtable_offheap_space_in_mb);

        /* Local IP, hostname or interface to bind services to */
        if (conf.listen_address != null && conf.listen_interface != null)
        {
            throw new ConfigurationException("Set listen_address OR listen_interface, not both");
        }
        else if (conf.listen_address != null)
        {
            try
            {
                listenAddress = InetAddress.getByName(conf.listen_address);
            }
            catch (UnknownHostException e)
            {
                throw new ConfigurationException("Unknown listen_address '" + conf.listen_address + "'");
            }

            if (listenAddress.isAnyLocalAddress())
                throw new ConfigurationException("listen_address cannot be a wildcard address (" + conf.listen_address + ")!");
        }
        else if (conf.listen_interface != null)
        {
            try
            {
                Enumeration<InetAddress> addrs = NetworkInterface.getByName(conf.listen_interface).getInetAddresses();
                listenAddress = addrs.nextElement();
                if (addrs.hasMoreElements())
                    throw new ConfigurationException("Interface " + conf.listen_interface +" can't have more than one address");
            }
            catch (SocketException e)
            {
                throw new ConfigurationException("Unknown network interface in listen_interface " + conf.listen_interface);
            }

        }

        /* Gossip Address to broadcast */
        if (conf.broadcast_address != null)
        {
            try
            {
                broadcastAddress = InetAddress.getByName(conf.broadcast_address);
            }
            catch (UnknownHostException e)
            {
                throw new ConfigurationException("Unknown broadcast_address '" + conf.broadcast_address + "'");
            }

            if (broadcastAddress.isAnyLocalAddress())
                throw new ConfigurationException("broadcast_address cannot be a wildcard address (" + conf.broadcast_address + ")!");
        }

        /* Local IP, hostname or interface to bind RPC server to */
        if (conf.rpc_address != null && conf.rpc_interface != null)
        {
            throw new ConfigurationException("Set rpc_address OR rpc_interface, not both");
        }
        else if (conf.rpc_address != null)
        {
            try
            {
                rpcAddress = InetAddress.getByName(conf.rpc_address);
            }
            catch (UnknownHostException e)
            {
                throw new ConfigurationException("Unknown host in rpc_address " + conf.rpc_address);
            }
        }
        else if (conf.rpc_interface != null)
        {
            try
            {
                Enumeration<InetAddress> addrs = NetworkInterface.getByName(conf.rpc_interface).getInetAddresses();
                rpcAddress = addrs.nextElement();
                if (addrs.hasMoreElements())
                    throw new ConfigurationException("Interface " + conf.rpc_interface +" can't have more than one address");
            }
            catch (SocketException e)
            {
                throw new ConfigurationException("Unknown network interface in rpc_interface " + conf.rpc_interface);
            }
        }
        else
        {
            rpcAddress = getLocalAddress();
        }

        /* RPC address to broadcast */
        if (conf.broadcast_rpc_address != null)
        {
            try
            {
                broadcastRpcAddress = InetAddress.getByName(conf.broadcast_rpc_address);
            }
            catch (UnknownHostException e)
            {
                throw new ConfigurationException("Unknown broadcast_rpc_address '" + conf.broadcast_rpc_address + "'");
            }

            if (broadcastRpcAddress.isAnyLocalAddress())
                throw new ConfigurationException("broadcast_rpc_address cannot be a wildcard address (" + conf.broadcast_rpc_address + ")!");
        }
        else
        {
            if (rpcAddress.isAnyLocalAddress())
                throw new ConfigurationException("If rpc_address is set to a wildcard address (" + conf.rpc_address + "), then " +
                                                 "you must set broadcast_rpc_address to a value other than " + conf.rpc_address);
            broadcastRpcAddress = rpcAddress;
        }

        if (conf.thrift_framed_transport_size_in_mb <= 0)
            throw new ConfigurationException("thrift_framed_transport_size_in_mb must be positive");

        if (conf.native_transport_max_frame_size_in_mb <= 0)
            throw new ConfigurationException("native_transport_max_frame_size_in_mb must be positive");

        /* end point snitch */
        if (conf.endpoint_snitch == null)
        {
            throw new ConfigurationException("Missing endpoint_snitch directive");
        }

        /* Request Scheduler setup */
        requestSchedulerOptions = conf.request_scheduler_options;
        if (conf.request_scheduler != null)
        {
            try
            {
                if (requestSchedulerOptions == null)
                {
                    requestSchedulerOptions = new RequestSchedulerOptions();
                }
                Class<?> cls = Class.forName(conf.request_scheduler);
                requestScheduler = (IRequestScheduler) cls.getConstructor(RequestSchedulerOptions.class).newInstance(requestSchedulerOptions);
            }
            catch (ClassNotFoundException e)
            {
                throw new ConfigurationException("Invalid Request Scheduler class " + conf.request_scheduler);
            }
            catch (Exception e)
            {
                throw new ConfigurationException("Unable to instantiate request scheduler", e);
            }
        }
        else
        {
            requestScheduler = new NoScheduler();
        }

        if (conf.request_scheduler_id == RequestSchedulerId.keyspace)
        {
            requestSchedulerId = conf.request_scheduler_id;
        }
        else
        {
            // Default to Keyspace
            requestSchedulerId = RequestSchedulerId.keyspace;
        }

        // if data dirs, commitlog dir, or saved caches dir are set in cassandra.yaml, use that.  Otherwise,
        // use -Dcassandra.storagedir (set in cassandra-env.sh) as the parent dir for data/, commitlog/, and saved_caches/
        if (conf.commitlog_directory == null)
        {
            conf.commitlog_directory = System.getProperty("cassandra.storagedir", null);
            if (conf.commitlog_directory == null)
                throw new ConfigurationException("commitlog_directory is missing and -Dcassandra.storagedir is not set");
            conf.commitlog_directory += File.separator + "commitlog";
        }
        if (conf.saved_caches_directory == null)
        {
            conf.saved_caches_directory = System.getProperty("cassandra.storagedir", null);
            if (conf.saved_caches_directory == null)
                throw new ConfigurationException("saved_caches_directory is missing and -Dcassandra.storagedir is not set");
            conf.saved_caches_directory += File.separator + "saved_caches";
        }
        if (conf.data_file_directories == null)
        {
            String defaultDataDir = System.getProperty("cassandra.storagedir", null);
            if (defaultDataDir == null)
                throw new ConfigurationException("data_file_directories is not missing and -Dcassandra.storagedir is not set");
            conf.data_file_directories = new String[]{ defaultDataDir + File.separator + "data" };
        }

        /* data file and commit log directories. they get created later, when they're needed. */
        for (String datadir : conf.data_file_directories)
        {
            if (datadir.equals(conf.commitlog_directory))
                throw new ConfigurationException("commitlog_directory must not be the same as any data_file_directories");
            if (datadir.equals(conf.saved_caches_directory))
                throw new ConfigurationException("saved_caches_directory must not be the same as any data_file_directories");
        }

        if (conf.commitlog_directory.equals(conf.saved_caches_directory))
            throw new ConfigurationException("saved_caches_directory must not be the same as the commitlog_directory");

        if (conf.memtable_flush_writers == null)
            conf.memtable_flush_writers = Math.min(8, Math.max(2, Math.min(FBUtilities.getAvailableProcessors(), conf.data_file_directories.length)));

        if (conf.memtable_flush_writers < 1)
            throw new ConfigurationException("memtable_flush_writers must be at least 1");

        if (conf.memtable_cleanup_threshold == null)
            conf.memtable_cleanup_threshold = (float) (1.0 / (1 + conf.memtable_flush_writers));

        if (conf.memtable_cleanup_threshold < 0.01f)
            throw new ConfigurationException("memtable_cleanup_threshold must be >= 0.01");
        if (conf.memtable_cleanup_threshold > 0.99f)
            throw new ConfigurationException("memtable_cleanup_threshold must be <= 0.99");
        if (conf.memtable_cleanup_threshold < 0.1f)
            logger.warn("memtable_cleanup_threshold is set very low, which may cause performance degradation");

        if (conf.concurrent_compactors == null)
            conf.concurrent_compactors = Math.min(8, Math.max(2, Math.min(FBUtilities.getAvailableProcessors(), conf.data_file_directories.length)));

        if (conf.concurrent_compactors <= 0)
            throw new ConfigurationException("concurrent_compactors should be strictly greater than 0");

        if (conf.initial_token != null)
            for (String token : tokensFromString(conf.initial_token))
                partitioner.getTokenFactory().validate(token);

        if (conf.num_tokens == null)
        	conf.num_tokens = 1;
        else if (conf.num_tokens > MAX_NUM_TOKENS)
            throw new ConfigurationException(String.format("A maximum number of %d tokens per node is supported", MAX_NUM_TOKENS));

        try
        {
            // if key_cache_size_in_mb option was set to "auto" then size of the cache should be "min(5% of Heap (in MB), 100MB)
            keyCacheSizeInMB = (conf.key_cache_size_in_mb == null)
                ? Math.min(Math.max(1, (int) (Runtime.getRuntime().totalMemory() * 0.05 / 1024 / 1024)), 100)
                : conf.key_cache_size_in_mb;

            if (keyCacheSizeInMB < 0)
                throw new NumberFormatException(); // to escape duplicating error message
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException("key_cache_size_in_mb option was set incorrectly to '"
                    + conf.key_cache_size_in_mb + "', supported values are <integer> >= 0.");
        }

        try
        {
            // if counter_cache_size_in_mb option was set to "auto" then size of the cache should be "min(2.5% of Heap (in MB), 50MB)
            counterCacheSizeInMB = (conf.counter_cache_size_in_mb == null)
                    ? Math.min(Math.max(1, (int) (Runtime.getRuntime().totalMemory() * 0.025 / 1024 / 1024)), 50)
                    : conf.counter_cache_size_in_mb;

            if (counterCacheSizeInMB < 0)
                throw new NumberFormatException(); // to escape duplicating error message
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException("counter_cache_size_in_mb option was set incorrectly to '"
                    + conf.counter_cache_size_in_mb + "', supported values are <integer> >= 0.");
        }

        // if set to empty/"auto" then use 5% of Heap size
        indexSummaryCapacityInMB = (conf.index_summary_capacity_in_mb == null)
            ? Math.max(1, (int) (Runtime.getRuntime().totalMemory() * 0.05 / 1024 / 1024))
            : conf.index_summary_capacity_in_mb;

        if (indexSummaryCapacityInMB < 0)
            throw new ConfigurationException("index_summary_capacity_in_mb option was set incorrectly to '"
                    + conf.index_summary_capacity_in_mb + "', it should be a non-negative integer.");

        memoryAllocator = FBUtilities.newOffHeapAllocator(conf.memory_allocator);

        if(conf.encryption_options != null)
        {
            logger.warn("Please rename encryption_options as server_encryption_options in the yaml");
            //operate under the assumption that server_encryption_options is not set in yaml rather than both
            conf.server_encryption_options = conf.encryption_options;
        }

        /* Load the seeds for node contact points */
        if (conf.seed_provider == null)
        {
            throw new ConfigurationException("seeds configuration is missing; a minimum of one seed is required.");
        }
        try
        {
            Class<?> seedProviderClass = Class.forName(conf.seed_provider.class_name);
            seedProvider = (SeedProvider)seedProviderClass.getConstructor(Map.class).newInstance(conf.seed_provider.parameters);
        }
        // there are about 5 checked exceptions that could be thrown here.
        catch (Exception e)
        {
            logger.error("Fatal configuration error", e);
            System.err.println(e.getMessage() + "\nFatal configuration error; unable to start server.  See log for stacktrace.");
            System.exit(1);
        }
        if (seedProvider.getSeeds().size() == 0)
            throw new ConfigurationException("The seed provider lists no seeds.");
    }

    public void loadSnitch()
    {
        try
        {
            snitch = createEndpointSnitch(conf.endpoint_snitch);
            localDC = snitch.getDatacenter(getBroadcastAddress());
            localComparator = new Comparator<InetAddress>()
            {
                public int compare(InetAddress endpoint1, InetAddress endpoint2)
                {
                    boolean local1 = localDC.equals(snitch.getDatacenter(endpoint1));
                    boolean local2 = localDC.equals(snitch.getDatacenter(endpoint2));
                    if (local1 && !local2)
                        return -1;
                    if (local2 && !local1)
                        return 1;
                    return 0;
                }
            };

        }
        catch (ConfigurationException e)
        {
            logger.error("Fatal configuration error, unable to create snitch", e);
            System.err.println(e.getMessage() + "\nFatal configuration error; unable to start. See log for stacktrace.");
            throw new IllegalStateException(e);
        }
        EndpointSnitchInfo.create();
    }

    public boolean isClientMode()
    {
        return clientMode;
    }


    private IEndpointSnitch createEndpointSnitch(String snitchClassName) throws ConfigurationException
    {
        if (!snitchClassName.contains("."))
            snitchClassName = "org.apache.cassandra.locator." + snitchClassName;
        IEndpointSnitch snitch = FBUtilities.construct(snitchClassName, "snitch");
        return conf.dynamic_snitch ? new DynamicEndpointSnitch(snitch, null, getDynamicUpdateInterval(), getDynamicResetInterval(), getDynamicBadnessThreshold()) : snitch;
    }

    /** load keyspace (keyspace) definitions, but do not initialize the keyspace instances. */
    public void loadSchemas()
    {
        ColumnFamilyStore schemaCFS = SystemKeyspace.instance.schemaCFS(SystemKeyspace.SCHEMA_KEYSPACES_CF);

        // if keyspace with definitions is empty try loading the old way
        if (schemaCFS.estimateKeys() == 0)
        {
            logger.info("Couldn't detect any schema definitions in local storage.");
            // peek around the data directories to see if anything is there.
            if (hasExistingNoSystemTables())
                logger.info("Found keyspace data in data directories. Consider using cqlsh to define your schema.");
            else
                logger.info("To create keyspaces and column families, see 'help create' in cqlsh.");
        }
        else
        {
            Schema.instance.load(DefsTables.instance.loadFromKeyspace());
        }

        Schema.instance.updateVersion();
    }

    private boolean hasExistingNoSystemTables()
    {
        for (String dataDir : getAllDataFileLocations())
        {
            File dataPath = new File(dataDir);
            if (dataPath.exists() && dataPath.isDirectory())
            {
                // see if there are other directories present.
                int dirCount = dataPath.listFiles(new FileFilter()
                {
                    public boolean accept(File pathname)
                    {
                        return (pathname.isDirectory() && !Schema.systemKeyspaceNames.contains(pathname.getName()));
                    }
                }).length;

                if (dirCount > 0)
                    return true;
            }
        }

        return false;
    }

    public IAuthenticator getAuthenticator()
    {
        return authenticator;
    }

    public IAuthorizer getAuthorizer()
    {
        return authorizer;
    }

    public int getPermissionsValidity()
    {
        return conf.permissions_validity_in_ms;
    }

    public int getThriftFramedTransportSize()
    {
        return conf.thrift_framed_transport_size_in_mb * 1024 * 1024;
    }

    /**
     * Creates all storage-related directories.
     */
    public void createAllDirectories()
    {
        try
        {
            if (conf.data_file_directories.length == 0)
                throw new ConfigurationException("At least one DataFileDirectory must be specified");

            for (String dataFileDirectory : conf.data_file_directories)
            {
                FileUtils.createDirectory(dataFileDirectory);
            }

            if (conf.commitlog_directory == null)
                throw new ConfigurationException("commitlog_directory must be specified");

            FileUtils.createDirectory(conf.commitlog_directory);

            if (conf.saved_caches_directory == null)
                throw new ConfigurationException("saved_caches_directory must be specified");

            FileUtils.createDirectory(conf.saved_caches_directory);
        }
        catch (ConfigurationException e)
        {
            logger.error("Fatal error: {}", e.getMessage());
            System.err.println("Bad configuration; unable to start server");
            System.exit(1);
        }
        catch (FSWriteError e)
        {
            logger.error("Fatal error: {}", e.getMessage());
            System.err.println(e.getCause().getMessage() + "; unable to start server");
            System.exit(1);
        }
    }

    public IPartitioner<?> getPartitioner()
    {
        return partitioner;
    }

    public String getPartitionerName()
    {
        return paritionerName;
    }

    /* For tests ONLY, don't use otherwise or all hell will break loose */
    public void setPartitioner(IPartitioner<?> newPartitioner)
    {
        partitioner = newPartitioner;
    }

    public IEndpointSnitch getEndpointSnitch()
    {
        return snitch;
    }
    public void setEndpointSnitch(IEndpointSnitch eps)
    {
        snitch = eps;
    }

    public IRequestScheduler getRequestScheduler()
    {
        return requestScheduler;
    }

    public RequestSchedulerOptions getRequestSchedulerOptions()
    {
        return requestSchedulerOptions;
    }

    public RequestSchedulerId getRequestSchedulerId()
    {
        return requestSchedulerId;
    }

    public int getColumnIndexSize()
    {
        return conf.column_index_size_in_kb * 1024;
    }

    public int getBatchSizeWarnThreshold()
    {
        return conf.batch_size_warn_threshold_in_kb * 1024;
    }

    public Collection<String> getInitialTokens()
    {
        return tokensFromString(System.getProperty("cassandra.initial_token", conf.initial_token));
    }

    public Collection<String> tokensFromString(String tokenString)
    {
        List<String> tokens = new ArrayList<String>();
        if (tokenString != null)
            for (String token : tokenString.split(","))
                tokens.add(token.replaceAll("^\\s+", "").replaceAll("\\s+$", ""));
        return tokens;
    }

    public Integer getNumTokens()
    {
        return conf.num_tokens;
    }

    public InetAddress getReplaceAddress()
    {
        try
        {
            if (System.getProperty("cassandra.replace_address", null) != null)
                return InetAddress.getByName(System.getProperty("cassandra.replace_address", null));
            else if (System.getProperty("cassandra.replace_address_first_boot", null) != null)
                return InetAddress.getByName(System.getProperty("cassandra.replace_address_first_boot", null));
            return null;
        }
        catch (UnknownHostException e)
        {
            return null;
        }
    }

    public Collection<String> getReplaceTokens()
    {
        return tokensFromString(System.getProperty("cassandra.replace_token", null));
    }

    public UUID getReplaceNode()
    {
        try
        {
            return UUID.fromString(System.getProperty("cassandra.replace_node", null));
        } catch (NullPointerException e)
        {
            return null;
        }
    }

    public boolean isReplacing()
    {
        if (System.getProperty("cassandra.replace_address_first_boot", null) != null && SystemKeyspace.instance.bootstrapComplete())
        {
            logger.info("Replace address on first boot requested; this node is already bootstrapped");
            return false;
        }
        return getReplaceAddress() != null;
    }

    public String getClusterName()
    {
        return conf.cluster_name;
    }

    public int getMaxStreamingRetries()
    {
        return conf.max_streaming_retries;
    }

    public int getStoragePort()
    {
        return Integer.parseInt(System.getProperty("cassandra.storage_port", conf.storage_port.toString()));
    }

    public int getSSLStoragePort()
    {
        return Integer.parseInt(System.getProperty("cassandra.ssl_storage_port", conf.ssl_storage_port.toString()));
    }

    public int getRpcPort()
    {
        return Integer.parseInt(System.getProperty("cassandra.rpc_port", conf.rpc_port.toString()));
    }

    public int getRpcListenBacklog()
    {
        return conf.rpc_listen_backlog;
    }

    public long getRpcTimeout()
    {
        return conf.request_timeout_in_ms;
    }

    public void setRpcTimeout(Long timeOutInMillis)
    {
        conf.request_timeout_in_ms = timeOutInMillis;
    }

    public long getReadRpcTimeout()
    {
        return conf.read_request_timeout_in_ms;
    }

    public void setReadRpcTimeout(Long timeOutInMillis)
    {
        conf.read_request_timeout_in_ms = timeOutInMillis;
    }

    public long getRangeRpcTimeout()
    {
        return conf.range_request_timeout_in_ms;
    }

    public void setRangeRpcTimeout(Long timeOutInMillis)
    {
        conf.range_request_timeout_in_ms = timeOutInMillis;
    }

    public long getWriteRpcTimeout()
    {
        return conf.write_request_timeout_in_ms;
    }

    public void setWriteRpcTimeout(Long timeOutInMillis)
    {
        conf.write_request_timeout_in_ms = timeOutInMillis;
    }

    public long getCounterWriteRpcTimeout()
    {
        return conf.counter_write_request_timeout_in_ms;
    }

    public void setCounterWriteRpcTimeout(Long timeOutInMillis)
    {
        conf.counter_write_request_timeout_in_ms = timeOutInMillis;
    }

    public long getCasContentionTimeout()
    {
        return conf.cas_contention_timeout_in_ms;
    }

    public void setCasContentionTimeout(Long timeOutInMillis)
    {
        conf.cas_contention_timeout_in_ms = timeOutInMillis;
    }

    public long getTruncateRpcTimeout()
    {
        return conf.truncate_request_timeout_in_ms;
    }

    public void setTruncateRpcTimeout(Long timeOutInMillis)
    {
        conf.truncate_request_timeout_in_ms = timeOutInMillis;
    }

    public boolean hasCrossNodeTimeout()
    {
        return conf.cross_node_timeout;
    }

    // not part of the Verb enum so we can change timeouts easily via JMX
    public long getTimeout(MessagingService.Verb verb)
    {
        switch (verb)
        {
            case READ:
                return getReadRpcTimeout();
            case RANGE_SLICE:
                return getRangeRpcTimeout();
            case TRUNCATE:
                return getTruncateRpcTimeout();
            case READ_REPAIR:
            case MUTATION:
            case PAXOS_COMMIT:
            case PAXOS_PREPARE:
            case PAXOS_PROPOSE:
                return getWriteRpcTimeout();
            case COUNTER_MUTATION:
                return getCounterWriteRpcTimeout();
            default:
                return getRpcTimeout();
        }
    }

    /**
     * @return the minimum configured {read, write, range, truncate, misc} timeout
     */
    public long getMinRpcTimeout()
    {
        return Longs.min(getRpcTimeout(),
                         getReadRpcTimeout(),
                         getRangeRpcTimeout(),
                         getWriteRpcTimeout(),
                         getCounterWriteRpcTimeout(),
                         getTruncateRpcTimeout());
    }

    public double getPhiConvictThreshold()
    {
        return conf.phi_convict_threshold;
    }

    public void setPhiConvictThreshold(double phiConvictThreshold)
    {
        conf.phi_convict_threshold = phiConvictThreshold;
    }

    public int getConcurrentReaders()
    {
        return conf.concurrent_reads;
    }

    public int getConcurrentWriters()
    {
        return conf.concurrent_writes;
    }

    public int getConcurrentCounterWriters()
    {
        return conf.concurrent_counter_writes;
    }

    public int getFlushWriters()
    {
            return conf.memtable_flush_writers;
    }

    public int getConcurrentCompactors()
    {
        return conf.concurrent_compactors;
    }

    public int getCompactionThroughputMbPerSec()
    {
        return conf.compaction_throughput_mb_per_sec;
    }

    public void setCompactionThroughputMbPerSec(int value)
    {
        conf.compaction_throughput_mb_per_sec = value;
    }

    public boolean getDisableSTCSInL0()
    {
        return Boolean.getBoolean("cassandra.disable_stcs_in_l0");
    }

    public int getStreamThroughputOutboundMegabitsPerSec()
    {
        return conf.stream_throughput_outbound_megabits_per_sec;
    }

    public void setStreamThroughputOutboundMegabitsPerSec(int value)
    {
        conf.stream_throughput_outbound_megabits_per_sec = value;
    }

    public int getInterDCStreamThroughputOutboundMegabitsPerSec()
    {
        return conf.inter_dc_stream_throughput_outbound_megabits_per_sec;
    }

    public void setInterDCStreamThroughputOutboundMegabitsPerSec(int value)
    {
        conf.inter_dc_stream_throughput_outbound_megabits_per_sec = value;
    }

    public String[] getAllDataFileLocations()
    {
        return conf.data_file_directories;
    }

    public String getCommitLogLocation()
    {
        return conf.commitlog_directory;
    }

    public int getTombstoneWarnThreshold()
    {
        return conf.tombstone_warn_threshold;
    }

    public void setTombstoneWarnThreshold(int threshold)
    {
        conf.tombstone_warn_threshold = threshold;
    }

    public int getTombstoneFailureThreshold()
    {
        return conf.tombstone_failure_threshold;
    }

    public void setTombstoneFailureThreshold(int threshold)
    {
        conf.tombstone_failure_threshold = threshold;
    }

    /**
     * size of commitlog segments to allocate
     */
    public int getCommitLogSegmentSize()
    {
        return conf.commitlog_segment_size_in_mb * 1024 * 1024;
    }

    public String getSavedCachesLocation()
    {
        return conf.saved_caches_directory;
    }

    public Set<InetAddress> getSeeds()
    {
        return ImmutableSet.<InetAddress>builder().addAll(seedProvider.getSeeds()).build();
    }

    public InetAddress getListenAddress()
    {
        return listenAddress;
    }

    public InetAddress getLocalAddress()
    {
        try
        {
            return getListenAddress() == null ? InetAddress.getLocalHost() : getListenAddress();
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    public InetAddress getBroadcastAddress()
    {
        return broadcastAddress == null ? getLocalAddress() : broadcastAddress;
    }

    public IInternodeAuthenticator getInternodeAuthenticator()
    {
        return internodeAuthenticator;
    }

    public void setBroadcastAddress(InetAddress broadcastAdd)
    {
        broadcastAddress = broadcastAdd;
    }

    public boolean startRpc()
    {
        return conf.start_rpc;
    }

    public InetAddress getRpcAddress()
    {
        return rpcAddress;
    }

    public void setBroadcastRpcAddress(InetAddress broadcastRPCAddr)
    {
        broadcastRpcAddress = broadcastRPCAddr;
    }

    public InetAddress getBroadcastRpcAddress()
    {
        return broadcastRpcAddress;
    }

    public String getRpcServerType()
    {
        return conf.rpc_server_type;
    }

    public boolean getRpcKeepAlive()
    {
        return conf.rpc_keepalive;
    }

    public Integer getRpcMinThreads()
    {
        return conf.rpc_min_threads;
    }

    public Integer getRpcMaxThreads()
    {
        return conf.rpc_max_threads;
    }

    public Integer getRpcSendBufferSize()
    {
        return conf.rpc_send_buff_size_in_bytes;
    }

    public Integer getRpcRecvBufferSize()
    {
        return conf.rpc_recv_buff_size_in_bytes;
    }

    public Integer getInternodeSendBufferSize()
    {
        return conf.internode_send_buff_size_in_bytes;
    }

    public Integer getInternodeRecvBufferSize()
    {
        return conf.internode_recv_buff_size_in_bytes;
    }

    public boolean startNativeTransport()
    {
        return conf.start_native_transport;
    }

    public int getNativeTransportPort()
    {
        return Integer.parseInt(System.getProperty("cassandra.native_transport_port", conf.native_transport_port.toString()));
    }

    public Integer getNativeTransportMaxThreads()
    {
        return conf.native_transport_max_threads;
    }

    public int getNativeTransportMaxFrameSize()
    {
        return conf.native_transport_max_frame_size_in_mb * 1024 * 1024;
    }

    public double getCommitLogSyncBatchWindow()
    {
        return conf.commitlog_sync_batch_window_in_ms;
    }

    public int getCommitLogSyncPeriod()
    {
        return conf.commitlog_sync_period_in_ms;
    }

    public int getCommitLogPeriodicQueueSize()
    {
        return conf.commitlog_periodic_queue_size;
    }

    public Config.CommitLogSync getCommitLogSync()
    {
        return conf.commitlog_sync;
    }

    public Config.DiskAccessMode getDiskAccessMode()
    {
        return conf.disk_access_mode;
    }

    public Config.DiskAccessMode getIndexAccessMode()
    {
        return indexAccessMode;
    }

    public void setDiskFailurePolicy(Config.DiskFailurePolicy policy)
    {
        conf.disk_failure_policy = policy;
    }

    public Config.DiskFailurePolicy getDiskFailurePolicy()
    {
        return conf.disk_failure_policy;
    }

    public void setCommitFailurePolicy(Config.CommitFailurePolicy policy)
    {
        conf.commit_failure_policy = policy;
    }

    public Config.CommitFailurePolicy getCommitFailurePolicy()
    {
        return conf.commit_failure_policy;
    }

    public boolean isSnapshotBeforeCompaction()
    {
        return conf.snapshot_before_compaction;
    }

    public boolean isAutoSnapshot() {
        return conf.auto_snapshot;
    }

    @VisibleForTesting
    public void setAutoSnapshot(boolean autoSnapshot) {
        conf.auto_snapshot = autoSnapshot;
    }

    public boolean isAutoBootstrap()
    {
        return Boolean.parseBoolean(System.getProperty("cassandra.auto_bootstrap", conf.auto_bootstrap.toString()));
    }

    public void setHintedHandoffEnabled(boolean hintedHandoffEnabled)
    {
        conf.hinted_handoff_enabled_global = hintedHandoffEnabled;
        conf.hinted_handoff_enabled_by_dc.clear();
    }

    public void setHintedHandoffEnabled(final String dcNames)
    {
        List<String> dcNameList;
        try
        {
            dcNameList = Config.parseHintedHandoffEnabledDCs(dcNames);
        }
        catch (IOException e)
        {
            throw new IllegalArgumentException("Could not read csv of dcs for hinted handoff enable. " + dcNames, e);
        }

        if (dcNameList.isEmpty())
            throw new IllegalArgumentException("Empty list of Dcs for hinted handoff enable");

        conf.hinted_handoff_enabled_by_dc.clear();
        conf.hinted_handoff_enabled_by_dc.addAll(dcNameList);
    }

    public boolean hintedHandoffEnabled()
    {
        return conf.hinted_handoff_enabled_global;
    }

    public Set<String> hintedHandoffEnabledByDC()
    {
        return Collections.unmodifiableSet(conf.hinted_handoff_enabled_by_dc);
    }

    public boolean shouldHintByDC()
    {
        return !conf.hinted_handoff_enabled_by_dc.isEmpty();
    }

    public boolean hintedHandoffEnabled(final String dcName)
    {
        return conf.hinted_handoff_enabled_by_dc.contains(dcName);
    }

    public void setMaxHintWindow(int ms)
    {
        conf.max_hint_window_in_ms = ms;
    }

    public int getMaxHintWindow()
    {
        return conf.max_hint_window_in_ms;
    }

    @Deprecated
    public Integer getIndexInterval()
    {
        return conf.index_interval;
    }

    public File getSerializedCachePath(String ksName, String cfName, UUID cfId, CacheService.CacheType cacheType, String version)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(ksName).append('-');
        builder.append(cfName).append('-');
        if (cfId != null)
            builder.append(ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(cfId))).append('-');
        builder.append(cacheType);
        builder.append((version == null ? "" : "-" + version + ".db"));
        return new File(conf.saved_caches_directory, builder.toString());
    }

    public int getDynamicUpdateInterval()
    {
        return conf.dynamic_snitch_update_interval_in_ms;
    }
    public void setDynamicUpdateInterval(Integer dynamicUpdateInterval)
    {
        conf.dynamic_snitch_update_interval_in_ms = dynamicUpdateInterval;
    }

    public int getDynamicResetInterval()
    {
        return conf.dynamic_snitch_reset_interval_in_ms;
    }
    public void setDynamicResetInterval(Integer dynamicResetInterval)
    {
        conf.dynamic_snitch_reset_interval_in_ms = dynamicResetInterval;
    }

    public double getDynamicBadnessThreshold()
    {
        return conf.dynamic_snitch_badness_threshold;
    }

    public void setDynamicBadnessThreshold(Double dynamicBadnessThreshold)
    {
        conf.dynamic_snitch_badness_threshold = dynamicBadnessThreshold;
    }

    public ServerEncryptionOptions getServerEncryptionOptions()
    {
        return conf.server_encryption_options;
    }

    public ClientEncryptionOptions getClientEncryptionOptions()
    {
        return conf.client_encryption_options;
    }

    public int getHintedHandoffThrottleInKB()
    {
        return conf.hinted_handoff_throttle_in_kb;
    }

    public int getBatchlogReplayThrottleInKB()
    {
        return conf.batchlog_replay_throttle_in_kb;
    }

    public void setHintedHandoffThrottleInKB(Integer throttleInKB)
    {
        conf.hinted_handoff_throttle_in_kb = throttleInKB;
    }

    public int getMaxHintsThread()
    {
        return conf.max_hints_delivery_threads;
    }

    public boolean isIncrementalBackupsEnabled()
    {
        return conf.incremental_backups;
    }

    public void setIncrementalBackupsEnabled(boolean value)
    {
        conf.incremental_backups = value;
    }

    public int getFileCacheSizeInMB()
    {
        return conf.file_cache_size_in_mb;
    }

    public long getTotalCommitlogSpaceInMB()
    {
        return conf.commitlog_total_space_in_mb;
    }

    public int getSSTablePreempiveOpenIntervalInMB()
    {
        return conf.sstable_preemptive_open_interval_in_mb;
    }

    public boolean getTrickleFsync()
    {
        return conf.trickle_fsync;
    }

    public int getTrickleFsyncIntervalInKb()
    {
        return conf.trickle_fsync_interval_in_kb;
    }

    public long getKeyCacheSizeInMB()
    {
        return keyCacheSizeInMB;
    }

    public long getIndexSummaryCapacityInMB()
    {
        return indexSummaryCapacityInMB;
    }

    public int getKeyCacheSavePeriod()
    {
        return conf.key_cache_save_period;
    }

    public void setKeyCacheSavePeriod(int keyCacheSavePeriod)
    {
        conf.key_cache_save_period = keyCacheSavePeriod;
    }

    public int getKeyCacheKeysToSave()
    {
        return conf.key_cache_keys_to_save;
    }

    public void setKeyCacheKeysToSave(int keyCacheKeysToSave)
    {
        conf.key_cache_keys_to_save = keyCacheKeysToSave;
    }

    public long getRowCacheSizeInMB()
    {
        return conf.row_cache_size_in_mb;
    }

    public int getRowCacheSavePeriod()
    {
        return conf.row_cache_save_period;
    }

    public void setRowCacheSavePeriod(int rowCacheSavePeriod)
    {
        conf.row_cache_save_period = rowCacheSavePeriod;
    }

    public int getRowCacheKeysToSave()
    {
        return conf.row_cache_keys_to_save;
    }

    public long getCounterCacheSizeInMB()
    {
        return counterCacheSizeInMB;
    }

    public int getCounterCacheSavePeriod()
    {
        return conf.counter_cache_save_period;
    }

    public void setCounterCacheSavePeriod(int counterCacheSavePeriod)
    {
        conf.counter_cache_save_period = counterCacheSavePeriod;
    }

    public int getCounterCacheKeysToSave()
    {
        return conf.counter_cache_keys_to_save;
    }

    public void setCounterCacheKeysToSave(int counterCacheKeysToSave)
    {
        conf.counter_cache_keys_to_save = counterCacheKeysToSave;
    }

    public IAllocator getoffHeapMemoryAllocator()
    {
        return memoryAllocator;
    }

    public void setRowCacheKeysToSave(int rowCacheKeysToSave)
    {
        conf.row_cache_keys_to_save = rowCacheKeysToSave;
    }

    public int getStreamingSocketTimeout()
    {
        return conf.streaming_socket_timeout_in_ms;
    }

    public String getLocalDataCenter()
    {
        return localDC;
    }

    public Comparator<InetAddress> getLocalComparator()
    {
        return localComparator;
    }

    public Config.InternodeCompression internodeCompression()
    {
        return conf.internode_compression;
    }

    public boolean getInterDCTcpNoDelay()
    {
        return conf.inter_dc_tcp_nodelay;
    }

    public MemtablePool getMemtableAllocatorPool()
    {
        long heapLimit = ((long) conf.memtable_heap_space_in_mb) << 20;
        long offHeapLimit = ((long) conf.memtable_offheap_space_in_mb) << 20;
        switch (conf.memtable_allocation_type)
        {
            case unslabbed_heap_buffers:
                return new HeapPool(heapLimit, conf.memtable_cleanup_threshold, new ColumnFamilyStore.FlushLargestColumnFamily());
            case heap_buffers:
                return new SlabPool(heapLimit, 0, conf.memtable_cleanup_threshold, new ColumnFamilyStore.FlushLargestColumnFamily());
            case offheap_buffers:
                if (!FileUtils.isCleanerAvailable())
                {
                    logger.error("Could not free direct byte buffer: offheap_buffers is not a safe memtable_allocation_type without this ability, please adjust your config. This feature is only guaranteed to work on an Oracle JVM. Refusing to start.");
                    System.exit(-1);
                }
                return new SlabPool(heapLimit, offHeapLimit, conf.memtable_cleanup_threshold, new ColumnFamilyStore.FlushLargestColumnFamily());
            case offheap_objects:
                return new NativePool(heapLimit, offHeapLimit, conf.memtable_cleanup_threshold, new ColumnFamilyStore.FlushLargestColumnFamily());
            default:
                throw new AssertionError();
        }
    }

    public int getIndexSummaryResizeIntervalInMinutes()
    {
        return conf.index_summary_resize_interval_in_minutes;
    }

    public boolean hasLargeAddressSpace()
    {
        // currently we just check if it's a 64bit arch, but any we only really care if the address space is large
        String datamodel = System.getProperty("sun.arch.data.model");
        if (datamodel != null)
        {
            switch (datamodel)
            {
                case "64": return true;
                case "32": return false;
            }
        }
        String arch = System.getProperty("os.arch");
        return arch.contains("64") || arch.contains("sparcv9");
    }
}

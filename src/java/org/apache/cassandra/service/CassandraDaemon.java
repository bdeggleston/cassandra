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
package org.apache.cassandra.service;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.thrift.ThriftSessionManager;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.addthis.metrics.reporter.config.ReporterConfig;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.cql3.functions.Functions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.thrift.ThriftServer;
import org.apache.cassandra.utils.CLibrary;
import org.apache.cassandra.utils.Pair;

/**
 * The <code>CassandraDaemon</code> is an abstraction for a Cassandra daemon
 * service, which defines not only a way to activate and deactivate it, but also
 * hooks into its lifecycle methods (see {@link #setup()}, {@link #start()},
 * {@link #stop()} and {@link #setup()}).
 */
public class CassandraDaemon
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=NativeAccess";

    // Have a dedicated thread to call exit to avoid deadlock in the case where the thread that wants to invoke exit
    // belongs to an executor that our shutdown hook wants to wait to exit gracefully. See CASSANDRA-5273.
    private static final Thread exitThread = new Thread(new Runnable()
    {
        public void run()
        {
            System.exit(100);
        }
    }, "Exit invoker");

    private static final Logger logger = LoggerFactory.getLogger(CassandraDaemon.class);

    private final DatabaseDescriptor databaseDescriptor;
    public CassandraDaemon(DatabaseDescriptor databaseDescriptor)
    {
        this.databaseDescriptor = databaseDescriptor;
    }

    public Server thriftServer;
    public Server nativeServer;

    /**
     * This is a hook for concrete daemons to initialize themselves suitably.
     *
     * Subclasses should override this to finish the job (listening on ports, etc.)
     *
     * @throws IOException
     */
    protected void setup()
    {
        try 
        {
            logger.info("Hostname: {}", InetAddress.getLocalHost().getHostName());
        }
        catch (UnknownHostException e1)
        {
            logger.info("Could not resolve local host");
        }
        // log warnings for different kinds of sub-optimal JVMs.  tldr use 64-bit Oracle >= 1.6u32
        if (!databaseDescriptor.hasLargeAddressSpace())
            logger.info("32bit JVM detected.  It is recommended to run Cassandra on a 64bit JVM for better performance.");
        String javaVersion = System.getProperty("java.version");
        String javaVmName = System.getProperty("java.vm.name");
        logger.info("JVM vendor/version: {}/{}", javaVmName, javaVersion);
        if (javaVmName.contains("OpenJDK"))
        {
            // There is essentially no QA done on OpenJDK builds, and
            // clusters running OpenJDK have seen many heap and load issues.
            logger.warn("OpenJDK is not recommended. Please upgrade to the newest Oracle Java release");
        }
        else if (!javaVmName.contains("HotSpot"))
        {
            logger.warn("Non-Oracle JVM detected.  Some features, such as immediate unmap of compacted SSTables, may not work as intended");
        }
     /*   else
        {
            String[] java_version = javaVersion.split("_");
            String java_major = java_version[0];
            int java_minor;
            try
            {
                java_minor = (java_version.length > 1) ? Integer.parseInt(java_version[1]) : 0;
            }
            catch (NumberFormatException e)
            {
                // have only seen this with java7 so far but no doubt there are other ways to break this
                logger.info("Unable to parse java version {}", Arrays.toString(java_version));
                java_minor = 32;
            }
        }
     */
        logger.info("Heap size: {}/{}", Runtime.getRuntime().totalMemory(), Runtime.getRuntime().maxMemory());
        for(MemoryPoolMXBean pool: ManagementFactory.getMemoryPoolMXBeans())
            logger.info("{} {}: {}", pool.getName(), pool.getType(), pool.getPeakUsage());
        logger.info("Classpath: {}", System.getProperty("java.class.path"));

        // Fail-fast if JNA is not available or failing to initialize properly
        // except with -Dcassandra.boot_without_jna=true. See CASSANDRA-6575.
        if (!CLibrary.jnaAvailable())
        {
            boolean jnaRequired = !Boolean.getBoolean("cassandra.boot_without_jna");

            if (jnaRequired)
            {
                logger.error("JNA failing to initialize properly. Use -Dcassandra.boot_without_jna=true to bootstrap even so.");
                System.exit(3);
            }
        }

        CLibrary.tryMlockall();

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
        {
            public void uncaughtException(Thread t, Throwable e)
            {
                StorageMetrics.exceptions.inc();
                logger.error("Exception in thread {}", t, e);
                databaseDescriptor.getTracing().trace("Exception in thread {}", t, e);
                for (Throwable e2 = e; e2 != null; e2 = e2.getCause())
                {
                    // some code, like FileChannel.map, will wrap an OutOfMemoryError in another exception
                    if (e2 instanceof OutOfMemoryError)
                        exitThread.start();

                    if (e2 instanceof FSError)
                    {
                        if (e2 != e) // make sure FSError gets logged exactly once.
                            logger.error("Exception in thread {}", t, e2);
                        FileUtils.handleFSError((FSError) e2,
                                                databaseDescriptor.getDiskFailurePolicy(),
                                                databaseDescriptor.getStorageService(),
                                                databaseDescriptor.getKeyspaceManager());
                    }

                    if (e2 instanceof CorruptSSTableException)
                    {
                        if (e2 != e)
                            logger.error("Exception in thread " + t, e2);
                        FileUtils.handleCorruptSSTable((CorruptSSTableException) e2,
                                                       databaseDescriptor.getDiskFailurePolicy(),
                                                       databaseDescriptor.getStorageService());
                    }
                }
            }
        });

        // check all directories(data, commitlog, saved cache) for existence and permission
        Iterable<String> dirs = Iterables.concat(Arrays.asList(databaseDescriptor.getAllDataFileLocations()),
                                                 Arrays.asList(databaseDescriptor.getCommitLogLocation(),
                                                               databaseDescriptor.getSavedCachesLocation()));
        for (String dataDir : dirs)
        {
            logger.debug("Checking directory {}", dataDir);
            File dir = new File(dataDir);

            // check that directories exist.
            if (!dir.exists())
            {
                logger.error("Directory {} doesn't exist", dataDir);
                // if they don't, failing their creation, stop cassandra.
                if (!dir.mkdirs())
                {
                    logger.error("Has no permission to create {} directory", dataDir);
                    System.exit(3);
                }
            }
            // if directories exist verify their permissions
            if (!Directories.verifyFullPermissions(dir, dataDir))
            {
                // if permissions aren't sufficient, stop cassandra.
                System.exit(3);
            }
        }

        if (databaseDescriptor.getCacheService() == null) // should never happen
            throw new RuntimeException("Failed to initialize Cache Service.");

        // check the system keyspace to keep user from shooting self in foot by changing partitioner, cluster name, etc.
        // we do a one-off scrub of the system keyspace first; we can't load the list of the rest of the keyspaces,
        // until system keyspace is opened.
        SystemKeyspace systemKeyspace = databaseDescriptor.getSystemKeyspace();  // FIXME: forcing initialization before system keyspace access
        for (CFMetaData cfm : databaseDescriptor.getSchema().getKeyspaceMetaData(Keyspace.SYSTEM_KS).values())
            ColumnFamilyStore.scrubDataDirectories(cfm,
                                                   databaseDescriptor,
                                                   databaseDescriptor.getTracing(),
                                                   databaseDescriptor.getCFMetaDataFactory(),
                                                   databaseDescriptor.getKeyspaceManager(),
                                                   databaseDescriptor.getDBConfig(),
                                                   databaseDescriptor.getColumnFamilyStoreManager().dataDirectories);
        try
        {
            databaseDescriptor.getSystemKeyspace().checkHealth();
        }
        catch (ConfigurationException e)
        {
            logger.error("Fatal exception during initialization", e);
            System.exit(100);
        }

        // load keyspace && function descriptions.
        databaseDescriptor.loadSchemas();
        Functions.loadUDFFromSchema(databaseDescriptor.getQueryProcessor(), databaseDescriptor.getMutationFactory(), databaseDescriptor.getCFMetaDataFactory());

        // clean up compaction leftovers
        Map<Pair<String, String>, Map<Integer, UUID>> unfinishedCompactions = databaseDescriptor.getSystemKeyspace().getUnfinishedCompactions();
        for (Pair<String, String> kscf : unfinishedCompactions.keySet())
        {
            CFMetaData cfm = databaseDescriptor.getSchema().getCFMetaData(kscf.left, kscf.right);
            // CFMetaData can be null if CF is already dropped
            if (cfm != null)
                databaseDescriptor.getColumnFamilyStoreManager().removeUnfinishedCompactionLeftovers(cfm, unfinishedCompactions.get(kscf));
        }
        databaseDescriptor.getSystemKeyspace().discardCompactionsInProgress();

        // clean up debris in the rest of the keyspaces
        for (String keyspaceName : databaseDescriptor.getSchema().getKeyspaces())
        {
            // Skip system as we've already cleaned it
            if (keyspaceName.equals(Keyspace.SYSTEM_KS))
                continue;

            for (CFMetaData cfm : databaseDescriptor.getSchema().getKeyspaceMetaData(keyspaceName).values())
                ColumnFamilyStore.scrubDataDirectories(cfm,
                                                       databaseDescriptor,
                                                       databaseDescriptor.getTracing(),
                                                       databaseDescriptor.getCFMetaDataFactory(),
                                                       databaseDescriptor.getKeyspaceManager(),
                                                       databaseDescriptor.getDBConfig(),
                                                       databaseDescriptor.getColumnFamilyStoreManager().dataDirectories);
        }

        databaseDescriptor.getKeyspaceManager().setInitialized();
        // initialize keyspaces
        for (String keyspaceName : databaseDescriptor.getSchema().getKeyspaces())
        {
            if (logger.isDebugEnabled())
                logger.debug("opening keyspace {}", keyspaceName);
            // disable auto compaction until commit log replay ends
            for (ColumnFamilyStore cfs : databaseDescriptor.getKeyspaceManager().open(keyspaceName).getColumnFamilyStores())
            {
                for (ColumnFamilyStore store : cfs.concatWithIndexes())
                {
                    store.disableAutoCompaction();
                }
            }
        }

        if (databaseDescriptor.getCacheService().keyCache.size() > 0)
            logger.info("completed pre-loading ({} keys) key cache.", databaseDescriptor.getCacheService().keyCache.size());

        if (databaseDescriptor.getCacheService().rowCache.size() > 0)
            logger.info("completed pre-loading ({} keys) row cache.", databaseDescriptor.getCacheService().rowCache.size());

        try
        {
            GCInspector.register(databaseDescriptor.getStatusLogger());
        }
        catch (Throwable t)
        {
            logger.warn("Unable to start GCInspector (currently only supported on the Sun JVM)");
        }

        // replay the log if necessary
        try
        {
            databaseDescriptor.getCommitLog().recover();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        // enable auto compaction
        for (Keyspace keyspace : databaseDescriptor.getKeyspaceManager().all())
        {
            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
            {
                for (final ColumnFamilyStore store : cfs.concatWithIndexes())
                {
                    if (store.getCompactionStrategy().shouldBeEnabled())
                        store.enableAutoCompaction();
                }
            }
        }
        // start compactions in five minutes (if no flushes have occurred by then to do so)
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                for (Keyspace keyspaceName : databaseDescriptor.getKeyspaceManager().all())
                {
                    for (ColumnFamilyStore cf : keyspaceName.getColumnFamilyStores())
                    {
                        for (ColumnFamilyStore store : cf.concatWithIndexes())
                            databaseDescriptor.getCompactionManager().submitBackground(store);
                    }
                }
            }
        };
        databaseDescriptor.getStorageServiceExecutors().optionalTasks.schedule(runnable, 5 * 60, TimeUnit.SECONDS);

        databaseDescriptor.getSystemKeyspace().finishStartup();

        // start server internals
        databaseDescriptor.getStorageService().registerDaemon(this);
        try
        {
            databaseDescriptor.getStorageService().initServer();
        }
        catch (ConfigurationException e)
        {
            logger.error("Fatal configuration error", e);
            System.err.println(e.getMessage() + "\nFatal configuration error; unable to start server.  See log for stacktrace.");
            System.exit(1);
        }

        Mx4jTool.maybeLoad(databaseDescriptor.getBroadcastAddress());

        // Metrics
        String metricsReporterConfigFile = System.getProperty("cassandra.metricsReporterConfigFile");
        if (metricsReporterConfigFile != null)
        {
            logger.info("Trying to load metrics-reporter-config from file: {}", metricsReporterConfigFile);
            try
            {
                String reportFileLocation = CassandraDaemon.class.getClassLoader().getResource(metricsReporterConfigFile).getFile();
                ReporterConfig.loadFromFile(reportFileLocation).enableAll();
            }
            catch (Exception e)
            {
                logger.warn("Failed to load metrics-reporter-config, metric sinks will not be activated", e);
            }
        }

        if (!databaseDescriptor.getBroadcastAddress().equals(InetAddress.getLoopbackAddress()))
            waitForGossipToSettle();

        // Thift
        InetAddress rpcAddr = databaseDescriptor.getRpcAddress();
        int rpcPort = databaseDescriptor.getRpcPort();
        int listenBacklog = databaseDescriptor.getRpcListenBacklog();
        thriftServer = new ThriftServer(rpcAddr, rpcPort, listenBacklog,
                                        databaseDescriptor, databaseDescriptor.getTracing(),
                                        databaseDescriptor.getSchema(), databaseDescriptor.getAuth(), databaseDescriptor.getStorageProxy(),
                                        databaseDescriptor.getMessagingService(), databaseDescriptor.getKeyspaceManager(),
                                        databaseDescriptor.getMutationFactory(), databaseDescriptor.getCounterMutationFactory(),
                                        databaseDescriptor.getStorageService(), databaseDescriptor.getCFMetaDataFactory(),
                                        databaseDescriptor.getMigrationManager(), databaseDescriptor.getKSMetaDataFactory(),
                                        databaseDescriptor.getQueryHandler(), databaseDescriptor.getLocatorConfig(),
                                        databaseDescriptor.getDBConfig(),
                                        new ThriftSessionManager(databaseDescriptor),
                                        ClientMetrics.instance);

        // Native transport
        InetAddress nativeAddr = databaseDescriptor.getRpcAddress();
        int nativePort = databaseDescriptor.getNativeTransportPort();
        Map<Message.Type, Message.Codec> codecs = Message.Type.getCodecMap(databaseDescriptor,
                                                                           databaseDescriptor.getTracing(),
                                                                           databaseDescriptor.getSchema(),
                                                                           databaseDescriptor.getAuth().getAuthenticator(),
                                                                           databaseDescriptor.getQueryHandler(),
                                                                           databaseDescriptor.getQueryProcessor(),
                                                                           databaseDescriptor.getKeyspaceManager(),
                                                                           databaseDescriptor.getStorageProxy(),
                                                                           databaseDescriptor.getMutationFactory(),
                                                                           databaseDescriptor.getCounterMutationFactory(),
                                                                           databaseDescriptor.getMessagingService(),
                                                                           databaseDescriptor.getDBConfig(),
                                                                           databaseDescriptor.getLocatorConfig());
        nativeServer = new org.apache.cassandra.transport.Server(nativeAddr, nativePort, codecs, databaseDescriptor, databaseDescriptor.getTracing(),
                                                                 databaseDescriptor.getAuth(), ClientMetrics.instance, databaseDescriptor.getStorageService(),
                                                                 databaseDescriptor.getMigrationManager());
    }

    /**
     * Initialize the Cassandra Daemon based on the given <a
     * href="http://commons.apache.org/daemon/jsvc.html">Commons
     * Daemon</a>-specific arguments. To clarify, this is a hook for JSVC.
     *
     * @param arguments
     *            the arguments passed in from JSVC
     * @throws IOException
     */
    public void init(String[] arguments) throws IOException
    {
        setup();
    }

    /**
     * Start the Cassandra Daemon, assuming that it has already been
     * initialized via {@link #init(String[])}
     *
     * Hook for JSVC
     */
    public void start()
    {
        String nativeFlag = System.getProperty("cassandra.start_native_transport");
        if ((nativeFlag != null && Boolean.parseBoolean(nativeFlag)) || (nativeFlag == null && databaseDescriptor.startNativeTransport()))
            nativeServer.start();
        else
            logger.info("Not starting native transport as requested. Use JMX (StorageService->startNativeTransport()) or nodetool (enablebinary) to start it");

        String rpcFlag = System.getProperty("cassandra.start_rpc");
        if ((rpcFlag != null && Boolean.parseBoolean(rpcFlag)) || (rpcFlag == null && databaseDescriptor.startRpc()))
            thriftServer.start();
        else
            logger.info("Not starting RPC server as requested. Use JMX (StorageService->startRPCServer()) or nodetool (enablethrift) to start it");
    }

    /**
     * Stop the daemon, ideally in an idempotent manner.
     *
     * Hook for JSVC
     */
    public void stop()
    {
        // this doesn't entirely shut down Cassandra, just the RPC server.
        // jsvc takes care of taking the rest down
        logger.info("Cassandra shutting down...");
        thriftServer.stop();
        nativeServer.stop();
    }


    /**
     * Clean up all resources obtained during the lifetime of the daemon. This
     * is a hook for JSVC.
     */
    public void destroy()
    {}

    /**
     * A convenience method to initialize and start the daemon in one shot.
     */
    public void activate()
    {
        String pidFile = System.getProperty("cassandra-pidfile");

        try
        {
            try
            {
                MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                mbs.registerMBean(new StandardMBean(new NativeAccess(), NativeAccessMBean.class), new ObjectName(MBEAN_NAME));
            }
            catch (Exception e)
            {
                logger.error("error registering MBean {}", MBEAN_NAME, e);
                //Allow the server to start even if the bean can't be registered
            }
            
            setup();

            if (pidFile != null)
            {
                new File(pidFile).deleteOnExit();
            }

            if (System.getProperty("cassandra-foreground") == null)
            {
                System.out.close();
                System.err.close();
            }

            start();
        }
        catch (Throwable e)
        {
            logger.error("Exception encountered during startup", e);

            // try to warn user on stdout too, if we haven't already detached
            e.printStackTrace();
            System.out.println("Exception encountered during startup: " + e.getMessage());

            System.exit(3);
        }
    }

    /**
     * A convenience method to stop and destroy the daemon in one shot.
     */
    public void deactivate()
    {
        stop();
        destroy();
    }

    private void waitForGossipToSettle()
    {
        int forceAfter = Integer.getInteger("cassandra.skip_wait_for_gossip_to_settle", -1);
        if (forceAfter == 0)
        {
            return;
        }
        final int GOSSIP_SETTLE_MIN_WAIT_MS = 5000;
        final int GOSSIP_SETTLE_POLL_INTERVAL_MS = 1000;
        final int GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED = 3;

        logger.info("Waiting for gossip to settle before accepting client requests...");
        Uninterruptibles.sleepUninterruptibly(GOSSIP_SETTLE_MIN_WAIT_MS, TimeUnit.MILLISECONDS);
        int totalPolls = 0;
        int numOkay = 0;
        JMXEnabledThreadPoolExecutor gossipStage = (JMXEnabledThreadPoolExecutor)databaseDescriptor.getStageManager().getStage(Stage.GOSSIP);
        while (numOkay < GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED)
        {
            Uninterruptibles.sleepUninterruptibly(GOSSIP_SETTLE_POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
            long completed = gossipStage.getCompletedTasks();
            long active = gossipStage.getActiveCount();
            long pending = gossipStage.getPendingTasks();
            totalPolls++;
            if (active == 0 && pending == 0)
            {
                logger.debug("Gossip looks settled. CompletedTasks: {}", completed);
                numOkay++;
            }
            else
            {
                logger.info("Gossip not settled after {} polls. Gossip Stage active/pending/completed: {}/{}/{}", totalPolls, active, pending, completed);
                numOkay = 0;
            }
            if (forceAfter > 0 && totalPolls > forceAfter)
            {
                logger.warn("Gossip not settled but startup forced by cassandra.skip_wait_for_gossip_to_settle. Gossip Stage total/active/pending/completed: {}/{}/{}/{}",
                            totalPolls, active, pending, completed);
                break;
            }
        }
        if (totalPolls > GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED)
            logger.info("Gossip settled after {} extra polls; proceeding", totalPolls - GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED);
        else
            logger.info("No gossip backlog; proceeding");
    }

    public void stop(String[] args)
    {
        deactivate();
    }

    public static void main(String[] args)
    {
        CassandraDaemon daemon = new CassandraDaemon(DatabaseDescriptor.createMain(false, true));
        daemon.activate();
    }
    
    static class NativeAccess implements NativeAccessMBean
    {
        public boolean isAvailable()
        {
            return CLibrary.jnaAvailable();
        }
        
        public boolean isMemoryLockable() 
        {
            return CLibrary.jnaMemoryLockable();
        }
    }

    public interface Server
    {
        /**
         * Start the server.
         * This method shoud be able to restart a server stopped through stop().
         * Should throw a RuntimeException if the server cannot be started
         */
        public void start();

        /**
         * Stop the server.
         * This method should be able to stop server started through start().
         * Should throw a RuntimeException if the server cannot be stopped
         */
        public void stop();

        /**
         * Returns whether the server is currently running.
         */
        public boolean isRunning();
    }
}

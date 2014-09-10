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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.ObjectName;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.jmx.JMXConfiguratorMBean;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;

import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.config.*;
import org.apache.cassandra.io.sstable.SSTableReaderFactory;
import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.Auth;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.io.sstable.SSTableDeletingTask;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.*;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.AsyncOneResponse;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ResponseVerbHandler;
import org.apache.cassandra.repair.RepairFuture;
import org.apache.cassandra.repair.RepairMessageVerbHandler;
import org.apache.cassandra.service.paxos.CommitVerbHandler;
import org.apache.cassandra.service.paxos.PrepareVerbHandler;
import org.apache.cassandra.service.paxos.ProposeVerbHandler;
import org.apache.cassandra.streaming.*;
import org.apache.cassandra.thrift.EndpointDetails;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.cassandraConstants;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.*;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

/**
 * This abstraction contains the token/identifier of this node
 * on the identifier space. This token gets gossiped around.
 * This class will also maintain histograms of the load information
 * of other nodes in the cluster.
 */
public class StorageService extends NotificationBroadcasterSupport implements IEndpointStateChangeSubscriber, StorageServiceMBean
{
    private static final Logger logger = LoggerFactory.getLogger(StorageService.class);

    /* JMX notification serial number counter */
    private final AtomicLong notificationSerialNumber = new AtomicLong();

    public volatile VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(LocatorConfig.instance.getPartitioner());

    public static final StorageService instance = new StorageService();

    private final Set<InetAddress> replicatingNodes = Collections.synchronizedSet(new HashSet<InetAddress>());
    private CassandraDaemon daemon;

    private InetAddress removingNode;

    /* Are we starting this node in bootstrap mode? */
    private boolean isBootstrapMode;

    /* we bootstrap but do NOT join the ring unless told to do so */
    private boolean isSurveyMode= Boolean.parseBoolean(System.getProperty("cassandra.write_survey", "false"));

    /* when intialized as a client, we shouldn't write to the system keyspace. */
    private boolean isClientMode;
    private boolean initialized;
    private volatile boolean joined = false;

    private static enum Mode { STARTING, NORMAL, CLIENT, JOINING, LEAVING, DECOMMISSIONED, MOVING, DRAINING, DRAINED }
    private Mode operationMode = Mode.STARTING;

    /* Used for tracking drain progress */
    private volatile int totalCFs, remainingCFs;

    private final AtomicInteger nextRepairCommand = new AtomicInteger();

    private final List<IEndpointLifecycleSubscriber> lifecycleSubscribers = new CopyOnWriteArrayList<>();

    private final BackgroundActivityMonitor bgMonitor;

    private final ObjectName jmxObjectName;

    private Collection<Token> bootstrapTokens = null;

    public void finishBootstrapping()
    {
        isBootstrapMode = false;
    }

    /** This method updates the local token on disk  */
    public void setTokens(Collection<Token> tokens)
    {
        if (logger.isDebugEnabled())
            logger.debug("Setting tokens to {}", tokens);
        SystemKeyspace.instance.updateTokens(tokens);
        LocatorConfig.instance.getTokenMetadata().updateNormalTokens(tokens, DatabaseDescriptor.instance.getBroadcastAddress());
        // order is important here, the gossiper can fire in between adding these two states.  It's ok to send TOKENS without STATUS, but *not* vice versa.
        Collection<Token> localTokens = getLocalTokens();
        Gossiper.instance.addLocalApplicationState(ApplicationState.TOKENS, valueFactory.tokens(localTokens));
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.normal(localTokens));
        setMode(Mode.NORMAL, false);
    }

    public StorageService()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            jmxObjectName = new ObjectName("org.apache.cassandra.db:type=StorageService");
            mbs.registerMBean(this, jmxObjectName);
            mbs.registerMBean(StreamManager.instance, new ObjectName(StreamManager.OBJECT_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        bgMonitor = new BackgroundActivityMonitor(Gossiper.instance, this);

        /* register the verb handlers */
        MessagingService.instance.registerVerbHandlers(MessagingService.Verb.MUTATION, new MutationVerbHandler(Tracing.instance, MessagingService.instance, MutationFactory.instance.serializer));
        MessagingService.instance.registerVerbHandlers(MessagingService.Verb.READ_REPAIR, new ReadRepairVerbHandler(MessagingService.instance));
        MessagingService.instance.registerVerbHandlers(MessagingService.Verb.READ, new ReadVerbHandler(KeyspaceManager.instance, MessagingService.instance, this, Tracing.instance, MessagingService.instance.readResponseSerializer));
        MessagingService.instance.registerVerbHandlers(MessagingService.Verb.RANGE_SLICE, new RangeSliceVerbHandler(Tracing.instance, MessagingService.instance, this, MessagingService.instance.rangeSliceReplySerializer));
        MessagingService.instance.registerVerbHandlers(MessagingService.Verb.PAGED_RANGE, new RangeSliceVerbHandler(Tracing.instance, MessagingService.instance, this, MessagingService.instance.rangeSliceReplySerializer));
        MessagingService.instance.registerVerbHandlers(MessagingService.Verb.COUNTER_MUTATION, new CounterMutationVerbHandler(LocatorConfig.instance, MessagingService.instance, StorageProxy.instance));
        MessagingService.instance.registerVerbHandlers(MessagingService.Verb.TRUNCATE, new TruncateVerbHandler(Tracing.instance, KeyspaceManager.instance, MessagingService.instance));
        MessagingService.instance.registerVerbHandlers(MessagingService.Verb.PAXOS_PREPARE, new PrepareVerbHandler(Tracing.instance, SystemKeyspace.instance, MessagingService.instance, MessagingService.instance.prepareResponseSerializer));
        MessagingService.instance.registerVerbHandlers(MessagingService.Verb.PAXOS_PROPOSE, new ProposeVerbHandler(Tracing.instance, SystemKeyspace.instance, MessagingService.instance));
        MessagingService.instance.registerVerbHandlers(MessagingService.Verb.PAXOS_COMMIT, new CommitVerbHandler(Tracing.instance, SystemKeyspace.instance, KeyspaceManager.instance, MessagingService.instance));

        // see BootStrapper for a summary of how the bootstrap verbs interact
        MessagingService.instance.registerVerbHandlers(MessagingService.Verb.REPLICATION_FINISHED, new ReplicationFinishedVerbHandler(MessagingService.instance, this));
        MessagingService.instance.registerVerbHandlers(MessagingService.Verb.REQUEST_RESPONSE, new ResponseVerbHandler(Tracing.instance, MessagingService.instance));
        MessagingService.instance.registerVerbHandlers(MessagingService.Verb.INTERNAL_RESPONSE, new ResponseVerbHandler(Tracing.instance, MessagingService.instance));
        MessagingService.instance.registerVerbHandlers(MessagingService.Verb.REPAIR_MESSAGE, new RepairMessageVerbHandler(DatabaseDescriptor.instance,
                                                                                                                          StageManager.instance,
                                                                                                                          KeyspaceManager.instance,
                                                                                                                          Schema.instance,
                                                                                                                          ActiveRepairService.instance,
                                                                                                                          CompactionManager.instance,
                                                                                                                          MessagingService.instance,
                                                                                                                          StreamManager.instance,
                                                                                                                          DBConfig.instance));
        MessagingService.instance.registerVerbHandlers(MessagingService.Verb.GOSSIP_SHUTDOWN, new GossipShutdownVerbHandler(Gossiper.instance, FailureDetector.instance));

        MessagingService.instance.registerVerbHandlers(MessagingService.Verb.GOSSIP_DIGEST_SYN, new GossipDigestSynVerbHandler(DatabaseDescriptor.instance, Gossiper.instance, MessagingService.instance, LocatorConfig.instance));
        MessagingService.instance.registerVerbHandlers(MessagingService.Verb.GOSSIP_DIGEST_ACK, new GossipDigestAckVerbHandler(Gossiper.instance, MessagingService.instance));
        MessagingService.instance.registerVerbHandlers(MessagingService.Verb.GOSSIP_DIGEST_ACK2, new GossipDigestAck2VerbHandler(Gossiper.instance));

        MessagingService.instance.registerVerbHandlers(MessagingService.Verb.DEFINITIONS_UPDATE, new DefinitionsUpdateVerbHandler(StageManager.instance, DefsTables.instance));
        MessagingService.instance.registerVerbHandlers(MessagingService.Verb.SCHEMA_CHECK, new SchemaCheckVerbHandler(Schema.instance, MessagingService.instance));
        MessagingService.instance.registerVerbHandlers(MessagingService.Verb.MIGRATION_REQUEST, new MigrationRequestVerbHandler(SystemKeyspace.instance, MessagingService.instance));

        MessagingService.instance.registerVerbHandlers(MessagingService.Verb.SNAPSHOT, new SnapshotVerbHandler(KeyspaceManager.instance, MessagingService.instance));
        MessagingService.instance.registerVerbHandlers(MessagingService.Verb.ECHO, new EchoVerbHandler(MessagingService.instance));
    }

    public void registerDaemon(CassandraDaemon daemon)
    {
        this.daemon = daemon;
    }

    public void register(IEndpointLifecycleSubscriber subscriber)
    {
        lifecycleSubscribers.add(subscriber);
    }

    public void unregister(IEndpointLifecycleSubscriber subscriber)
    {
        lifecycleSubscribers.remove(subscriber);
    }

    // should only be called via JMX
    public void stopGossiping()
    {
        if (initialized)
        {
            logger.warn("Stopping gossip by operator request");
            Gossiper.instance.stop();
            initialized = false;
        }
    }

    // should only be called via JMX
    public void startGossiping()
    {
        if (!initialized)
        {
            logger.warn("Starting gossip by operator request");
            Gossiper.instance.start((int) (System.currentTimeMillis() / 1000));
            initialized = true;
        }
    }

    // should only be called via JMX
    public void startRPCServer()
    {
        if (daemon == null)
        {
            throw new IllegalStateException("No configured daemon");
        }
        daemon.thriftServer.start();
    }

    public void stopRPCServer()
    {
        if (daemon == null)
        {
            throw new IllegalStateException("No configured daemon");
        }
        if (daemon.thriftServer != null)
            daemon.thriftServer.stop();
    }

    public boolean isRPCServerRunning()
    {
        if ((daemon == null) || (daemon.thriftServer == null))
        {
            return false;
        }
        return daemon.thriftServer.isRunning();
    }

    public void startNativeTransport()
    {
        if (daemon == null)
        {
            throw new IllegalStateException("No configured daemon");
        }
        
        try
        {
            daemon.nativeServer.start();
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error starting native transport: " + e.getMessage());
        }
    }

    public void stopNativeTransport()
    {
        if (daemon == null)
        {
            throw new IllegalStateException("No configured daemon");
        }
        if (daemon.nativeServer != null)
            daemon.nativeServer.stop();
    }

    public boolean isNativeTransportRunning()
    {
        if ((daemon == null) || (daemon.nativeServer == null))
        {
            return false;
        }
        return daemon.nativeServer.isRunning();
    }

    public void stopTransports()
    {
        if (isInitialized())
        {
            logger.error("Stopping gossiper");
            stopGossiping();
        }
        if (isRPCServerRunning())
        {
            logger.error("Stopping RPC server");
            stopRPCServer();
        }
        if (isNativeTransportRunning())
        {
            logger.error("Stopping native transport");
            stopNativeTransport();
        }
    }

    private void shutdownClientServers()
    {
        stopRPCServer();
        stopNativeTransport();
    }

    public void stopClient()
    {
        Gossiper.instance.unregister(this);
        Gossiper.instance.stop();
        MessagingService.instance.shutdown();
        // give it a second so that task accepted before the MessagingService shutdown gets submitted to the stage (to avoid RejectedExecutionException)
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        StageManager.instance.shutdownNow();
    }

    public boolean isInitialized()
    {
        return initialized;
    }

    public void stopDaemon()
    {
        if (daemon == null)
            throw new IllegalStateException("No configured daemon");
        daemon.deactivate();
        // completely shut down cassandra
        System.exit(0);
    }

    public synchronized Collection<Token> prepareReplacementInfo() throws ConfigurationException
    {
        logger.info("Gathering node replacement information for {}", DatabaseDescriptor.instance.getReplaceAddress());
        if (!MessagingService.instance.isListening())
            MessagingService.instance.listen(DatabaseDescriptor.instance.getLocalAddress());

        // make magic happen
        Gossiper.instance.doShadowRound();

        UUID hostId = null;
        // now that we've gossiped at least once, we should be able to find the node we're replacing
        if (Gossiper.instance.getEndpointStateForEndpoint(DatabaseDescriptor.instance.getReplaceAddress())== null)
            throw new RuntimeException("Cannot replace_address " + DatabaseDescriptor.instance.getReplaceAddress() + " because it doesn't exist in gossip");
        hostId = Gossiper.instance.getHostId(DatabaseDescriptor.instance.getReplaceAddress());
        try
        {
            if (Gossiper.instance.getEndpointStateForEndpoint(DatabaseDescriptor.instance.getReplaceAddress()).getApplicationState(ApplicationState.TOKENS) == null)
                throw new RuntimeException("Could not find tokens for " + DatabaseDescriptor.instance.getReplaceAddress() + " to replace");
            Collection<Token> tokens = TokenSerializer.deserialize(LocatorConfig.instance.getPartitioner(), new DataInputStream(new ByteArrayInputStream(getApplicationStateValue(DatabaseDescriptor.instance.getReplaceAddress(), ApplicationState.TOKENS))));
            
            SystemKeyspace.instance.setLocalHostId(hostId); // use the replacee's host Id as our own so we receive hints, etc
            Gossiper.instance.resetEndpointStateMap(); // clean up since we have what we need
            return tokens;        
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public synchronized void checkForEndpointCollision() throws ConfigurationException
    {
        logger.debug("Starting shadow gossip round to check for endpoint collision");
        if (!MessagingService.instance.isListening())
            MessagingService.instance.listen(DatabaseDescriptor.instance.getLocalAddress());
        Gossiper.instance.doShadowRound();
        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(DatabaseDescriptor.instance.getBroadcastAddress());
        if (epState != null && !Gossiper.instance.isDeadState(epState))
        {
            throw new RuntimeException(String.format("A node with address %s already exists, cancelling join. " +
                                                     "Use cassandra.replace_address if you want to replace this node.",
                                                     DatabaseDescriptor.instance.getBroadcastAddress()));
        }
        Gossiper.instance.resetEndpointStateMap();
    }

    public synchronized void initClient() throws ConfigurationException
    {
        // We don't wait, because we're going to actually try to work on
        initClient(0);

        // sleep a while to allow gossip to warm up (the other nodes need to know about this one before they can reply).
        outer:
        while (true)
        {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            for (InetAddress address : Gossiper.instance.getLiveMembers())
            {
                if (!Gossiper.instance.isFatClient(address))
                    break outer;
            }
        }

        // sleep until any schema migrations have finished
        while (!MigrationManager.instance.isReadyForBootstrap())
        {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
    }

    public synchronized void initClient(int ringDelay) throws ConfigurationException
    {
        if (initialized)
        {
            if (!isClientMode)
                throw new UnsupportedOperationException("StorageService does not support switching modes.");
            return;
        }
        initialized = true;
        isClientMode = true;
        logger.info("Starting up client gossip");
        setMode(Mode.CLIENT, false);
        Gossiper.instance.register(this);
        Gossiper.instance.start((int) (System.currentTimeMillis() / 1000)); // needed for node-ring gathering.
        Gossiper.instance.addLocalApplicationState(ApplicationState.NET_VERSION, valueFactory.networkVersion());

        if (!MessagingService.instance.isListening())
            MessagingService.instance.listen(DatabaseDescriptor.instance.getLocalAddress());
        Uninterruptibles.sleepUninterruptibly(ringDelay, TimeUnit.MILLISECONDS);
    }

    public synchronized void initServer() throws ConfigurationException
    {
        initServer(DatabaseDescriptor.instance.getRingDelay());
    }

    public synchronized void initServer(int delay) throws ConfigurationException
    {
        logger.info("Cassandra version: {}", FBUtilities.getReleaseVersionString());
        logger.info("Thrift API version: {}", cassandraConstants.VERSION);
        logger.info("CQL supported versions: {} (default: {})", StringUtils.join(ClientState.getCQLSupportedVersion(), ","), ClientState.DEFAULT_CQL_VERSION);

        if (initialized)
        {
            if (isClientMode)
                throw new UnsupportedOperationException("StorageService does not support switching modes.");
            return;
        }
        initialized = true;
        isClientMode = false;

        try
        {
            // Ensure StorageProxy is initialized on start-up; see CASSANDRA-3797.
            Class.forName("org.apache.cassandra.service.StorageProxy");
            // also IndexSummaryManager, which is otherwise unreferenced
            Class.forName("org.apache.cassandra.io.sstable.IndexSummaryManager");
        }
        catch (ClassNotFoundException e)
        {
            throw new AssertionError(e);
        }

        if (Boolean.parseBoolean(System.getProperty("cassandra.load_ring_state", "true")))
        {
            logger.info("Loading persisted ring state");
            Multimap<InetAddress, Token> loadedTokens = SystemKeyspace.instance.loadTokens();
            Map<InetAddress, UUID> loadedHostIds = SystemKeyspace.instance.loadHostIds();
            for (InetAddress ep : loadedTokens.keySet())
            {
                if (ep.equals(DatabaseDescriptor.instance.getBroadcastAddress()))
                {
                    // entry has been mistakenly added, delete it
                    SystemKeyspace.instance.removeEndpoint(ep);
                }
                else
                {
                    LocatorConfig.instance.getTokenMetadata().updateNormalTokens(loadedTokens.get(ep), ep);
                    if (loadedHostIds.containsKey(ep))
                        LocatorConfig.instance.getTokenMetadata().updateHostId(loadedHostIds.get(ep), ep);
                    Gossiper.instance.addSavedEndpoint(ep);
                }
            }
        }

        // daemon threads, like our executors', continue to run while shutdown hooks are invoked
        Thread drainOnShutdown = new Thread(new WrappedRunnable()
        {
            @Override
            public void runMayThrow() throws InterruptedException
            {
                ExecutorService counterMutationStage = StageManager.instance.getStage(Stage.COUNTER_MUTATION);
                ExecutorService mutationStage = StageManager.instance.getStage(Stage.MUTATION);
                if (mutationStage.isShutdown() && counterMutationStage.isShutdown())
                    return; // drained already

                shutdownClientServers();
                StorageServiceExecutors.instance.optionalTasks.shutdown();
                Gossiper.instance.stop();

                // In-progress writes originating here could generate hints to be written, so shut down MessagingService
                // before mutation stage, so we can get all the hints saved before shutting down
                MessagingService.instance.shutdown();
                counterMutationStage.shutdown();
                mutationStage.shutdown();
                counterMutationStage.awaitTermination(3600, TimeUnit.SECONDS);
                mutationStage.awaitTermination(3600, TimeUnit.SECONDS);
                StorageProxy.instance.verifyNoHintsInProgress();

                List<Future<?>> flushes = new ArrayList<>();
                for (Keyspace keyspace : KeyspaceManager.instance.all())
                {
                    KSMetaData ksm = Schema.instance.getKSMetaData(keyspace.getName());
                    if (!ksm.durableWrites)
                    {
                        for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
                            flushes.add(cfs.forceFlush());
                    }
                }
                try
                {
                    FBUtilities.waitOnFutures(flushes);
                }
                catch (Throwable e)
                {
                    // don't let this stop us from shutting down the commitlog and other thread pools
                    logger.warn("Caught exception while waiting for memtable flushes during shutdown hook", e);
                }

                CommitLog.instance.shutdownBlocking();

                // wait for miscellaneous tasks like sstable and commitlog segment deletion
                StorageServiceExecutors.instance.tasks.shutdown();
                if (!StorageServiceExecutors.instance.tasks.awaitTermination(1, TimeUnit.MINUTES))
                    logger.warn("Miscellaneous task executor still busy after one minute; proceeding with shutdown");
            }
        }, "StorageServiceShutdownHook");
        Runtime.getRuntime().addShutdownHook(drainOnShutdown);

        prepareToJoin();

        // Has to be called after the host id has potentially changed in prepareToJoin().
        for (ColumnFamilyStore cfs : ColumnFamilyStoreManager.instance.all())
            if (cfs.metadata.isCounter())
                cfs.initCounterCache();

        if (Boolean.parseBoolean(System.getProperty("cassandra.join_ring", "true")))
        {
            joinTokenRing(delay);
        }
        else
        {
            Collection<Token> tokens = SystemKeyspace.instance.getSavedTokens();
            if (!tokens.isEmpty())
            {
                LocatorConfig.instance.getTokenMetadata().updateNormalTokens(tokens, DatabaseDescriptor.instance.getBroadcastAddress());
                // order is important here, the gossiper can fire in between adding these two states.  It's ok to send TOKENS without STATUS, but *not* vice versa.
                Gossiper.instance.addLocalApplicationState(ApplicationState.TOKENS, valueFactory.tokens(tokens));
                Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.hibernate(true));
            }
            logger.info("Not joining ring as requested. Use JMX (StorageService->joinRing()) to initiate ring joining");
        }
    }

    private boolean shouldBootstrap()
    {
        return DatabaseDescriptor.instance.isAutoBootstrap() && !SystemKeyspace.instance.bootstrapComplete() && !DatabaseDescriptor.instance.getSeeds().contains(DatabaseDescriptor.instance.getBroadcastAddress());
    }

    private void prepareToJoin() throws ConfigurationException
    {
        if (!joined)
        {
            Map<ApplicationState, VersionedValue> appStates = new HashMap<>();

            if (DatabaseDescriptor.instance.isReplacing() && !(Boolean.parseBoolean(System.getProperty("cassandra.join_ring", "true"))))
                throw new ConfigurationException("Cannot set both join_ring=false and attempt to replace a node");
            if (LocatorConfig.instance.getReplaceTokens().size() > 0 || DatabaseDescriptor.instance.getReplaceNode() != null)
                throw new RuntimeException("Replace method removed; use cassandra.replace_address instead");
            if (DatabaseDescriptor.instance.isReplacing())
            {
                if (SystemKeyspace.instance.bootstrapComplete())
                    throw new RuntimeException("Cannot replace address with a node that is already bootstrapped");
                if (!DatabaseDescriptor.instance.isAutoBootstrap())
                    throw new RuntimeException("Trying to replace_address with auto_bootstrap disabled will not work, check your configuration");
                bootstrapTokens = prepareReplacementInfo();
                appStates.put(ApplicationState.STATUS, valueFactory.hibernate(true));
                appStates.put(ApplicationState.TOKENS, valueFactory.tokens(bootstrapTokens));
            }
            else if (shouldBootstrap())
            {
                checkForEndpointCollision();
            }

            // have to start the gossip service before we can see any info on other nodes.  this is necessary
            // for bootstrap to get the load info it needs.
            // (we won't be part of the storage ring though until we add a counterId to our state, below.)
            // Seed the host ID-to-endpoint map with our own ID.
            UUID localHostId = SystemKeyspace.instance.getLocalHostId();
            LocatorConfig.instance.getTokenMetadata().updateHostId(localHostId, DatabaseDescriptor.instance.getBroadcastAddress());
            appStates.put(ApplicationState.NET_VERSION, valueFactory.networkVersion());
            appStates.put(ApplicationState.HOST_ID, valueFactory.hostId(localHostId));
            appStates.put(ApplicationState.RPC_ADDRESS, valueFactory.rpcaddress(DatabaseDescriptor.instance.getBroadcastRpcAddress()));
            appStates.put(ApplicationState.RELEASE_VERSION, valueFactory.releaseVersion());
            logger.info("Starting up server gossip");
            Gossiper.instance.register(this);
            Gossiper.instance.start(SystemKeyspace.instance.incrementAndGetGeneration(), appStates); // needed for node-ring gathering.
            // gossip snitch infos (local DC and rack)
            gossipSnitchInfo();
            // gossip Schema.emptyVersion forcing immediate check for schema updates (see MigrationManager#maybeScheduleSchemaPull)
            Schema.instance.updateVersionAndAnnounce(); // Ensure we know our own actual Schema UUID in preparation for updates

            if (!MessagingService.instance.isListening())
                MessagingService.instance.listen(DatabaseDescriptor.instance.getLocalAddress());
            LoadBroadcaster.instance.startBroadcasting();

            HintedHandOffManager.instance.start();
            BatchlogManager.instance.start();
        }
    }

    private void joinTokenRing(int delay) throws ConfigurationException
    {
        joined = true;

        // We bootstrap if we haven't successfully bootstrapped before, as long as we are not a seed.
        // If we are a seed, or if the user manually sets auto_bootstrap to false,
        // we'll skip streaming data from other nodes and jump directly into the ring.
        //
        // The seed check allows us to skip the RING_DELAY sleep for the single-node cluster case,
        // which is useful for both new users and testing.
        //
        // We attempted to replace this with a schema-presence check, but you need a meaningful sleep
        // to get schema info from gossip which defeats the purpose.  See CASSANDRA-4427 for the gory details.
        Set<InetAddress> current = new HashSet<>();
        logger.debug("Bootstrap variables: {} {} {} {}",
                     DatabaseDescriptor.instance.isAutoBootstrap(),
                     SystemKeyspace.instance.bootstrapInProgress(),
                     SystemKeyspace.instance.bootstrapComplete(),
                     DatabaseDescriptor.instance.getSeeds().contains(DatabaseDescriptor.instance.getBroadcastAddress()));
        if (DatabaseDescriptor.instance.isAutoBootstrap() && !SystemKeyspace.instance.bootstrapComplete() && DatabaseDescriptor.instance.getSeeds().contains(DatabaseDescriptor.instance.getBroadcastAddress()))
            logger.info("This node will not auto bootstrap because it is configured to be a seed node.");
        if (shouldBootstrap())
        {
            if (SystemKeyspace.instance.bootstrapInProgress())
                logger.warn("Detected previous bootstrap failure; retrying");
            else
                SystemKeyspace.instance.setBootstrapState(SystemKeyspace.BootstrapState.IN_PROGRESS);
            setMode(Mode.JOINING, "waiting for ring information", true);
            // first sleep the delay to make sure we see all our peers
            for (int i = 0; i < delay; i += 1000)
            {
                // if we see schema, we can proceed to the next check directly
                if (!Schema.instance.getVersion().equals(Schema.emptyVersion))
                {
                    logger.debug("got schema: {}", Schema.instance.getVersion());
                    break;
                }
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            }
            // if our schema hasn't matched yet, keep sleeping until it does
            // (post CASSANDRA-1391 we don't expect this to be necessary very often, but it doesn't hurt to be careful)
            while (!MigrationManager.instance.isReadyForBootstrap())
            {
                setMode(Mode.JOINING, "waiting for schema information to complete", true);
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            }
            setMode(Mode.JOINING, "schema complete, ready to bootstrap", true);
            setMode(Mode.JOINING, "waiting for pending range calculation", true);
            PendingRangeCalculatorService.instance.blockUntilFinished();
            setMode(Mode.JOINING, "calculation complete, ready to bootstrap", true);


            if (logger.isDebugEnabled())
                logger.debug("... got ring + schema info");

            if (!DatabaseDescriptor.instance.isReplacing())
            {
                if (LocatorConfig.instance.getTokenMetadata().isMember(DatabaseDescriptor.instance.getBroadcastAddress()))
                {
                    String s = "This node is already a member of the token ring; bootstrap aborted. (If replacing a dead node, remove the old one from the ring first.)";
                    throw new UnsupportedOperationException(s);
                }
                setMode(Mode.JOINING, "getting bootstrap token", true);
                bootstrapTokens = BootStrapper.getBootstrapTokens(LocatorConfig.instance.getTokenMetadata(), DatabaseDescriptor.instance, LocatorConfig.instance);
            }
            else
            {
                if (!DatabaseDescriptor.instance.getReplaceAddress().equals(DatabaseDescriptor.instance.getBroadcastAddress()))
                {
                    try
                    {
                        // Sleep additionally to make sure that the server actually is not alive
                        // and giving it more time to gossip if alive.
                        Thread.sleep(LoadBroadcaster.BROADCAST_INTERVAL);
                    }
                    catch (InterruptedException e)
                    {
                        throw new AssertionError(e);
                    }

                    // check for operator errors...
                    for (Token token : bootstrapTokens)
                    {
                        InetAddress existing = LocatorConfig.instance.getTokenMetadata().getEndpoint(token);
                        if (existing != null)
                        {
                            long nanoDelay = delay * 1000000L;
                            if (Gossiper.instance.getEndpointStateForEndpoint(existing).getUpdateTimestamp() > (System.nanoTime() - nanoDelay))
                                throw new UnsupportedOperationException("Cannot replace a live node... ");
                            current.add(existing);
                        }
                        else
                        {
                            throw new UnsupportedOperationException("Cannot replace token " + token + " which does not exist!");
                        }
                    }
                }
                else
                {
                    try
                    {
                        Thread.sleep(DatabaseDescriptor.instance.getRingDelay());
                    }
                    catch (InterruptedException e)
                    {
                        throw new AssertionError(e);
                    }

                }
                setMode(Mode.JOINING, "Replacing a node with token(s): " + bootstrapTokens, true);
            }

            bootstrap(bootstrapTokens);
            assert !isBootstrapMode; // bootstrap will block until finished
        }
        else
        {
            bootstrapTokens = SystemKeyspace.instance.getSavedTokens();
            if (bootstrapTokens.isEmpty())
            {
                Collection<String> initialTokens = LocatorConfig.instance.getInitialTokens();
                if (initialTokens.size() < 1)
                {
                    bootstrapTokens = BootStrapper.getRandomTokens(LocatorConfig.instance.getTokenMetadata(), DatabaseDescriptor.instance.getNumTokens());
                    if (DatabaseDescriptor.instance.getNumTokens() == 1)
                        logger.warn("Generated random token {}. Random tokens will result in an unbalanced ring; see http://wiki.apache.org/cassandra/Operations", bootstrapTokens);
                    else
                        logger.info("Generated random tokens. tokens are {}", bootstrapTokens);
                }
                else
                {
                    bootstrapTokens = new ArrayList<Token>(initialTokens.size());
                    for (String token : initialTokens)
                        bootstrapTokens.add(LocatorConfig.instance.getPartitioner().getTokenFactory().fromString(token));
                    logger.info("Saved tokens not found. Using configuration value: {}", bootstrapTokens);
                }
            }
            else
            {
                if (bootstrapTokens.size() != DatabaseDescriptor.instance.getNumTokens())
                    throw new ConfigurationException("Cannot change the number of tokens from " + bootstrapTokens.size() + " to " + DatabaseDescriptor.instance.getNumTokens());
                else
                    logger.info("Using saved tokens {}", bootstrapTokens);
            }
        }

        // if we don't have system_traces keyspace at this point, then create it manually
        if (Schema.instance.getKSMetaData(Tracing.TRACE_KS) == null)
        {
            KSMetaData tracingKeyspace = KSMetaDataFactory.instance.traceKeyspace();
            MigrationManager.instance.announceNewKeyspace(tracingKeyspace, 0, false);
        }

        if (!isSurveyMode)
        {
            // start participating in the ring.
            SystemKeyspace.instance.setBootstrapState(SystemKeyspace.BootstrapState.COMPLETED);
            setTokens(bootstrapTokens);
            // remove the existing info about the replaced node.
            if (!current.isEmpty())
                for (InetAddress existing : current)
                    Gossiper.instance.replacedEndpoint(existing);
            assert LocatorConfig.instance.getTokenMetadata().sortedTokens().size() > 0;

            Auth.instance.setup();
        }
        else
        {
            logger.info("Startup complete, but write survey mode is active, not becoming an active ring member. Use JMX (StorageService->joinRing()) to finalize ring joining.");
        }
    }

    public void gossipSnitchInfo()
    {
        IEndpointSnitch snitch = LocatorConfig.instance.getEndpointSnitch();
        String dc = snitch.getDatacenter(DatabaseDescriptor.instance.getBroadcastAddress());
        String rack = snitch.getRack(DatabaseDescriptor.instance.getBroadcastAddress());
        Gossiper.instance.addLocalApplicationState(ApplicationState.DC, valueFactory.datacenter(dc));
        Gossiper.instance.addLocalApplicationState(ApplicationState.RACK, valueFactory.rack(rack));
    }

    public synchronized void joinRing() throws IOException
    {
        if (!joined)
        {
            logger.info("Joining ring by operator request");
            try
            {
                joinTokenRing(0);
            }
            catch (ConfigurationException e)
            {
                throw new IOException(e.getMessage());
            }
        }
        else if (isSurveyMode)
        {
            setTokens(SystemKeyspace.instance.getSavedTokens());
            SystemKeyspace.instance.setBootstrapState(SystemKeyspace.BootstrapState.COMPLETED);
            isSurveyMode = false;
            logger.info("Leaving write survey mode and joining ring at operator request");
            assert LocatorConfig.instance.getTokenMetadata().sortedTokens().size() > 0;

            Auth.instance.setup();
        }
    }

    public boolean isJoined()
    {
        return joined;
    }

    public void rebuild(String sourceDc)
    {
        logger.info("rebuild from dc: {}", sourceDc == null ? "(any dc)" : sourceDc);

        RangeStreamer streamer = new RangeStreamer(LocatorConfig.instance.getTokenMetadata(), DatabaseDescriptor.instance.getBroadcastAddress(), "Rebuild",
                                                   DatabaseDescriptor.instance, Schema.instance, Gossiper.instance, StreamManager.instance, KeyspaceManager.instance, DBConfig.instance);
        streamer.addSourceFilter(new RangeStreamer.FailureDetectorSourceFilter(FailureDetector.instance));
        if (sourceDc != null)
            streamer.addSourceFilter(new RangeStreamer.SingleDatacenterFilter(LocatorConfig.instance.getEndpointSnitch(), sourceDc));

        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
            streamer.addRanges(keyspaceName, LocatorConfig.instance.getLocalRanges(keyspaceName));

        try
        {
            streamer.fetchAsync().get();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException("Interrupted while waiting on rebuild streaming");
        }
        catch (ExecutionException e)
        {
            // This is used exclusively through JMX, so log the full trace but only throw a simple RTE
            logger.error("Error while rebuilding node", e.getCause());
            throw new RuntimeException("Error while rebuilding node: " + e.getCause().getMessage());
        }
    }

    public void setStreamThroughputMbPerSec(int value)
    {
        DatabaseDescriptor.instance.setStreamThroughputOutboundMegabitsPerSec(value);
        logger.info("setstreamthroughput: throttle set to {}", value);
    }

    public int getStreamThroughputMbPerSec()
    {
        return DatabaseDescriptor.instance.getStreamThroughputOutboundMegabitsPerSec();
    }

    public int getCompactionThroughputMbPerSec()
    {
        return DatabaseDescriptor.instance.getCompactionThroughputMbPerSec();
    }

    public void setCompactionThroughputMbPerSec(int value)
    {
        DatabaseDescriptor.instance.setCompactionThroughputMbPerSec(value);
    }

    public boolean isIncrementalBackupsEnabled()
    {
        return DatabaseDescriptor.instance.isIncrementalBackupsEnabled();
    }

    public void setIncrementalBackupsEnabled(boolean value)
    {
        DatabaseDescriptor.instance.setIncrementalBackupsEnabled(value);
    }

    private void setMode(Mode m, boolean log)
    {
        setMode(m, null, log);
    }

    private void setMode(Mode m, String msg, boolean log)
    {
        operationMode = m;
        String logMsg = msg == null ? m.toString() : String.format("%s: %s", m, msg);
        if (log)
            logger.info(logMsg);
        else
            logger.debug(logMsg);
    }

    private void bootstrap(Collection<Token> tokens)
    {
        isBootstrapMode = true;
        SystemKeyspace.instance.updateTokens(tokens); // DON'T use setToken, that makes us part of the ring locally which is incorrect until we are done bootstrapping
        if (!DatabaseDescriptor.instance.isReplacing())
        {
            // if not an existing token then bootstrap
            // order is important here, the gossiper can fire in between adding these two states.  It's ok to send TOKENS without STATUS, but *not* vice versa.
            Gossiper.instance.addLocalApplicationState(ApplicationState.TOKENS, valueFactory.tokens(tokens));
            Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS,
                                                       valueFactory.bootstrapping(tokens));
            setMode(Mode.JOINING, "sleeping " + DatabaseDescriptor.instance.getRingDelay() + " ms for pending range setup", true);
            Uninterruptibles.sleepUninterruptibly(DatabaseDescriptor.instance.getRingDelay(), TimeUnit.MILLISECONDS);
        }
        else
        {
            // Dont set any state for the node which is bootstrapping the existing token...
            LocatorConfig.instance.getTokenMetadata().updateNormalTokens(tokens, DatabaseDescriptor.instance.getBroadcastAddress());
            SystemKeyspace.instance.removeEndpoint(DatabaseDescriptor.instance.getReplaceAddress());
        }
        if (!Gossiper.instance.seenAnySeed())
            throw new IllegalStateException("Unable to contact any seeds!");
        setMode(Mode.JOINING, "Starting to bootstrap...", true);
        new BootStrapper(DatabaseDescriptor.instance.getBroadcastAddress(), tokens, LocatorConfig.instance.getTokenMetadata(), DatabaseDescriptor.instance,
                         Schema.instance, Gossiper.instance, KeyspaceManager.instance, StreamManager.instance, this, DBConfig.instance).bootstrap(); // handles token update
        logger.info("Bootstrap completed! for the tokens {}", tokens);
    }

    public boolean isBootstrapMode()
    {
        return isBootstrapMode;
    }

    /**
     * Increment about the known Compaction severity of the events in this node
     */
    public void reportSeverity(double incr)
    {
        bgMonitor.incrCompactionSeverity(incr);
    }

    public void reportManualSeverity(double incr)
    {
        bgMonitor.incrManualSeverity(incr);
    }

    public double getSeverity(InetAddress endpoint)
    {
        return bgMonitor.getSeverity(endpoint);
    }

    /**
     * for a keyspace, return the ranges and corresponding listen addresses.
     * @param keyspace
     * @return the endpoint map
     */
    public Map<List<String>, List<String>> getRangeToEndpointMap(String keyspace)
    {
        /* All the ranges for the tokens */
        Map<List<String>, List<String>> map = new HashMap<>();
        for (Map.Entry<Range<Token>,List<InetAddress>> entry : getRangeToAddressMap(keyspace).entrySet())
        {
            map.put(entry.getKey().asList(), stringify(entry.getValue()));
        }
        return map;
    }

    /**
     * Return the rpc address associated with an endpoint as a string.
     * @param endpoint The endpoint to get rpc address for
     * @return the rpc address
     */
    public String getRpcaddress(InetAddress endpoint)
    {
        if (endpoint.equals(DatabaseDescriptor.instance.getBroadcastAddress()))
            return DatabaseDescriptor.instance.getBroadcastRpcAddress().getHostAddress();
        else if (Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.RPC_ADDRESS) == null)
            return endpoint.getHostAddress();
        else
            return Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.RPC_ADDRESS).value;
    }

    /**
     * for a keyspace, return the ranges and corresponding RPC addresses for a given keyspace.
     * @param keyspace
     * @return the endpoint map
     */
    public Map<List<String>, List<String>> getRangeToRpcaddressMap(String keyspace)
    {
        /* All the ranges for the tokens */
        Map<List<String>, List<String>> map = new HashMap<>();
        for (Map.Entry<Range<Token>, List<InetAddress>> entry : getRangeToAddressMap(keyspace).entrySet())
        {
            List<String> rpcaddrs = new ArrayList<>(entry.getValue().size());
            for (InetAddress endpoint: entry.getValue())
            {
                rpcaddrs.add(getRpcaddress(endpoint));
            }
            map.put(entry.getKey().asList(), rpcaddrs);
        }
        return map;
    }

    public Map<List<String>, List<String>> getPendingRangeToEndpointMap(String keyspace)
    {
        // some people just want to get a visual representation of things. Allow null and set it to the first
        // non-system keyspace.
        if (keyspace == null)
            keyspace = Schema.instance.getNonSystemKeyspaces().get(0);

        Map<List<String>, List<String>> map = new HashMap<>();
        for (Map.Entry<Range<Token>, Collection<InetAddress>> entry : LocatorConfig.instance.getTokenMetadata().getPendingRanges(keyspace).entrySet())
        {
            List<InetAddress> l = new ArrayList<>(entry.getValue());
            map.put(entry.getKey().asList(), stringify(l));
        }
        return map;
    }

    public Map<Range<Token>, List<InetAddress>> getRangeToAddressMap(String keyspace)
    {
        return getRangeToAddressMap(keyspace, LocatorConfig.instance.getTokenMetadata().sortedTokens());
    }

    public Map<Range<Token>, List<InetAddress>> getRangeToAddressMapInLocalDC(String keyspace)
    {
        Predicate<InetAddress> isLocalDC = new Predicate<InetAddress>()
        {
            public boolean apply(InetAddress address)
            {
                return isLocalDC(address);
            }
        };

        Map<Range<Token>, List<InetAddress>> origMap = getRangeToAddressMap(keyspace, getTokensInLocalDC());
        Map<Range<Token>, List<InetAddress>> filteredMap = Maps.newHashMap();
        for (Map.Entry<Range<Token>, List<InetAddress>> entry : origMap.entrySet())
        {
            List<InetAddress> endpointsInLocalDC = Lists.newArrayList(Collections2.filter(entry.getValue(), isLocalDC));
            filteredMap.put(entry.getKey(), endpointsInLocalDC);
        }

        return filteredMap;
    }

    private List<Token> getTokensInLocalDC()
    {
        List<Token> filteredTokens = Lists.newArrayList();
        for (Token token : LocatorConfig.instance.getTokenMetadata().sortedTokens())
        {
            InetAddress endpoint = LocatorConfig.instance.getTokenMetadata().getEndpoint(token);
            if (isLocalDC(endpoint))
                filteredTokens.add(token);
        }
        return filteredTokens;
    }

    private boolean isLocalDC(InetAddress targetHost)
    {
        String remoteDC = LocatorConfig.instance.getEndpointSnitch().getDatacenter(targetHost);
        String localDC = LocatorConfig.instance.getEndpointSnitch().getDatacenter(DatabaseDescriptor.instance.getBroadcastAddress());
        return remoteDC.equals(localDC);
    }

    private Map<Range<Token>, List<InetAddress>> getRangeToAddressMap(String keyspace, List<Token> sortedTokens)
    {
        // some people just want to get a visual representation of things. Allow null and set it to the first
        // non-system keyspace.
        if (keyspace == null)
            keyspace = Schema.instance.getNonSystemKeyspaces().get(0);

        List<Range<Token>> ranges = getAllRanges(sortedTokens);
        return constructRangeToEndpointMap(keyspace, ranges);
    }


    /**
     * The same as {@code describeRing(String)} but converts TokenRange to the String for JMX compatibility
     *
     * @param keyspace The keyspace to fetch information about
     *
     * @return a List of TokenRange(s) converted to String for the given keyspace
     */
    public List<String> describeRingJMX(String keyspace) throws IOException
    {
        List<TokenRange> tokenRanges;
        try
        {
            tokenRanges = describeRing(keyspace);
        }
        catch (InvalidRequestException e)
        {
            throw new IOException(e.getMessage());
        }
        List<String> result = new ArrayList<>(tokenRanges.size());

        for (TokenRange tokenRange : tokenRanges)
            result.add(tokenRange.toString());

        return result;
    }

    /**
     * The TokenRange for a given keyspace.
     *
     * @param keyspace The keyspace to fetch information about
     *
     * @return a List of TokenRange(s) for the given keyspace
     *
     * @throws InvalidRequestException if there is no ring information available about keyspace
     */
    public List<TokenRange> describeRing(String keyspace) throws InvalidRequestException
    {
        return describeRing(keyspace, false);
    }

    /**
     * The same as {@code describeRing(String)} but considers only the part of the ring formed by nodes in the local DC.
     */
    public List<TokenRange> describeLocalRing(String keyspace) throws InvalidRequestException
    {
        return describeRing(keyspace, true);
    }

    private List<TokenRange> describeRing(String keyspace, boolean includeOnlyLocalDC) throws InvalidRequestException
    {
        if (!Schema.instance.getKeyspaces().contains(keyspace))
            throw new InvalidRequestException("No such keyspace: " + keyspace);

        if (keyspace == null || KeyspaceManager.instance.open(keyspace).getReplicationStrategy() instanceof LocalStrategy)
            throw new InvalidRequestException("There is no ring for the keyspace: " + keyspace);

        List<TokenRange> ranges = new ArrayList<>();
        Token.TokenFactory tf = LocatorConfig.instance.getPartitioner().getTokenFactory();

        Map<Range<Token>, List<InetAddress>> rangeToAddressMap =
                includeOnlyLocalDC
                        ? getRangeToAddressMapInLocalDC(keyspace)
                        : getRangeToAddressMap(keyspace);

        for (Map.Entry<Range<Token>, List<InetAddress>> entry : rangeToAddressMap.entrySet())
        {
            Range range = entry.getKey();
            List<InetAddress> addresses = entry.getValue();
            List<String> endpoints = new ArrayList<>(addresses.size());
            List<String> rpc_endpoints = new ArrayList<>(addresses.size());
            List<EndpointDetails> epDetails = new ArrayList<>(addresses.size());

            for (InetAddress endpoint : addresses)
            {
                EndpointDetails details = new EndpointDetails();
                details.host = endpoint.getHostAddress();
                details.datacenter = LocatorConfig.instance.getEndpointSnitch().getDatacenter(endpoint);
                details.rack = LocatorConfig.instance.getEndpointSnitch().getRack(endpoint);

                endpoints.add(details.host);
                rpc_endpoints.add(getRpcaddress(endpoint));

                epDetails.add(details);
            }

            TokenRange tr = new TokenRange(tf.toString(range.left.getToken()), tf.toString(range.right.getToken()), endpoints)
                                    .setEndpoint_details(epDetails)
                                    .setRpc_endpoints(rpc_endpoints);

            ranges.add(tr);
        }

        return ranges;
    }

    public Map<String, String> getTokenToEndpointMap()
    {
        Map<Token, InetAddress> mapInetAddress = LocatorConfig.instance.getTokenMetadata().getNormalAndBootstrappingTokenToEndpointMap();
        // in order to preserve tokens in ascending order, we use LinkedHashMap here
        Map<String, String> mapString = new LinkedHashMap<>(mapInetAddress.size());
        List<Token> tokens = new ArrayList<>(mapInetAddress.keySet());
        Collections.sort(tokens);
        for (Token token : tokens)
        {
            mapString.put(token.toString(), mapInetAddress.get(token).getHostAddress());
        }
        return mapString;
    }

    public String getLocalHostId()
    {
        return LocatorConfig.instance.getTokenMetadata().getHostId(DatabaseDescriptor.instance.getBroadcastAddress()).toString();
    }

    public Map<String, String> getHostIdMap()
    {
        Map<String, String> mapOut = new HashMap<>();
        for (Map.Entry<InetAddress, UUID> entry : LocatorConfig.instance.getTokenMetadata().getEndpointToHostIdMapForReading().entrySet())
            mapOut.put(entry.getKey().getHostAddress(), entry.getValue().toString());
        return mapOut;
    }

    /**
     * Construct the range to endpoint mapping based on the true view
     * of the world.
     * @param ranges
     * @return mapping of ranges to the replicas responsible for them.
    */
    private Map<Range<Token>, List<InetAddress>> constructRangeToEndpointMap(String keyspace, List<Range<Token>> ranges)
    {
        Map<Range<Token>, List<InetAddress>> rangeToEndpointMap = new HashMap<>();
        for (Range<Token> range : ranges)
        {
            rangeToEndpointMap.put(range, KeyspaceManager.instance.open(keyspace).getReplicationStrategy().getNaturalEndpoints(range.right));
        }
        return rangeToEndpointMap;
    }

    public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue)
    {
        // no-op
    }

    /*
     * Handle the reception of a new particular ApplicationState for a particular endpoint. Note that the value of the
     * ApplicationState has not necessarily "changed" since the last known value, if we already received the same update
     * from somewhere else.
     *
     * onChange only ever sees one ApplicationState piece change at a time (even if many ApplicationState updates were
     * received at the same time), so we perform a kind of state machine here. We are concerned with two events: knowing
     * the token associated with an endpoint, and knowing its operation mode. Nodes can start in either bootstrap or
     * normal mode, and from bootstrap mode can change mode to normal. A node in bootstrap mode needs to have
     * pendingranges set in TokenMetadata; a node in normal mode should instead be part of the token ring.
     *
     * Normal progression of ApplicationState.STATUS values for a node should be like this:
     * STATUS_BOOTSTRAPPING,token
     *   if bootstrapping. stays this way until all files are received.
     * STATUS_NORMAL,token
     *   ready to serve reads and writes.
     * STATUS_LEAVING,token
     *   get ready to leave the cluster as part of a decommission
     * STATUS_LEFT,token
     *   set after decommission is completed.
     *
     * Other STATUS values that may be seen (possibly anywhere in the normal progression):
     * STATUS_MOVING,newtoken
     *   set if node is currently moving to a new token in the ring
     * REMOVING_TOKEN,deadtoken
     *   set if the node is dead and is being removed by its REMOVAL_COORDINATOR
     * REMOVED_TOKEN,deadtoken
     *   set if the node is dead and has been removed by its REMOVAL_COORDINATOR
     *
     * Note: Any time a node state changes from STATUS_NORMAL, it will not be visible to new nodes. So it follows that
     * you should never bootstrap a new node during a removenode, decommission or move.
     */
    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value)
    {
        if (state.equals(ApplicationState.STATUS))
        {
            String apStateValue = value.value;
            String[] pieces = apStateValue.split(VersionedValue.DELIMITER_STR, -1);
            assert (pieces.length > 0);

            String moveName = pieces[0];

            switch (moveName)
            {
                case VersionedValue.STATUS_BOOTSTRAPPING:
                    handleStateBootstrap(endpoint);
                    break;
                case VersionedValue.STATUS_NORMAL:
                    handleStateNormal(endpoint);
                    break;
                case VersionedValue.REMOVING_TOKEN:
                case VersionedValue.REMOVED_TOKEN:
                    handleStateRemoving(endpoint, pieces);
                    break;
                case VersionedValue.STATUS_LEAVING:
                    handleStateLeaving(endpoint);
                    break;
                case VersionedValue.STATUS_LEFT:
                    handleStateLeft(endpoint, pieces);
                    break;
                case VersionedValue.STATUS_MOVING:
                    handleStateMoving(endpoint, pieces);
                    break;
            }
        }
        else
        {
            EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
            if (epState == null || Gossiper.instance.isDeadState(epState))
            {
                logger.debug("Ignoring state change for dead or unknown endpoint: {}", endpoint);
                return;
            }

            switch (state)
            {
                case RELEASE_VERSION:
                    SystemKeyspace.instance.updatePeerInfo(endpoint, "release_version", value.value);
                    break;
                case DC:
                    SystemKeyspace.instance.updatePeerInfo(endpoint, "data_center", value.value);
                    break;
                case RACK:
                    SystemKeyspace.instance.updatePeerInfo(endpoint, "rack", value.value);
                    break;
                case RPC_ADDRESS:
                    try
                    {
                        SystemKeyspace.instance.updatePeerInfo(endpoint, "rpc_address", InetAddress.getByName(value.value));
                    }
                    catch (UnknownHostException e)
                    {
                        throw new RuntimeException(e);
                    }
                    break;
                case SCHEMA:
                    SystemKeyspace.instance.updatePeerInfo(endpoint, "schema_version", UUID.fromString(value.value));
                    MigrationManager.instance.scheduleSchemaPull(endpoint, epState);
                    break;
                case HOST_ID:
                    SystemKeyspace.instance.updatePeerInfo(endpoint, "host_id", UUID.fromString(value.value));
                    break;
            }
        }
    }

    private void updatePeerInfo(InetAddress endpoint)
    {
        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        for (Map.Entry<ApplicationState, VersionedValue> entry : epState.getApplicationStateMap().entrySet())
        {
            switch (entry.getKey())
            {
                case RELEASE_VERSION:
                    SystemKeyspace.instance.updatePeerInfo(endpoint, "release_version", entry.getValue().value);
                    break;
                case DC:
                    SystemKeyspace.instance.updatePeerInfo(endpoint, "data_center", entry.getValue().value);
                    break;
                case RACK:
                    SystemKeyspace.instance.updatePeerInfo(endpoint, "rack", entry.getValue().value);
                    break;
                case RPC_ADDRESS:
                    try
                    {
                        SystemKeyspace.instance.updatePeerInfo(endpoint, "rpc_address", InetAddress.getByName(entry.getValue().value));
                    }
                    catch (UnknownHostException e)
                    {
                        throw new RuntimeException(e);
                    }
                    break;
                case SCHEMA:
                    SystemKeyspace.instance.updatePeerInfo(endpoint, "schema_version", UUID.fromString(entry.getValue().value));
                    break;
                case HOST_ID:
                    SystemKeyspace.instance.updatePeerInfo(endpoint, "host_id", UUID.fromString(entry.getValue().value));
                    break;
            }
        }
    }

    private byte[] getApplicationStateValue(InetAddress endpoint, ApplicationState appstate)
    {
        String vvalue = Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(appstate).value;
        return vvalue.getBytes(ISO_8859_1);
    }

    private Collection<Token> getTokensFor(InetAddress endpoint)
    {
        try
        {
            return TokenSerializer.deserialize(LocatorConfig.instance.getPartitioner(), new DataInputStream(new ByteArrayInputStream(getApplicationStateValue(endpoint, ApplicationState.TOKENS))));
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Handle node bootstrap
     *
     * @param endpoint bootstrapping node
     */
    private void handleStateBootstrap(InetAddress endpoint)
    {
        Collection<Token> tokens;
        // explicitly check for TOKENS, because a bootstrapping node might be bootstrapping in legacy mode; that is, not using vnodes and no token specified
        tokens = getTokensFor(endpoint);

        if (logger.isDebugEnabled())
            logger.debug("Node {} state bootstrapping, token {}", endpoint, tokens);

        // if this node is present in token metadata, either we have missed intermediate states
        // or the node had crashed. Print warning if needed, clear obsolete stuff and
        // continue.
        if (LocatorConfig.instance.getTokenMetadata().isMember(endpoint))
        {
            // If isLeaving is false, we have missed both LEAVING and LEFT. However, if
            // isLeaving is true, we have only missed LEFT. Waiting time between completing
            // leave operation and rebootstrapping is relatively short, so the latter is quite
            // common (not enough time for gossip to spread). Therefore we report only the
            // former in the log.
            if (!LocatorConfig.instance.getTokenMetadata().isLeaving(endpoint))
                logger.info("Node {} state jump to bootstrap", endpoint);
            LocatorConfig.instance.getTokenMetadata().removeEndpoint(endpoint);
        }

        LocatorConfig.instance.getTokenMetadata().addBootstrapTokens(tokens, endpoint);
        PendingRangeCalculatorService.instance.update();

        if (Gossiper.instance.usesHostId(endpoint))
            LocatorConfig.instance.getTokenMetadata().updateHostId(Gossiper.instance.getHostId(endpoint), endpoint);
    }

    /**
     * Handle node move to normal state. That is, node is entering token ring and participating
     * in reads.
     *
     * @param endpoint node
     */
    private void handleStateNormal(final InetAddress endpoint)
    {
        Collection<Token> tokens;

        tokens = getTokensFor(endpoint);

        Set<Token> tokensToUpdateInMetadata = new HashSet<>();
        Set<Token> tokensToUpdateInSystemKeyspace = new HashSet<>();
        Set<Token> localTokensToRemove = new HashSet<>();
        Set<InetAddress> endpointsToRemove = new HashSet<>();


        if (logger.isDebugEnabled())
            logger.debug("Node {} state normal, token {}", endpoint, tokens);

        if (LocatorConfig.instance.getTokenMetadata().isMember(endpoint))
            logger.info("Node {} state jump to normal", endpoint);

        updatePeerInfo(endpoint);
        // Order Matters, TM.updateHostID() should be called before TM.updateNormalToken(), (see CASSANDRA-4300).
        if (Gossiper.instance.usesHostId(endpoint))
        {
            UUID hostId = Gossiper.instance.getHostId(endpoint);
            InetAddress existing = LocatorConfig.instance.getTokenMetadata().getEndpointForHostId(hostId);
            if (DatabaseDescriptor.instance.isReplacing() && Gossiper.instance.getEndpointStateForEndpoint(DatabaseDescriptor.instance.getReplaceAddress()) != null && (hostId.equals(Gossiper.instance.getHostId(DatabaseDescriptor.instance.getReplaceAddress()))))
                logger.warn("Not updating token metadata for {} because I am replacing it", endpoint);
            else
            {
                if (existing != null && !existing.equals(endpoint))
                {
                    if (existing.equals(DatabaseDescriptor.instance.getBroadcastAddress()))
                    {
                        logger.warn("Not updating host ID {} for {} because it's mine", hostId, endpoint);
                        LocatorConfig.instance.getTokenMetadata().removeEndpoint(endpoint);
                        endpointsToRemove.add(endpoint);
                    }
                    else if (Gossiper.instance.compareEndpointStartup(endpoint, existing) > 0)
                    {
                        logger.warn("Host ID collision for {} between {} and {}; {} is the new owner", hostId, existing, endpoint, endpoint);
                        LocatorConfig.instance.getTokenMetadata().removeEndpoint(existing);
                        endpointsToRemove.add(existing);
                        LocatorConfig.instance.getTokenMetadata().updateHostId(hostId, endpoint);
                    }
                    else
                    {
                        logger.warn("Host ID collision for {} between {} and {}; ignored {}", hostId, existing, endpoint, endpoint);
                        LocatorConfig.instance.getTokenMetadata().removeEndpoint(endpoint);
                        endpointsToRemove.add(endpoint);
                    }
                }
                else
                    LocatorConfig.instance.getTokenMetadata().updateHostId(hostId, endpoint);
            }
        }

        for (final Token token : tokens)
        {
            // we don't want to update if this node is responsible for the token and it has a later startup time than endpoint.
            InetAddress currentOwner = LocatorConfig.instance.getTokenMetadata().getEndpoint(token);
            if (currentOwner == null)
            {
                logger.debug("New node {} at token {}", endpoint, token);
                tokensToUpdateInMetadata.add(token);
                if (!isClientMode)
                    tokensToUpdateInSystemKeyspace.add(token);
            }
            else if (endpoint.equals(currentOwner))
            {
                // set state back to normal, since the node may have tried to leave, but failed and is now back up
                tokensToUpdateInMetadata.add(token);
                if (!isClientMode)
                    tokensToUpdateInSystemKeyspace.add(token);
            }
            else if (Gossiper.instance.compareEndpointStartup(endpoint, currentOwner) > 0)
            {
                tokensToUpdateInMetadata.add(token);
                if (!isClientMode)
                    tokensToUpdateInSystemKeyspace.add(token);

                // currentOwner is no longer current, endpoint is.  Keep track of these moves, because when
                // a host no longer has any tokens, we'll want to remove it.
                Multimap<InetAddress, Token> epToTokenCopy = LocatorConfig.instance.getTokenMetadata().getEndpointToTokenMapForReading();
                epToTokenCopy.get(currentOwner).remove(token);
                if (epToTokenCopy.get(currentOwner).size() < 1)
                    endpointsToRemove.add(currentOwner);

                logger.info(String.format("Nodes %s and %s have the same token %s.  %s is the new owner",
                                          endpoint,
                                          currentOwner,
                                          token,
                                          endpoint));
            }
            else
            {
                logger.info(String.format("Nodes %s and %s have the same token %s.  Ignoring %s",
                                           endpoint,
                                           currentOwner,
                                           token,
                                           endpoint));
            }
        }

        LocatorConfig.instance.getTokenMetadata().updateNormalTokens(tokensToUpdateInMetadata, endpoint);
        for (InetAddress ep : endpointsToRemove)
            removeEndpoint(ep);
        if (!tokensToUpdateInSystemKeyspace.isEmpty())
            SystemKeyspace.instance.updateTokens(endpoint, tokensToUpdateInSystemKeyspace);
        if (!localTokensToRemove.isEmpty())
            SystemKeyspace.instance.updateLocalTokens(Collections.<Token>emptyList(), localTokensToRemove);

        if (LocatorConfig.instance.getTokenMetadata().isMoving(endpoint)) // if endpoint was moving to a new token
        {
            LocatorConfig.instance.getTokenMetadata().removeFromMoving(endpoint);

            if (!isClientMode)
            {
                for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
                    subscriber.onMove(endpoint);
            }
        }

        PendingRangeCalculatorService.instance.update();
    }

    /**
     * Handle node preparing to leave the ring
     *
     * @param endpoint node
     */
    private void handleStateLeaving(InetAddress endpoint)
    {
        Collection<Token> tokens;
        tokens = getTokensFor(endpoint);

        if (logger.isDebugEnabled())
            logger.debug("Node {} state leaving, tokens {}", endpoint, tokens);

        // If the node is previously unknown or tokens do not match, update tokenmetadata to
        // have this node as 'normal' (it must have been using this token before the
        // leave). This way we'll get pending ranges right.
        if (!LocatorConfig.instance.getTokenMetadata().isMember(endpoint))
        {
            logger.info("Node {} state jump to leaving", endpoint);
            LocatorConfig.instance.getTokenMetadata().updateNormalTokens(tokens, endpoint);
        }
        else if (!LocatorConfig.instance.getTokenMetadata().getTokens(endpoint).containsAll(tokens))
        {
            logger.warn("Node {} 'leaving' token mismatch. Long network partition?", endpoint);
            LocatorConfig.instance.getTokenMetadata().updateNormalTokens(tokens, endpoint);
        }

        // at this point the endpoint is certainly a member with this token, so let's proceed
        // normally
        LocatorConfig.instance.getTokenMetadata().addLeavingEndpoint(endpoint);
        PendingRangeCalculatorService.instance.update();
    }

    /**
     * Handle node leaving the ring. This will happen when a node is decommissioned
     *
     * @param endpoint If reason for leaving is decommission, endpoint is the leaving node.
     * @param pieces STATE_LEFT,token
     */
    private void handleStateLeft(InetAddress endpoint, String[] pieces)
    {
        assert pieces.length >= 2;
        Collection<Token> tokens;
        tokens = getTokensFor(endpoint);

        if (logger.isDebugEnabled())
            logger.debug("Node {} state left, tokens {}", endpoint, tokens);

        excise(tokens, endpoint, extractExpireTime(pieces));
    }

    /**
     * Handle node moving inside the ring.
     *
     * @param endpoint moving endpoint address
     * @param pieces STATE_MOVING, token
     */
    private void handleStateMoving(InetAddress endpoint, String[] pieces)
    {
        assert pieces.length >= 2;
        Token token = LocatorConfig.instance.getPartitioner().getTokenFactory().fromString(pieces[1]);

        if (logger.isDebugEnabled())
            logger.debug("Node {} state moving, new token {}", endpoint, token);

        LocatorConfig.instance.getTokenMetadata().addMovingEndpoint(token, endpoint);

        PendingRangeCalculatorService.instance.update();
    }

    /**
     * Handle notification that a node being actively removed from the ring via 'removenode'
     *
     * @param endpoint node
     * @param pieces either REMOVED_TOKEN (node is gone) or REMOVING_TOKEN (replicas need to be restored)
     */
    private void handleStateRemoving(InetAddress endpoint, String[] pieces)
    {
        assert (pieces.length > 0);

        if (endpoint.equals(DatabaseDescriptor.instance.getBroadcastAddress()))
        {
            logger.info("Received removenode gossip about myself. Is this node rejoining after an explicit removenode?");
            try
            {
                drain();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            return;
        }
        if (LocatorConfig.instance.getTokenMetadata().isMember(endpoint))
        {
            String state = pieces[0];
            Collection<Token> removeTokens = LocatorConfig.instance.getTokenMetadata().getTokens(endpoint);

            if (VersionedValue.REMOVED_TOKEN.equals(state))
            {
                excise(removeTokens, endpoint, extractExpireTime(pieces));
            }
            else if (VersionedValue.REMOVING_TOKEN.equals(state))
            {
                if (logger.isDebugEnabled())
                    logger.debug("Tokens {} removed manually (endpoint was {})", removeTokens, endpoint);

                // Note that the endpoint is being removed
                LocatorConfig.instance.getTokenMetadata().addLeavingEndpoint(endpoint);
                PendingRangeCalculatorService.instance.update();

                // find the endpoint coordinating this removal that we need to notify when we're done
                String[] coordinator = Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.REMOVAL_COORDINATOR).value.split(VersionedValue.DELIMITER_STR, -1);
                UUID hostId = UUID.fromString(coordinator[1]);
                // grab any data we are now responsible for and notify responsible node
                restoreReplicaCount(endpoint, LocatorConfig.instance.getTokenMetadata().getEndpointForHostId(hostId));
            }
        }
        else // now that the gossiper has told us about this nonexistent member, notify the gossiper to remove it
        {
            if (VersionedValue.REMOVED_TOKEN.equals(pieces[0]))
                addExpireTimeIfFound(endpoint, extractExpireTime(pieces));
            removeEndpoint(endpoint);
        }
    }

    private void excise(Collection<Token> tokens, InetAddress endpoint)
    {
        logger.info("Removing tokens {} for {}", tokens, endpoint);
        HintedHandOffManager.instance.deleteHintsForEndpoint(endpoint);
        removeEndpoint(endpoint);
        LocatorConfig.instance.getTokenMetadata().removeEndpoint(endpoint);
        LocatorConfig.instance.getTokenMetadata().removeBootstrapTokens(tokens);

        if (!isClientMode)
        {
            for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
                subscriber.onLeaveCluster(endpoint);
        }
        PendingRangeCalculatorService.instance.update();
    }

    private void excise(Collection<Token> tokens, InetAddress endpoint, long expireTime)
    {
        addExpireTimeIfFound(endpoint, expireTime);
        excise(tokens, endpoint);
    }

    /** unlike excise we just need this endpoint gone without going through any notifications **/
    private void removeEndpoint(InetAddress endpoint)
    {
        Gossiper.instance.removeEndpoint(endpoint);
        if (!isClientMode)
            SystemKeyspace.instance.removeEndpoint(endpoint);
    }

    protected void addExpireTimeIfFound(InetAddress endpoint, long expireTime)
    {
        if (expireTime != 0L)
        {
            Gossiper.instance.addExpireTimeForEndpoint(endpoint, expireTime);
        }
    }

    protected long extractExpireTime(String[] pieces)
    {
        return Long.parseLong(pieces[2]);
    }

    /**
     * Finds living endpoints responsible for the given ranges
     *
     * @param keyspaceName the keyspace ranges belong to
     * @param ranges the ranges to find sources for
     * @return multimap of addresses to ranges the address is responsible for
     */
    private Multimap<InetAddress, Range<Token>> getNewSourceRanges(String keyspaceName, Set<Range<Token>> ranges)
    {
        InetAddress myAddress = DatabaseDescriptor.instance.getBroadcastAddress();
        Multimap<Range<Token>, InetAddress> rangeAddresses = KeyspaceManager.instance.open(keyspaceName).getReplicationStrategy().getRangeAddresses(LocatorConfig.instance.getTokenMetadata().cloneOnlyTokenMap());
        Multimap<InetAddress, Range<Token>> sourceRanges = HashMultimap.create();
        IFailureDetector failureDetector = FailureDetector.instance;

        // find alive sources for our new ranges
        for (Range<Token> range : ranges)
        {
            Collection<InetAddress> possibleRanges = rangeAddresses.get(range);
            IEndpointSnitch snitch = LocatorConfig.instance.getEndpointSnitch();
            List<InetAddress> sources = snitch.getSortedListByProximity(myAddress, possibleRanges);

            assert (!sources.contains(myAddress));

            for (InetAddress source : sources)
            {
                if (failureDetector.isAlive(source))
                {
                    sourceRanges.put(source, range);
                    break;
                }
            }
        }
        return sourceRanges;
    }

    /**
     * Sends a notification to a node indicating we have finished replicating data.
     *
     * @param remote node to send notification to
     */
    private void sendReplicationNotification(InetAddress remote)
    {
        // notify the remote token
        MessageOut msg = new MessageOut(MessagingService.Verb.REPLICATION_FINISHED);
        IFailureDetector failureDetector = FailureDetector.instance;
        if (logger.isDebugEnabled())
            logger.debug("Notifying {} of replication completion\n", remote);
        while (failureDetector.isAlive(remote))
        {
            AsyncOneResponse iar = MessagingService.instance.sendRR(msg, remote);
            try
            {
                iar.get(DatabaseDescriptor.instance.getRpcTimeout(), TimeUnit.MILLISECONDS);
                return; // done
            }
            catch(TimeoutException e)
            {
                // try again
            }
        }
    }

    /**
     * Called when an endpoint is removed from the ring. This function checks
     * whether this node becomes responsible for new ranges as a
     * consequence and streams data if needed.
     *
     * This is rather ineffective, but it does not matter so much
     * since this is called very seldom
     *
     * @param endpoint the node that left
     */
    private void restoreReplicaCount(InetAddress endpoint, final InetAddress notifyEndpoint)
    {
        Multimap<String, Map.Entry<InetAddress, Collection<Range<Token>>>> rangesToFetch = HashMultimap.create();

        InetAddress myAddress = DatabaseDescriptor.instance.getBroadcastAddress();

        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            Multimap<Range<Token>, InetAddress> changedRanges = getChangedRangesForLeaving(keyspaceName, endpoint);
            Set<Range<Token>> myNewRanges = new HashSet<>();
            for (Map.Entry<Range<Token>, InetAddress> entry : changedRanges.entries())
            {
                if (entry.getValue().equals(myAddress))
                    myNewRanges.add(entry.getKey());
            }
            Multimap<InetAddress, Range<Token>> sourceRanges = getNewSourceRanges(keyspaceName, myNewRanges);
            for (Map.Entry<InetAddress, Collection<Range<Token>>> entry : sourceRanges.asMap().entrySet())
            {
                rangesToFetch.put(keyspaceName, entry);
            }
        }

        StreamPlan stream = new StreamPlan("Restore replica count", DatabaseDescriptor.instance, Schema.instance,
                                           KeyspaceManager.instance, StreamManager.instance, DBConfig.instance);
        for (String keyspaceName : rangesToFetch.keySet())
        {
            for (Map.Entry<InetAddress, Collection<Range<Token>>> entry : rangesToFetch.get(keyspaceName))
            {
                InetAddress source = entry.getKey();
                Collection<Range<Token>> ranges = entry.getValue();
                if (logger.isDebugEnabled())
                    logger.debug("Requesting from {} ranges {}", source, StringUtils.join(ranges, ", "));
                stream.requestRanges(source, keyspaceName, ranges);
            }
        }
        StreamResultFuture future = stream.execute();
        Futures.addCallback(future, new FutureCallback<StreamState>()
        {
            public void onSuccess(StreamState finalState)
            {
                sendReplicationNotification(notifyEndpoint);
            }

            public void onFailure(Throwable t)
            {
                logger.warn("Streaming to restore replica count failed", t);
                // We still want to send the notification
                sendReplicationNotification(notifyEndpoint);
            }
        });
    }

    // needs to be modified to accept either a keyspace or ARS.
    private Multimap<Range<Token>, InetAddress> getChangedRangesForLeaving(String keyspaceName, InetAddress endpoint)
    {
        // First get all ranges the leaving endpoint is responsible for
        Collection<Range<Token>> ranges = LocatorConfig.instance.getRangesForEndpoint(keyspaceName, endpoint);

        if (logger.isDebugEnabled())
            logger.debug("Node {} ranges [{}]", endpoint, StringUtils.join(ranges, ", "));

        Map<Range<Token>, List<InetAddress>> currentReplicaEndpoints = new HashMap<>();

        // Find (for each range) all nodes that store replicas for these ranges as well
        TokenMetadata metadata = LocatorConfig.instance.getTokenMetadata().cloneOnlyTokenMap(); // don't do this in the loop! #7758
        for (Range<Token> range : ranges)
            currentReplicaEndpoints.put(range, KeyspaceManager.instance.open(keyspaceName).getReplicationStrategy().calculateNaturalEndpoints(range.right, metadata));

        TokenMetadata temp = LocatorConfig.instance.getTokenMetadata().cloneAfterAllLeft();

        // endpoint might or might not be 'leaving'. If it was not leaving (that is, removenode
        // command was used), it is still present in temp and must be removed.
        if (temp.isMember(endpoint))
            temp.removeEndpoint(endpoint);

        Multimap<Range<Token>, InetAddress> changedRanges = HashMultimap.create();

        // Go through the ranges and for each range check who will be
        // storing replicas for these ranges when the leaving endpoint
        // is gone. Whoever is present in newReplicaEndpoints list, but
        // not in the currentReplicaEndpoints list, will be needing the
        // range.
        for (Range<Token> range : ranges)
        {
            Collection<InetAddress> newReplicaEndpoints = KeyspaceManager.instance.open(keyspaceName).getReplicationStrategy().calculateNaturalEndpoints(range.right, temp);
            newReplicaEndpoints.removeAll(currentReplicaEndpoints.get(range));
            if (logger.isDebugEnabled())
                if (newReplicaEndpoints.isEmpty())
                    logger.debug("Range {} already in all replicas", range);
                else
                    logger.debug("Range {} will be responsibility of {}", range, StringUtils.join(newReplicaEndpoints, ", "));
            changedRanges.putAll(range, newReplicaEndpoints);
        }

        return changedRanges;
    }

    public void onJoin(InetAddress endpoint, EndpointState epState)
    {
        for (Map.Entry<ApplicationState, VersionedValue> entry : epState.getApplicationStateMap().entrySet())
        {
            onChange(endpoint, entry.getKey(), entry.getValue());
        }
        MigrationManager.instance.scheduleSchemaPull(endpoint, epState);
    }

    public void onAlive(InetAddress endpoint, EndpointState state)
    {
        if (isClientMode)
            return;

        if (LocatorConfig.instance.getTokenMetadata().isMember(endpoint))
        {
            HintedHandOffManager.instance.scheduleHintDelivery(endpoint);
            for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
                subscriber.onUp(endpoint);
        }
        else
        {
            for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
                subscriber.onJoinCluster(endpoint);
        }
        MigrationManager.instance.scheduleSchemaPull(endpoint, state);
    }

    public void onRemove(InetAddress endpoint)
    {
        LocatorConfig.instance.getTokenMetadata().removeEndpoint(endpoint);
        PendingRangeCalculatorService.instance.update();
    }

    public void onDead(InetAddress endpoint, EndpointState state)
    {
        MessagingService.instance.convict(endpoint);
        if (!isClientMode)
        {
            for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
                subscriber.onDown(endpoint);
        }
    }

    public void onRestart(InetAddress endpoint, EndpointState state)
    {
        // If we have restarted before the node was even marked down, we need to reset the connection pool
        if (state.isAlive())
            onDead(endpoint, state);
    }

    /** raw load value */
    public double getLoad()
    {
        double bytes = 0;
        for (String keyspaceName : Schema.instance.getKeyspaces())
        {
            Keyspace keyspace = Schema.instance.getKeyspaceInstance(keyspaceName);
            if (keyspace == null)
                continue;
            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
                bytes += cfs.getLiveDiskSpaceUsed();
        }
        return bytes;
    }

    public String getLoadString()
    {
        return FileUtils.stringifyFileSize(getLoad());
    }

    public Map<String, String> getLoadMap()
    {
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<InetAddress,Double> entry : LoadBroadcaster.instance.getLoadInfo().entrySet())
        {
            map.put(entry.getKey().getHostAddress(), FileUtils.stringifyFileSize(entry.getValue()));
        }
        // gossiper doesn't see its own updates, so we need to special-case the local node
        map.put(DatabaseDescriptor.instance.getBroadcastAddress().getHostAddress(), getLoadString());
        return map;
    }

    public final void deliverHints(String host) throws UnknownHostException
    {
        HintedHandOffManager.instance.scheduleHintDelivery(host);
    }

    public Collection<Token> getLocalTokens()
    {
        Collection<Token> tokens = SystemKeyspace.instance.getSavedTokens();
        assert tokens != null && !tokens.isEmpty(); // should not be called before initServer sets this
        return tokens;
    }

    /* These methods belong to the MBean interface */

    public List<String> getTokens()
    {
        return getTokens(DatabaseDescriptor.instance.getBroadcastAddress());
    }

    public List<String> getTokens(String endpoint) throws UnknownHostException
    {
        return getTokens(InetAddress.getByName(endpoint));
    }

    private List<String> getTokens(InetAddress endpoint)
    {
        List<String> strTokens = new ArrayList<>();
        for (Token tok : LocatorConfig.instance.getTokenMetadata().getTokens(endpoint))
            strTokens.add(tok.toString());
        return strTokens;
    }

    public String getReleaseVersion()
    {
        return FBUtilities.getReleaseVersionString();
    }

    public String getSchemaVersion()
    {
        return Schema.instance.getVersion().toString();
    }

    public List<String> getLeavingNodes()
    {
        return stringify(LocatorConfig.instance.getTokenMetadata().getLeavingEndpoints());
    }

    public List<String> getMovingNodes()
    {
        List<String> endpoints = new ArrayList<>();

        for (Pair<Token, InetAddress> node : LocatorConfig.instance.getTokenMetadata().getMovingEndpoints())
        {
            endpoints.add(node.right.getHostAddress());
        }

        return endpoints;
    }

    public List<String> getJoiningNodes()
    {
        return stringify(LocatorConfig.instance.getTokenMetadata().getBootstrapTokens().valueSet());
    }

    public List<String> getLiveNodes()
    {
        return stringify(Gossiper.instance.getLiveMembers());
    }

    public List<String> getUnreachableNodes()
    {
        return stringify(Gossiper.instance.getUnreachableMembers());
    }

    public String[] getAllDataFileLocations()
    {
        String[] locations = DatabaseDescriptor.instance.getAllDataFileLocations();
        for (int i = 0; i < locations.length; i++)
            locations[i] = FileUtils.getCanonicalPath(locations[i]);
        return locations;
    }

    public String getCommitLogLocation()
    {
        return FileUtils.getCanonicalPath(DatabaseDescriptor.instance.getCommitLogLocation());
    }

    public String getSavedCachesLocation()
    {
        return FileUtils.getCanonicalPath(DatabaseDescriptor.instance.getSavedCachesLocation());
    }

    private List<String> stringify(Iterable<InetAddress> endpoints)
    {
        List<String> stringEndpoints = new ArrayList<>();
        for (InetAddress ep : endpoints)
        {
            stringEndpoints.add(ep.getHostAddress());
        }
        return stringEndpoints;
    }

    public int getCurrentGenerationNumber()
    {
        return Gossiper.instance.getCurrentGenerationNumber(DatabaseDescriptor.instance.getBroadcastAddress());
    }

    public CompactionManager.AllSSTableOpStatus forceKeyspaceCleanup(String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        if (keyspaceName.equals(Keyspace.SYSTEM_KS))
            throw new RuntimeException("Cleanup of the system keyspace is neither necessary nor wise");

        CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(false, false, keyspaceName, columnFamilies))
        {
            CompactionManager.AllSSTableOpStatus oneStatus = cfStore.forceCleanup();
            if (oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL)
                status = oneStatus;
        }
        return status;
    }

    public CompactionManager.AllSSTableOpStatus scrub(boolean disableSnapshot, boolean skipCorrupted, String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(false, false, keyspaceName, columnFamilies))
        {
            CompactionManager.AllSSTableOpStatus oneStatus = cfStore.scrub(disableSnapshot, skipCorrupted);
            if (oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL)
                status = oneStatus;
        }
        return status;
    }

    public CompactionManager.AllSSTableOpStatus upgradeSSTables(String keyspaceName, boolean excludeCurrentVersion, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(true, true, keyspaceName, columnFamilies))
        {
            CompactionManager.AllSSTableOpStatus oneStatus = cfStore.sstablesRewrite(excludeCurrentVersion);
            if (oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL)
                status = oneStatus;
        }
        return status;
    }

    public void forceKeyspaceCompaction(String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(true, false, keyspaceName, columnFamilies))
        {
            cfStore.forceMajorCompaction();
        }
    }

    /**
     * Takes the snapshot for the given keyspaces. A snapshot name must be specified.
     *
     * @param tag the tag given to the snapshot; may not be null or empty
     * @param keyspaceNames the names of the keyspaces to snapshot; empty means "all."
     */
    public void takeSnapshot(String tag, String... keyspaceNames) throws IOException
    {
        if (operationMode.equals(Mode.JOINING))
            throw new IOException("Cannot snapshot until bootstrap completes");
        if (tag == null || tag.equals(""))
            throw new IOException("You must supply a snapshot name.");

        Iterable<Keyspace> keyspaces;
        if (keyspaceNames.length == 0)
        {
            keyspaces = KeyspaceManager.instance.all();
        }
        else
        {
            ArrayList<Keyspace> t = new ArrayList<>(keyspaceNames.length);
            for (String keyspaceName : keyspaceNames)
                t.add(getValidKeyspace(keyspaceName));
            keyspaces = t;
        }

        // Do a check to see if this snapshot exists before we actually snapshot
        for (Keyspace keyspace : keyspaces)
            if (keyspace.snapshotExists(tag))
                throw new IOException("Snapshot " + tag + " already exists.");


        for (Keyspace keyspace : keyspaces)
            keyspace.snapshot(tag, null);
    }

    /**
     * Takes the snapshot of a specific column family. A snapshot name must be specified.
     *
     * @param keyspaceName the keyspace which holds the specified column family
     * @param columnFamilyName the column family to snapshot
     * @param tag the tag given to the snapshot; may not be null or empty
     */
    public void takeColumnFamilySnapshot(String keyspaceName, String columnFamilyName, String tag) throws IOException
    {
        if (keyspaceName == null)
            throw new IOException("You must supply a keyspace name");
        if (operationMode.equals(Mode.JOINING))
            throw new IOException("Cannot snapshot until bootstrap completes");

        if (columnFamilyName == null)
            throw new IOException("You must supply a table name");
        if (columnFamilyName.contains("."))
            throw new IllegalArgumentException("Cannot take a snapshot of a secondary index by itself. Run snapshot on the table that owns the index.");

        if (tag == null || tag.equals(""))
            throw new IOException("You must supply a snapshot name.");

        Keyspace keyspace = getValidKeyspace(keyspaceName);
        if (keyspace.snapshotExists(tag))
            throw new IOException("Snapshot " + tag + " already exists.");

        keyspace.snapshot(tag, columnFamilyName);
    }

    private Keyspace getValidKeyspace(String keyspaceName) throws IOException
    {
        if (!Schema.instance.getKeyspaces().contains(keyspaceName))
        {
            throw new IOException("Keyspace " + keyspaceName + " does not exist");
        }
        return KeyspaceManager.instance.open(keyspaceName);
    }

    /**
     * Remove the snapshot with the given name from the given keyspaces.
     * If no tag is specified we will remove all snapshots.
     */
    public void clearSnapshot(String tag, String... keyspaceNames) throws IOException
    {
        if(tag == null)
            tag = "";

        Set<String> keyspaces = new HashSet<>();
        for (String dataDir : DatabaseDescriptor.instance.getAllDataFileLocations())
        {
            for(String keyspaceDir : new File(dataDir).list())
            {
                // Only add a ks if it has been specified as a param, assuming params were actually provided.
                if (keyspaceNames.length > 0 && !Arrays.asList(keyspaceNames).contains(keyspaceDir))
                    continue;
                keyspaces.add(keyspaceDir);
            }
        }

        for (String keyspace : keyspaces)
            Keyspace.clearSnapshot(tag, keyspace);

        if (logger.isDebugEnabled())
            logger.debug("Cleared out snapshot directories");
    }

    public Map<String, TabularData> getSnapshotDetails()
    {
        Map<String, TabularData> snapshotMap = new HashMap<>();
        for (Keyspace keyspace : KeyspaceManager.instance.all())
        {
            if (Keyspace.SYSTEM_KS.equals(keyspace.getName()))
                continue;

            for (ColumnFamilyStore cfStore : keyspace.getColumnFamilyStores())
            {
                for (Map.Entry<String, Pair<Long,Long>> snapshotDetail : cfStore.getSnapshotDetails().entrySet())
                {
                    TabularDataSupport data = (TabularDataSupport)snapshotMap.get(snapshotDetail.getKey());
                    if (data == null)
                    {
                        data = new TabularDataSupport(SnapshotDetailsTabularData.TABULAR_TYPE);
                        snapshotMap.put(snapshotDetail.getKey(), data);
                    }

                    SnapshotDetailsTabularData.from(snapshotDetail.getKey(), keyspace.getName(), cfStore.getColumnFamilyName(), snapshotDetail, data);
                }
            }
        }
        return snapshotMap;
    }

    public long trueSnapshotsSize()
    {
        long total = 0;
        for (Keyspace keyspace : KeyspaceManager.instance.all())
        {
            if (Keyspace.SYSTEM_KS.equals(keyspace.getName()))
                continue;

            for (ColumnFamilyStore cfStore : keyspace.getColumnFamilyStores())
            {
                total += cfStore.trueSnapshotsSize();
            }
        }

        return total;
    }

    /**
     * @param allowIndexes Allow index CF names to be passed in
     * @param autoAddIndexes Automatically add secondary indexes if a CF has them
     * @param keyspaceName keyspace
     * @param cfNames CFs
     * @throws java.lang.IllegalArgumentException when given CF name does not exist
     */
    public Iterable<ColumnFamilyStore> getValidColumnFamilies(boolean allowIndexes, boolean autoAddIndexes, String keyspaceName, String... cfNames) throws IOException
    {
        Keyspace keyspace = getValidKeyspace(keyspaceName);
        Set<ColumnFamilyStore> valid = new HashSet<>();

        if (cfNames.length == 0)
        {
            // all stores are interesting
            for (ColumnFamilyStore cfStore : keyspace.getColumnFamilyStores())
            {
                valid.add(cfStore);
                if (autoAddIndexes)
                {
                    for (SecondaryIndex si : cfStore.indexManager.getIndexes())
                    {
                        if (si.getIndexCfs() != null) {
                            logger.info("adding secondary index {} to operation", si.getIndexName());
                            valid.add(si.getIndexCfs());
                        }
                    }

                }
            }
            return valid;
        }
        // filter out interesting stores
        for (String cfName : cfNames)
        {
            //if the CF name is an index, just flush the CF that owns the index
            String baseCfName = cfName;
            String idxName = null;
            if (cfName.contains(".")) // secondary index
            {
                if(!allowIndexes)
                {
                   logger.warn("Operation not allowed on secondary Index table ({})", cfName);
                    continue;
                }

                String[] parts = cfName.split("\\.", 2);
                baseCfName = parts[0];
                idxName = parts[1];
            }

            ColumnFamilyStore cfStore = keyspace.getColumnFamilyStore(baseCfName);
            if (idxName != null)
            {
                Collection< SecondaryIndex > indexes = cfStore.indexManager.getIndexesByNames(new HashSet<>(Arrays.asList(cfName)));
                if (indexes.isEmpty())
                    logger.warn(String.format("Invalid index specified: %s/%s. Proceeding with others.", baseCfName, idxName));
                else
                    valid.add(Iterables.get(indexes, 0).getIndexCfs());
            }
            else
            {
                valid.add(cfStore);
                if(autoAddIndexes)
                {
                    for(SecondaryIndex si : cfStore.indexManager.getIndexes())
                    {
                        if (si.getIndexCfs() != null) {
                            logger.info("adding secondary index {} to operation", si.getIndexName());
                            valid.add(si.getIndexCfs());
                        }
                    }
                }
            }
        }
        return valid;
    }

    /**
     * Flush all memtables for a keyspace and column families.
     * @param keyspaceName
     * @param columnFamilies
     * @throws IOException
     */
    public void forceKeyspaceFlush(String keyspaceName, String... columnFamilies) throws IOException
    {
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(true, false, keyspaceName, columnFamilies))
        {
            logger.debug("Forcing flush on keyspace {}, CF {}", keyspaceName, cfStore.name);
            cfStore.forceBlockingFlush();
        }
    }

    /**
     * Sends JMX notification to subscribers.
     *
     * @param type Message type
     * @param message Message itself
     * @param userObject Arbitrary object to attach to notification
     */
    public void sendNotification(String type, String message, Object userObject)
    {
        Notification jmxNotification = new Notification(type, jmxObjectName, notificationSerialNumber.incrementAndGet(), message);
        jmxNotification.setUserData(userObject);
        sendNotification(jmxNotification);
    }

    public int forceRepairAsync(String keyspace, boolean isSequential, Collection<String> dataCenters, Collection<String> hosts, boolean primaryRange, boolean fullRepair, String... columnFamilies) throws IOException
    {
        // when repairing only primary range, dataCenter nor hosts can be set
        if (primaryRange && (dataCenters != null || hosts != null))
        {
            throw new IllegalArgumentException("You need to run primary range repair on all nodes in the cluster.");
        }
        Collection<Range<Token>> ranges = primaryRange ? LocatorConfig.instance.getLocalPrimaryRanges(keyspace) : LocatorConfig.instance.getLocalRanges(keyspace);

        return forceRepairAsync(keyspace, isSequential, dataCenters, hosts, ranges, fullRepair, columnFamilies);
    }

    public int forceRepairAsync(String keyspace, boolean isSequential, Collection<String> dataCenters, Collection<String> hosts, Collection<Range<Token>> ranges, boolean fullRepair, String... columnFamilies)
    {
        if (Keyspace.SYSTEM_KS.equals(keyspace) || ranges.isEmpty())
            return 0;

        int cmd = nextRepairCommand.incrementAndGet();
        if (ranges.size() > 0)
        {
            if (!FBUtilities.isUnix() && isSequential)
            {
                logger.warn("Snapshot-based repair is not yet supported on Windows.  Reverting to parallel repair.");
                isSequential = false;
            }
            new Thread(createRepairTask(cmd, keyspace, ranges, isSequential, dataCenters, hosts, fullRepair, columnFamilies)).start();
        }
        return cmd;
    }

    public int forceRepairAsync(String keyspace, boolean isSequential, boolean isLocal, boolean primaryRange, boolean fullRepair, String... columnFamilies)
    {
        // when repairing only primary range, you cannot repair only on local DC
        if (primaryRange && isLocal)
        {
            throw new IllegalArgumentException("You need to run primary range repair on all nodes in the cluster.");
        }
        Collection<Range<Token>> ranges = primaryRange ? LocatorConfig.instance.getLocalPrimaryRanges(keyspace) : LocatorConfig.instance.getLocalRanges(keyspace);
        return forceRepairAsync(keyspace, isSequential, isLocal, ranges, fullRepair, columnFamilies);
    }

    public int forceRepairAsync(final String keyspace, final boolean isSequential, final boolean isLocal, final Collection<Range<Token>> ranges, final boolean fullRepair, final String... columnFamilies)
    {
        if (Keyspace.SYSTEM_KS.equals(keyspace) || ranges.isEmpty())
            return 0;

        int cmd = nextRepairCommand.incrementAndGet();
        new Thread(createRepairTask(cmd, keyspace, ranges, isSequential, isLocal, fullRepair, columnFamilies)).start();
        return cmd;
    }

    public int forceRepairRangeAsync(String beginToken, String endToken, String keyspaceName, boolean isSequential, Collection<String> dataCenters, Collection<String> hosts, boolean fullRepair, String... columnFamilies) throws IOException
    {
        Collection<Range<Token>> repairingRange = createRepairRangeFrom(beginToken, endToken);

        logger.info("starting user-requested repair of range {} for keyspace {} and column families {}",
                           repairingRange, keyspaceName, columnFamilies);
        return forceRepairAsync(keyspaceName, isSequential, dataCenters, hosts, repairingRange, fullRepair, columnFamilies);
    }

    public int forceRepairRangeAsync(String beginToken, String endToken, String keyspaceName, boolean isSequential, boolean isLocal, boolean fullRepair, String... columnFamilies)
    {
        Collection<Range<Token>> repairingRange = createRepairRangeFrom(beginToken, endToken);

        logger.info("starting user-requested repair of range {} for keyspace {} and column families {}",
                           repairingRange, keyspaceName, columnFamilies);
        return forceRepairAsync(keyspaceName, isSequential, isLocal, repairingRange, fullRepair, columnFamilies);
    }

    /**
     * Create collection of ranges that match ring layout from given tokens.
     *
     * @param beginToken beginning token of the range
     * @param endToken end token of the range
     * @return collection of ranges that match ring layout in TokenMetadata
     */
    @SuppressWarnings("unchecked")
    private Collection<Range<Token>> createRepairRangeFrom(String beginToken, String endToken)
    {
        Token parsedBeginToken = LocatorConfig.instance.getPartitioner().getTokenFactory().fromString(beginToken);
        Token parsedEndToken = LocatorConfig.instance.getPartitioner().getTokenFactory().fromString(endToken);

        Deque<Range<Token>> repairingRange = new ArrayDeque<>();
        // Break up given range to match ring layout in TokenMetadata
        Token previous = LocatorConfig.instance.getTokenMetadata().getPredecessor(TokenMetadata.firstToken(LocatorConfig.instance.getTokenMetadata().sortedTokens(), parsedEndToken));
        while (parsedBeginToken.compareTo(previous) < 0)
        {
            repairingRange.addFirst(new Range<>(previous, parsedEndToken, LocatorConfig.instance.getPartitioner()));

            parsedEndToken = previous;
            previous = LocatorConfig.instance.getTokenMetadata().getPredecessor(previous);
        }
        repairingRange.addFirst(new Range<>(parsedBeginToken, parsedEndToken, LocatorConfig.instance.getPartitioner()));

        return repairingRange;
    }

    private FutureTask<Object> createRepairTask(int cmd,
                                                String keyspace,
                                                Collection<Range<Token>> ranges,
                                                boolean isSequential,
                                                boolean isLocal,
                                                boolean fullRepair,
                                                String... columnFamilies)
    {
        Set<String> dataCenters = null;
        if (isLocal)
        {
            dataCenters = Sets.newHashSet(DatabaseDescriptor.instance.getLocalDataCenter());
        }
        return createRepairTask(cmd, keyspace, ranges, isSequential, dataCenters, null, fullRepair, columnFamilies);
    }

    private FutureTask<Object> createRepairTask(final int cmd,
                                                final String keyspace,
                                                final Collection<Range<Token>> ranges,
                                                final boolean isSequential,
                                                final Collection<String> dataCenters,
                                                final Collection<String> hosts,
                                                final boolean fullRepair,
                                                final String... columnFamilies)
    {
        if (dataCenters != null && !dataCenters.contains(DatabaseDescriptor.instance.getLocalDataCenter()))
        {
            throw new IllegalArgumentException("the local data center must be part of the repair");
        }

        return new FutureTask<>(new WrappedRunnable()
        {
            protected void runMayThrow() throws Exception
            {
                String message = String.format("Starting repair command #%d, repairing %d ranges for keyspace %s (seq=%b, full=%b)", cmd, ranges.size(), keyspace, isSequential, fullRepair);
                logger.info(message);
                sendNotification("repair", message, new int[]{cmd, ActiveRepairService.Status.STARTED.ordinal()});

                if (isSequential && !fullRepair)
                {
                    message = "It is not possible to mix sequential repair and incremental repairs.";
                    logger.error(message);
                    sendNotification("repair", message, new int[]{cmd, ActiveRepairService.Status.FINISHED.ordinal()});
                    return;
                }

                Set<InetAddress> allNeighbors = new HashSet<>();
                Map<Range, Set<InetAddress>> rangeToNeighbors = new HashMap<>();
                for (Range<Token> range : ranges)
                {
                    try
                    {
                        Set<InetAddress> neighbors = ActiveRepairService.instance.getNeighbors(keyspace, range, dataCenters, hosts);
                        rangeToNeighbors.put(range, neighbors);
                        allNeighbors.addAll(neighbors);
                    }
                    catch (IllegalArgumentException e)
                    {
                        logger.error("Repair failed:", e);
                        sendNotification("repair", e.getMessage(), new int[]{cmd, ActiveRepairService.Status.FINISHED.ordinal()});
                        return;
                    }
                }

                // Validate columnfamilies
                List<ColumnFamilyStore> columnFamilyStores = new ArrayList<>();
                try
                {
                    Iterables.addAll(columnFamilyStores, getValidColumnFamilies(false, false, keyspace, columnFamilies));
                }
                catch (IllegalArgumentException e)
                {
                    sendNotification("repair", e.getMessage(), new int[]{cmd, ActiveRepairService.Status.FINISHED.ordinal()});
                    return;
                }

                UUID parentSession = null;
                if (!fullRepair)
                {
                    try
                    {
                        parentSession = ActiveRepairService.instance.prepareForRepair(allNeighbors, ranges, columnFamilyStores);
                    }
                    catch (Throwable t)
                    {
                        sendNotification("repair", String.format("Repair failed with error %s", t.getMessage()), new int[]{cmd, ActiveRepairService.Status.FINISHED.ordinal()});
                        return;
                    }
                }

                List<RepairFuture> futures = new ArrayList<>(ranges.size());
                String[] cfnames = new String[columnFamilyStores.size()];
                for (int i = 0; i < columnFamilyStores.size(); i++)
                {
                    cfnames[i] = columnFamilyStores.get(i).name;
                }
                for (Range<Token> range : ranges)
                {
                    RepairFuture future = ActiveRepairService.instance.submitRepairSession(parentSession, range, keyspace, isSequential, rangeToNeighbors.get(range), cfnames);
                    if (future == null)
                        continue;
                    futures.add(future);
                    // wait for a session to be done with its differencing before starting the next one
                    try
                    {
                        future.session.differencingDone.await();
                    }
                    catch (InterruptedException e)
                    {
                        message = "Interrupted while waiting for the differencing of repair session " + future.session + " to be done. Repair may be imprecise.";
                        logger.error(message, e);
                        sendNotification("repair", message, new int[]{cmd, ActiveRepairService.Status.SESSION_FAILED.ordinal()});
                    }
                }

                boolean successful = true;
                for (RepairFuture future : futures)
                {
                    try
                    {
                        future.get();
                        message = String.format("Repair session %s for range %s finished", future.session.getId(), future.session.getRange().toString());
                        logger.info(message);
                        sendNotification("repair", message, new int[]{cmd, ActiveRepairService.Status.SESSION_SUCCESS.ordinal()});
                    }
                    catch (ExecutionException e)
                    {
                        successful = false;
                        message = String.format("Repair session %s for range %s failed with error %s", future.session.getId(), future.session.getRange().toString(), e.getCause().getMessage());
                        logger.error(message, e);
                        sendNotification("repair", message, new int[]{cmd, ActiveRepairService.Status.SESSION_FAILED.ordinal()});
                    }
                    catch (Exception e)
                    {
                        successful = false;
                        message = String.format("Repair session %s for range %s failed with error %s", future.session.getId(), future.session.getRange().toString(), e.getMessage());
                        logger.error(message, e);
                        sendNotification("repair", message, new int[]{cmd, ActiveRepairService.Status.SESSION_FAILED.ordinal()});
                    }
                }
                if (!fullRepair)
                    ActiveRepairService.instance.finishParentSession(parentSession, allNeighbors, successful);
                sendNotification("repair", String.format("Repair command #%d finished", cmd), new int[]{cmd, ActiveRepairService.Status.FINISHED.ordinal()});
            }
        }, null);
    }

    public void forceTerminateAllRepairSessions() {
        ActiveRepairService.instance.terminateSessions();
    }

    /* End of MBean interface methods */

    /**
     * Get all ranges that span the ring given a set
     * of tokens. All ranges are in sorted order of
     * ranges.
     * @return ranges in sorted order
    */
    public List<Range<Token>> getAllRanges(List<Token> sortedTokens)
    {
        if (logger.isDebugEnabled())
            logger.debug("computing ranges for {}", StringUtils.join(sortedTokens, ", "));

        if (sortedTokens.isEmpty())
            return Collections.emptyList();
        int size = sortedTokens.size();
        List<Range<Token>> ranges = new ArrayList<>(size + 1);
        for (int i = 1; i < size; ++i)
        {
            Range<Token> range = new Range<>(sortedTokens.get(i - 1), sortedTokens.get(i), LocatorConfig.instance.getPartitioner());
            ranges.add(range);
        }
        Range<Token> range = new Range<>(sortedTokens.get(size - 1), sortedTokens.get(0), LocatorConfig.instance.getPartitioner());
        ranges.add(range);

        return ranges;
    }

    @Override
    public List<InetAddress> getNaturalEndpoints(String keyspaceName, String cf, String key)
    {
        return LocatorConfig.instance.getNaturalEndpoints(keyspaceName, cf, key);
    }

    @Override
    public List<InetAddress> getNaturalEndpoints(String keyspaceName, ByteBuffer key)
    {
        return LocatorConfig.instance.getNaturalEndpoints(keyspaceName, key);
    }

    public void setLoggingLevel(String classQualifier, String rawLevel) throws Exception
    {
        ch.qos.logback.classic.Logger logBackLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(classQualifier);

        // if both classQualifer and rawLevel are empty, reload from configuration
        if (StringUtils.isBlank(classQualifier) && StringUtils.isBlank(rawLevel) )
        {
            JMXConfiguratorMBean jmxConfiguratorMBean = JMX.newMBeanProxy(ManagementFactory.getPlatformMBeanServer(), 
                    new ObjectName("ch.qos.logback.classic:Name=default,Type=ch.qos.logback.classic.jmx.JMXConfigurator"),
                    JMXConfiguratorMBean.class);
            jmxConfiguratorMBean.reloadDefaultConfiguration();
            return;
        }
        // classQualifer is set, but blank level given
        else if (StringUtils.isNotBlank(classQualifier) && StringUtils.isBlank(rawLevel) )
        {
            if (logBackLogger.getLevel() != null || hasAppenders(logBackLogger))
                logBackLogger.setLevel(null);
            return;
        }

        ch.qos.logback.classic.Level level = ch.qos.logback.classic.Level.toLevel(rawLevel);
        logBackLogger.setLevel(level);
        logger.info("set log level to {} for classes under '{}' (if the level doesn't look like '{}' then the logger couldn't parse '{}')", level, classQualifier, rawLevel, rawLevel);
    }
    
    /**
     * @return the runtime logging levels for all the configured loggers
     */
    @Override
    public Map<String,String>getLoggingLevels() {
        Map<String, String> logLevelMaps = Maps.newLinkedHashMap();
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        for (ch.qos.logback.classic.Logger logger : lc.getLoggerList())
        {
            if(logger.getLevel() != null || hasAppenders(logger))
                logLevelMaps.put(logger.getName(), logger.getLevel().toString());
        }
        return logLevelMaps;
    }

    private boolean hasAppenders(ch.qos.logback.classic.Logger logger) {
        Iterator<Appender<ILoggingEvent>> it = logger.iteratorForAppenders();
        return it.hasNext();
    }

    /**
     * @return list of Token ranges (_not_ keys!) together with estimated key count,
     *      breaking up the data this node is responsible for into pieces of roughly keysPerSplit
     */
    public List<Pair<Range<Token>, Long>> getSplits(String keyspaceName, String cfName, Range<Token> range, int keysPerSplit)
    {
        Keyspace t = KeyspaceManager.instance.open(keyspaceName);
        ColumnFamilyStore cfs = t.getColumnFamilyStore(cfName);
        List<DecoratedKey> keys = keySamples(Collections.singleton(cfs), range);

        long totalRowCountEstimate = cfs.estimatedKeysForRange(range);

        // splitCount should be much smaller than number of key samples, to avoid huge sampling error
        int minSamplesPerSplit = 4;
        int maxSplitCount = keys.size() / minSamplesPerSplit + 1;
        int splitCount = Math.max(1, Math.min(maxSplitCount, (int)(totalRowCountEstimate / keysPerSplit)));

        List<Token> tokens = keysToTokens(range, keys);
        return getSplits(tokens, splitCount, cfs);
    }

    private List<Pair<Range<Token>, Long>> getSplits(List<Token> tokens, int splitCount, ColumnFamilyStore cfs)
    {
        double step = (double) (tokens.size() - 1) / splitCount;
        Token prevToken = tokens.get(0);
        List<Pair<Range<Token>, Long>> splits = Lists.newArrayListWithExpectedSize(splitCount);
        for (int i = 1; i <= splitCount; i++)
        {
            int index = (int) Math.round(i * step);
            Token token = tokens.get(index);
            Range<Token> range = new Range<>(prevToken, token, LocatorConfig.instance.getPartitioner());
            // always return an estimate > 0 (see CASSANDRA-7322)
            splits.add(Pair.create(range, Math.max(cfs.metadata.getMinIndexInterval(), cfs.estimatedKeysForRange(range))));
            prevToken = token;
        }
        return splits;
    }

    private List<Token> keysToTokens(Range<Token> range, List<DecoratedKey> keys)
    {
        List<Token> tokens = Lists.newArrayListWithExpectedSize(keys.size() + 2);
        tokens.add(range.left);
        for (DecoratedKey key : keys)
            tokens.add(key.getToken());
        tokens.add(range.right);
        return tokens;
    }

    private List<DecoratedKey> keySamples(Iterable<ColumnFamilyStore> cfses, Range<Token> range)
    {
        List<DecoratedKey> keys = new ArrayList<>();
        for (ColumnFamilyStore cfs : cfses)
            Iterables.addAll(keys, cfs.keySamples(range));
        FBUtilities.sortSampledKeys(keys, range);
        return keys;
    }

    /**
     * Broadcast leaving status and update local tokenMetadata accordingly
     */
    private void startLeaving()
    {
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.leaving(getLocalTokens()));
        LocatorConfig.instance.getTokenMetadata().addLeavingEndpoint(DatabaseDescriptor.instance.getBroadcastAddress());
        PendingRangeCalculatorService.instance.update();
    }

    public void decommission() throws InterruptedException
    {
        if (!LocatorConfig.instance.getTokenMetadata().isMember(DatabaseDescriptor.instance.getBroadcastAddress()))
            throw new UnsupportedOperationException("local node is not a member of the token ring yet");
        if (LocatorConfig.instance.getTokenMetadata().cloneAfterAllLeft().sortedTokens().size() < 2)
            throw new UnsupportedOperationException("no other normal nodes in the ring; decommission would be pointless");

        PendingRangeCalculatorService.instance.blockUntilFinished();
        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            if (LocatorConfig.instance.getTokenMetadata().getPendingRanges(keyspaceName, DatabaseDescriptor.instance.getBroadcastAddress()).size() > 0)
                throw new UnsupportedOperationException("data is currently moving to this node; unable to leave the ring");
        }

        if (logger.isDebugEnabled())
            logger.debug("DECOMMISSIONING");
        startLeaving();
        setMode(Mode.LEAVING, "sleeping " + DatabaseDescriptor.instance.getRingDelay() + " ms for pending range setup", true);
        Thread.sleep(DatabaseDescriptor.instance.getRingDelay());

        Runnable finishLeaving = new Runnable()
        {
            public void run()
            {
                shutdownClientServers();
                Gossiper.instance.stop();
                MessagingService.instance.shutdown();
                StageManager.instance.shutdownNow();
                setMode(Mode.DECOMMISSIONED, true);
                // let op be responsible for killing the process
            }
        };
        unbootstrap(finishLeaving);
    }

    private void leaveRing()
    {
        SystemKeyspace.instance.setBootstrapState(SystemKeyspace.BootstrapState.NEEDS_BOOTSTRAP);
        LocatorConfig.instance.getTokenMetadata().removeEndpoint(DatabaseDescriptor.instance.getBroadcastAddress());
        PendingRangeCalculatorService.instance.update();

        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.left(getLocalTokens(),Gossiper.computeExpireTime()));
        int delay = Math.max(DatabaseDescriptor.instance.getRingDelay(), Gossiper.intervalInMillis * 2);
        logger.info("Announcing that I have left the ring for {}ms", delay);
        Uninterruptibles.sleepUninterruptibly(delay, TimeUnit.MILLISECONDS);
    }

    private void unbootstrap(Runnable onFinish)
    {
        Map<String, Multimap<Range<Token>, InetAddress>> rangesToStream = new HashMap<>();

        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            Multimap<Range<Token>, InetAddress> rangesMM = getChangedRangesForLeaving(keyspaceName, DatabaseDescriptor.instance.getBroadcastAddress());

            if (logger.isDebugEnabled())
                logger.debug("Ranges needing transfer are [{}]", StringUtils.join(rangesMM.keySet(), ","));

            rangesToStream.put(keyspaceName, rangesMM);
        }

        setMode(Mode.LEAVING, "streaming data to other nodes", true);

        Future<StreamState> streamSuccess = streamRanges(rangesToStream);
        Future<StreamState> hintsSuccess = streamHints();

        // wait for the transfer runnables to signal the latch.
        logger.debug("waiting for stream aks.");
        try
        {
            streamSuccess.get();
            hintsSuccess.get();
        }
        catch (ExecutionException | InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        logger.debug("stream acks all received.");
        leaveRing();
        onFinish.run();
    }

    private Future<StreamState> streamHints()
    {
        // StreamPlan will not fail if there are zero files to transfer, so flush anyway (need to get any in-memory hints, as well)
        ColumnFamilyStore hintsCF = KeyspaceManager.instance.open(Keyspace.SYSTEM_KS).getColumnFamilyStore(SystemKeyspace.HINTS_CF);
        FBUtilities.waitOnFuture(hintsCF.forceFlush());

        // gather all live nodes in the cluster that aren't also leaving
        List<InetAddress> candidates = new ArrayList<>(LocatorConfig.instance.getTokenMetadata().cloneAfterAllLeft().getAllEndpoints());
        candidates.remove(DatabaseDescriptor.instance.getBroadcastAddress());
        for (Iterator<InetAddress> iter = candidates.iterator(); iter.hasNext(); )
        {
            InetAddress address = iter.next();
            if (!FailureDetector.instance.isAlive(address))
                iter.remove();
        }

        if (candidates.isEmpty())
        {
            logger.warn("Unable to stream hints since no live endpoints seen");
            return Futures.immediateFuture(null);
        }
        else
        {
            // stream to the closest peer as chosen by the snitch
            LocatorConfig.instance.getEndpointSnitch().sortByProximity(DatabaseDescriptor.instance.getBroadcastAddress(), candidates);
            InetAddress hintsDestinationHost = candidates.get(0);

            // stream all hints -- range list will be a singleton of "the entire ring"
            Token token = LocatorConfig.instance.getPartitioner().getMinimumToken();
            List<Range<Token>> ranges = Collections.singletonList(new Range<>(token, token, LocatorConfig.instance.getPartitioner()));

            return new StreamPlan("Hints", DatabaseDescriptor.instance, Schema.instance,
                                  KeyspaceManager.instance, StreamManager.instance, DBConfig.instance).transferRanges(hintsDestinationHost,
                                                                      Keyspace.SYSTEM_KS,
                                                                      ranges,
                                                                      SystemKeyspace.HINTS_CF)
                                                      .execute();
        }
    }

    public void move(String newToken) throws IOException
    {
        try
        {
            LocatorConfig.instance.getPartitioner().getTokenFactory().validate(newToken);
        }
        catch (ConfigurationException e)
        {
            throw new IOException(e.getMessage());
        }
        move(LocatorConfig.instance.getPartitioner().getTokenFactory().fromString(newToken));
    }

    /**
     * move the node to new token or find a new token to boot to according to load
     *
     * @param newToken new token to boot to, or if null, find balanced token to boot to
     *
     * @throws IOException on any I/O operation error
     */
    private void move(Token newToken) throws IOException
    {
        if (newToken == null)
            throw new IOException("Can't move to the undefined (null) token.");

        if (LocatorConfig.instance.getTokenMetadata().sortedTokens().contains(newToken))
            throw new IOException("target token " + newToken + " is already owned by another node.");

        // address of the current node
        InetAddress localAddress = DatabaseDescriptor.instance.getBroadcastAddress();

        // This doesn't make any sense in a vnodes environment.
        if (LocatorConfig.instance.getTokenMetadata().getTokens(localAddress).size() > 1)
        {
            logger.error("Invalid request to move(Token); This node has more than one token and cannot be moved thusly.");
            throw new UnsupportedOperationException("This node has more than one token and cannot be moved thusly.");
        }

        List<String> keyspacesToProcess = Schema.instance.getNonSystemKeyspaces();

        PendingRangeCalculatorService.instance.blockUntilFinished();
        // checking if data is moving to this node
        for (String keyspaceName : keyspacesToProcess)
        {
            if (LocatorConfig.instance.getTokenMetadata().getPendingRanges(keyspaceName, localAddress).size() > 0)
                throw new UnsupportedOperationException("data is currently moving to this node; unable to leave the ring");
        }

        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.moving(newToken));
        setMode(Mode.MOVING, String.format("Moving %s from %s to %s.", localAddress, getLocalTokens().iterator().next(), newToken), true);

        setMode(Mode.MOVING, String.format("Sleeping %s ms before start streaming/fetching ranges", DatabaseDescriptor.instance.getRingDelay()), true);
        Uninterruptibles.sleepUninterruptibly(DatabaseDescriptor.instance.getRingDelay(), TimeUnit.MILLISECONDS);

        RangeRelocator relocator = new RangeRelocator(Collections.singleton(newToken), keyspacesToProcess);

        if (relocator.streamsNeeded())
        {
            setMode(Mode.MOVING, "fetching new ranges and streaming old ranges", true);
            try
            {
                relocator.stream().get();
            }
            catch (ExecutionException | InterruptedException e)
            {
                throw new RuntimeException("Interrupted while waiting for stream/fetch ranges to finish: " + e.getMessage());
            }
        }
        else
        {
            setMode(Mode.MOVING, "No ranges to fetch/stream", true);
        }

        setTokens(Collections.singleton(newToken)); // setting new token as we have everything settled

        if (logger.isDebugEnabled())
            logger.debug("Successfully moved to new token {}", getLocalTokens().iterator().next());
    }

    private class RangeRelocator
    {
        private final StreamPlan streamPlan = new StreamPlan("Relocation", DatabaseDescriptor.instance, Schema.instance,
                                                             KeyspaceManager.instance, StreamManager.instance, DBConfig.instance);

        private RangeRelocator(Collection<Token> tokens, List<String> keyspaceNames)
        {
            calculateToFromStreams(tokens, keyspaceNames);
        }

        private void calculateToFromStreams(Collection<Token> newTokens, List<String> keyspaceNames)
        {
            InetAddress localAddress = DatabaseDescriptor.instance.getBroadcastAddress();
            IEndpointSnitch snitch = LocatorConfig.instance.getEndpointSnitch();
            TokenMetadata tokenMetaCloneAllSettled = LocatorConfig.instance.getTokenMetadata().cloneAfterAllSettled();
            // clone to avoid concurrent modification in calculateNaturalEndpoints
            TokenMetadata tokenMetaClone = LocatorConfig.instance.getTokenMetadata().cloneOnlyTokenMap();

            for (String keyspace : keyspaceNames)
            {
                logger.debug("Calculating ranges to stream and request for keyspace {}", keyspace);
                for (Token newToken : newTokens)
                {
                    // replication strategy of the current keyspace (aka table)
                    AbstractReplicationStrategy strategy = KeyspaceManager.instance.open(keyspace).getReplicationStrategy();

                    // getting collection of the currently used ranges by this keyspace
                    Collection<Range<Token>> currentRanges = LocatorConfig.instance.getRangesForEndpoint(keyspace, localAddress);
                    // collection of ranges which this node will serve after move to the new token
                    Collection<Range<Token>> updatedRanges = strategy.getPendingAddressRanges(tokenMetaClone, newToken, localAddress);

                    // ring ranges and endpoints associated with them
                    // this used to determine what nodes should we ping about range data
                    Multimap<Range<Token>, InetAddress> rangeAddresses = strategy.getRangeAddresses(tokenMetaClone);

                    // calculated parts of the ranges to request/stream from/to nodes in the ring
                    Pair<Set<Range<Token>>, Set<Range<Token>>> rangesPerKeyspace = calculateStreamAndFetchRanges(currentRanges, updatedRanges);

                    /**
                     * In this loop we are going through all ranges "to fetch" and determining
                     * nodes in the ring responsible for data we are interested in
                     */
                    Multimap<Range<Token>, InetAddress> rangesToFetchWithPreferredEndpoints = ArrayListMultimap.create();
                    for (Range<Token> toFetch : rangesPerKeyspace.right)
                    {
                        for (Range<Token> range : rangeAddresses.keySet())
                        {
                            if (range.contains(toFetch))
                            {
                                List<InetAddress> endpoints = null;

                                if (RangeStreamer.useStrictConsistency)
                                {
                                    Set<InetAddress> oldEndpoints = Sets.newHashSet(rangeAddresses.get(range));
                                    Set<InetAddress> newEndpoints = Sets.newHashSet(strategy.calculateNaturalEndpoints(toFetch.right, tokenMetaCloneAllSettled));

                                    //Due to CASSANDRA-5953 we can have a higher RF then we have endpoints.
                                    //So we need to be careful to only be strict when endpoints == RF
                                    if (oldEndpoints.size() == strategy.getReplicationFactor())
                                    {
                                        oldEndpoints.removeAll(newEndpoints);

                                        //No relocation required 
                                        if (oldEndpoints.isEmpty())
                                            continue;

                                        assert oldEndpoints.size() == 1 : "Expected 1 endpoint but found " + oldEndpoints.size();
                                    }

                                    endpoints = Lists.newArrayList(oldEndpoints.iterator().next());
                                }
                                else
                                {
                                    endpoints = snitch.getSortedListByProximity(localAddress, rangeAddresses.get(range));
                                }

                                // storing range and preferred endpoint set
                                rangesToFetchWithPreferredEndpoints.putAll(toFetch, endpoints);
                            }
                        }

                        Collection<InetAddress> addressList = rangesToFetchWithPreferredEndpoints.get(toFetch);
                        if (addressList == null || addressList.isEmpty())
                            continue;

                        if (RangeStreamer.useStrictConsistency)
                        {
                            if (addressList.size() > 1)
                                throw new IllegalStateException("Multiple strict sources found for " + toFetch);

                            InetAddress sourceIp = addressList.iterator().next();
                            if (Gossiper.instance.isEnabled() && !Gossiper.instance.getEndpointStateForEndpoint(sourceIp).isAlive())
                                throw new RuntimeException("A node required to move the data consistently is down ("+sourceIp+").  If you wish to move the data from a potentially inconsistent replica, restart the node with -Dcassandra.consistent.rangemovement=false");
                        }
                    }

                    // calculating endpoints to stream current ranges to if needed
                    // in some situations node will handle current ranges as part of the new ranges
                    Multimap<InetAddress, Range<Token>> endpointRanges = HashMultimap.create();
                    for (Range<Token> toStream : rangesPerKeyspace.left)
                    {
                        Set<InetAddress> currentEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(toStream.right, tokenMetaClone));
                        Set<InetAddress> newEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(toStream.right, tokenMetaCloneAllSettled));
                        logger.debug("Range: {} Current endpoints: {} New endpoints: {}", toStream, currentEndpoints, newEndpoints);
                        for (InetAddress address : Sets.difference(newEndpoints, currentEndpoints))
                        {
                            logger.debug("Range {} has new owner {}", toStream, address);
                            endpointRanges.put(address, toStream);
                        }
                    }

                    // stream ranges
                    for (InetAddress address : endpointRanges.keySet())
                    {
                        logger.debug("Will stream range {} of keyspace {} to endpoint {}", endpointRanges.get(address), keyspace, address);
                        streamPlan.transferRanges(address, keyspace, endpointRanges.get(address));
                    }

                    // stream requests
                    Multimap<InetAddress, Range<Token>> workMap = RangeStreamer.getWorkMap(rangesToFetchWithPreferredEndpoints, DatabaseDescriptor.instance, FailureDetector.instance);
                    for (InetAddress address : workMap.keySet())
                    {
                        logger.debug("Will request range {} of keyspace {} from endpoint {}", workMap.get(address), keyspace, address);
                        streamPlan.requestRanges(address, keyspace, workMap.get(address));
                    }

                    if (logger.isDebugEnabled())
                        logger.debug("Keyspace {}: work map {}.", keyspace, workMap);
                }
            }
        }

        public Future<StreamState> stream()
        {
            return streamPlan.execute();
        }

        public boolean streamsNeeded()
        {
            return !streamPlan.isEmpty();
        }
    }

    /**
     * Get the status of a token removal.
     */
    public String getRemovalStatus()
    {
        if (removingNode == null) {
            return "No token removals in process.";
        }
        return String.format("Removing token (%s). Waiting for replication confirmation from [%s].",
                             LocatorConfig.instance.getTokenMetadata().getToken(removingNode),
                             StringUtils.join(replicatingNodes, ","));
    }

    /**
     * Force a remove operation to complete. This may be necessary if a remove operation
     * blocks forever due to node/stream failure. removeToken() must be called
     * first, this is a last resort measure.  No further attempt will be made to restore replicas.
     */
    public void forceRemoveCompletion()
    {
        if (!replicatingNodes.isEmpty()  || !LocatorConfig.instance.getTokenMetadata().getLeavingEndpoints().isEmpty())
        {
            logger.warn("Removal not confirmed for for {}", StringUtils.join(this.replicatingNodes, ","));
            for (InetAddress endpoint : LocatorConfig.instance.getTokenMetadata().getLeavingEndpoints())
            {
                UUID hostId = LocatorConfig.instance.getTokenMetadata().getHostId(endpoint);
                Gossiper.instance.advertiseTokenRemoved(endpoint, hostId);
                excise(LocatorConfig.instance.getTokenMetadata().getTokens(endpoint), endpoint);
            }
            replicatingNodes.clear();
            removingNode = null;
        }
        else
        {
            logger.warn("No tokens to force removal on, call 'removenode' first");
        }
    }

    /**
     * Remove a node that has died, attempting to restore the replica count.
     * If the node is alive, decommission should be attempted.  If decommission
     * fails, then removeToken should be called.  If we fail while trying to
     * restore the replica count, finally forceRemoveCompleteion should be
     * called to forcibly remove the node without regard to replica count.
     *
     * @param hostIdString token for the node
     */
    public void removeNode(String hostIdString)
    {
        InetAddress myAddress = DatabaseDescriptor.instance.getBroadcastAddress();
        UUID localHostId = LocatorConfig.instance.getTokenMetadata().getHostId(myAddress);
        UUID hostId = UUID.fromString(hostIdString);
        InetAddress endpoint = LocatorConfig.instance.getTokenMetadata().getEndpointForHostId(hostId);

        if (endpoint == null)
            throw new UnsupportedOperationException("Host ID not found.");

        Collection<Token> tokens = LocatorConfig.instance.getTokenMetadata().getTokens(endpoint);

        if (endpoint.equals(myAddress))
             throw new UnsupportedOperationException("Cannot remove self");

        if (Gossiper.instance.getLiveMembers().contains(endpoint))
            throw new UnsupportedOperationException("Node " + endpoint + " is alive and owns this ID. Use decommission command to remove it from the ring");

        // A leaving endpoint that is dead is already being removed.
        if (LocatorConfig.instance.getTokenMetadata().isLeaving(endpoint))
            logger.warn("Node {} is already being removed, continuing removal anyway", endpoint);

        if (!replicatingNodes.isEmpty())
            throw new UnsupportedOperationException("This node is already processing a removal. Wait for it to complete, or use 'removenode force' if this has failed.");

        // Find the endpoints that are going to become responsible for data
        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            // if the replication factor is 1 the data is lost so we shouldn't wait for confirmation
            if (KeyspaceManager.instance.open(keyspaceName).getReplicationStrategy().getReplicationFactor() == 1)
                continue;

            // get all ranges that change ownership (that is, a node needs
            // to take responsibility for new range)
            Multimap<Range<Token>, InetAddress> changedRanges = getChangedRangesForLeaving(keyspaceName, endpoint);
            IFailureDetector failureDetector = FailureDetector.instance;
            for (InetAddress ep : changedRanges.values())
            {
                if (failureDetector.isAlive(ep))
                    replicatingNodes.add(ep);
                else
                    logger.warn("Endpoint {} is down and will not receive data for re-replication of {}", ep, endpoint);
            }
        }
        removingNode = endpoint;

        LocatorConfig.instance.getTokenMetadata().addLeavingEndpoint(endpoint);
        PendingRangeCalculatorService.instance.update();

        // the gossiper will handle spoofing this node's state to REMOVING_TOKEN for us
        // we add our own token so other nodes to let us know when they're done
        Gossiper.instance.advertiseRemoving(endpoint, hostId, localHostId);

        // kick off streaming commands
        restoreReplicaCount(endpoint, myAddress);

        // wait for ReplicationFinishedVerbHandler to signal we're done
        while (!replicatingNodes.isEmpty())
        {
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        }

        excise(tokens, endpoint);

        // gossiper will indicate the token has left
        Gossiper.instance.advertiseTokenRemoved(endpoint, hostId);

        replicatingNodes.clear();
        removingNode = null;
    }

    public void confirmReplication(InetAddress node)
    {
        // replicatingNodes can be empty in the case where this node used to be a removal coordinator,
        // but restarted before all 'replication finished' messages arrived. In that case, we'll
        // still go ahead and acknowledge it.
        if (!replicatingNodes.isEmpty())
        {
            replicatingNodes.remove(node);
        }
        else
        {
            logger.info("Received unexpected REPLICATION_FINISHED message from {}. Was this node recently a removal coordinator?", node);
        }
    }

    public boolean isClientMode()
    {
        return isClientMode;
    }

    public synchronized void requestGC()
    {
        if (hasUnreclaimedSpace())
        {
            logger.info("requesting GC to free disk space");
            System.gc();
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
    }

    private boolean hasUnreclaimedSpace()
    {
        for (ColumnFamilyStore cfs : ColumnFamilyStoreManager.instance.all())
        {
            if (cfs.hasUnreclaimedSpace())
                return true;
        }
        return false;
    }

    public String getOperationMode()
    {
        return operationMode.toString();
    }

    public String getDrainProgress()
    {
        return String.format("Drained %s/%s ColumnFamilies", remainingCFs, totalCFs);
    }

    /**
     * Shuts node off to writes, empties memtables and the commit log.
     * There are two differences between drain and the normal shutdown hook:
     * - Drain waits for in-progress streaming to complete
     * - Drain flushes *all* columnfamilies (shutdown hook only flushes non-durable CFs)
     */
    public synchronized void drain() throws IOException, InterruptedException, ExecutionException
    {
        ExecutorService counterMutationStage = StageManager.instance.getStage(Stage.COUNTER_MUTATION);
        ExecutorService mutationStage = StageManager.instance.getStage(Stage.MUTATION);
        if (mutationStage.isTerminated() && counterMutationStage.isTerminated())
        {
            logger.warn("Cannot drain node (did it already happen?)");
            return;
        }
        setMode(Mode.DRAINING, "starting drain process", true);
        shutdownClientServers();
        StorageServiceExecutors.instance.optionalTasks.shutdown();
        Gossiper.instance.stop();

        setMode(Mode.DRAINING, "shutting down MessageService", false);
        MessagingService.instance.shutdown();

        setMode(Mode.DRAINING, "clearing mutation stage", false);
        counterMutationStage.shutdown();
        mutationStage.shutdown();
        counterMutationStage.awaitTermination(3600, TimeUnit.SECONDS);
        mutationStage.awaitTermination(3600, TimeUnit.SECONDS);

        StorageProxy.instance.instance.verifyNoHintsInProgress();

        setMode(Mode.DRAINING, "flushing column families", false);
        // count CFs first, since forceFlush could block for the flushWriter to get a queue slot empty
        totalCFs = 0;
        for (Keyspace keyspace : KeyspaceManager.instance.nonSystem())
            totalCFs += keyspace.getColumnFamilyStores().size();
        remainingCFs = totalCFs;
        // flush
        List<Future<?>> flushes = new ArrayList<>();
        for (Keyspace keyspace : KeyspaceManager.instance.nonSystem())
        {
            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
                flushes.add(cfs.forceFlush());
        }
        // wait for the flushes.
        // TODO this is a godawful way to track progress, since they flush in parallel.  a long one could
        // thus make several short ones "instant" if we wait for them later.
        for (Future f : flushes)
        {
            FBUtilities.waitOnFuture(f);
            remainingCFs--;
        }
        // flush the system ones after all the rest are done, just in case flushing modifies any system state
        // like CASSANDRA-5151. don't bother with progress tracking since system data is tiny.
        flushes.clear();
        for (Keyspace keyspace : KeyspaceManager.instance.system())
        {
            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
                flushes.add(cfs.forceFlush());
        }
        FBUtilities.waitOnFutures(flushes);

        BatchlogManager.instance.shutdown();

        // whilst we've flushed all the CFs, which will have recycled all completed segments, we want to ensure
        // there are no segments to replay, so we force the recycling of any remaining (should be at most one)
        CommitLog.instance.forceRecycleAllSegments();

        ColumnFamilyStoreManager.instance.shutdownPostFlushExecutor();

        CommitLog.instance.shutdownBlocking();

        // wait for miscellaneous tasks like sstable and commitlog segment deletion
        StorageServiceExecutors.instance.tasks.shutdown();
        if (!StorageServiceExecutors.instance.tasks.awaitTermination(1, TimeUnit.MINUTES))
            logger.warn("Miscellaneous task executor still busy after one minute; proceeding with shutdown");

        setMode(Mode.DRAINED, true);
    }

    // Never ever do this at home. Used by tests.
    IPartitioner setPartitionerUnsafe(IPartitioner newPartitioner)
    {
        IPartitioner oldPartitioner = LocatorConfig.instance.getPartitioner();
        LocatorConfig.instance.setPartitioner(newPartitioner);
        valueFactory = new VersionedValue.VersionedValueFactory(LocatorConfig.instance.getPartitioner());
        return oldPartitioner;
    }

    TokenMetadata setTokenMetadataUnsafe(TokenMetadata tmd)
    {
        TokenMetadata old = LocatorConfig.instance.getTokenMetadata();
        LocatorConfig.instance.setTokenMetadataUnsafe(tmd);
        return old;
    }

    public void truncate(String keyspace, String columnFamily) throws TimeoutException, IOException
    {
        try
        {
            StorageProxy.instance.truncateBlocking(keyspace, columnFamily);
        }
        catch (UnavailableException e)
        {
            throw new IOException(e.getMessage());
        }
    }

    public Map<InetAddress, Float> getOwnership()
    {
        List<Token> sortedTokens = LocatorConfig.instance.getTokenMetadata().sortedTokens();
        // describeOwnership returns tokens in an unspecified order, let's re-order them
        Map<Token, Float> tokenMap = new TreeMap<Token, Float>(LocatorConfig.instance.getPartitioner().describeOwnership(sortedTokens));
        Map<InetAddress, Float> nodeMap = new LinkedHashMap<>();
        for (Map.Entry<Token, Float> entry : tokenMap.entrySet())
        {
            InetAddress endpoint = LocatorConfig.instance.getTokenMetadata().getEndpoint(entry.getKey());
            Float tokenOwnership = entry.getValue();
            if (nodeMap.containsKey(endpoint))
                nodeMap.put(endpoint, nodeMap.get(endpoint) + tokenOwnership);
            else
                nodeMap.put(endpoint, tokenOwnership);
        }
        return nodeMap;
    }

    /**
     * Calculates ownership. If there are multiple DC's and the replication strategy is DC aware then ownership will be
     * calculated per dc, i.e. each DC will have total ring ownership divided amongst its nodes. Without replication
     * total ownership will be a multiple of the number of DC's and this value will then go up within each DC depending
     * on the number of replicas within itself. For DC unaware replication strategies, ownership without replication
     * will be 100%.
     *
     * @throws IllegalStateException when node is not configured properly.
     */
    public LinkedHashMap<InetAddress, Float> effectiveOwnership(String keyspace) throws IllegalStateException
    {
        if (Schema.instance.getNonSystemKeyspaces().size() <= 0)
            throw new IllegalStateException("Couldn't find any Non System Keyspaces to infer replication topology");
        if (keyspace == null && !hasSameReplication(Schema.instance.getNonSystemKeyspaces()))
            throw new IllegalStateException("Non System keyspaces doesnt have the same topology");

        TokenMetadata metadata = LocatorConfig.instance.getTokenMetadata().cloneOnlyTokenMap();

        if (keyspace == null)
            keyspace = Schema.instance.getNonSystemKeyspaces().get(0);

        Collection<Collection<InetAddress>> endpointsGroupedByDc = new ArrayList<>();
        // mapping of dc's to nodes, use sorted map so that we get dcs sorted
        SortedMap<String, Collection<InetAddress>> sortedDcsToEndpoints = new TreeMap<>();
        sortedDcsToEndpoints.putAll(metadata.getTopology().getDatacenterEndpoints().asMap());
        for (Collection<InetAddress> endpoints : sortedDcsToEndpoints.values())
            endpointsGroupedByDc.add(endpoints);

        Map<Token, Float> tokenOwnership = LocatorConfig.instance.getPartitioner().describeOwnership(LocatorConfig.instance.getTokenMetadata().sortedTokens());
        LinkedHashMap<InetAddress, Float> finalOwnership = Maps.newLinkedHashMap();

        // calculate ownership per dc
        for (Collection<InetAddress> endpoints : endpointsGroupedByDc)
        {
            // calculate the ownership with replication and add the endpoint to the final ownership map
            for (InetAddress endpoint : endpoints)
            {
                float ownership = 0.0f;
                for (Range<Token> range : LocatorConfig.instance.getRangesForEndpoint(keyspace, endpoint))
                {
                    if (tokenOwnership.containsKey(range.right))
                        ownership += tokenOwnership.get(range.right);
                }
                finalOwnership.put(endpoint, ownership);
            }
        }
        return finalOwnership;
    }


    private boolean hasSameReplication(List<String> list)
    {
        if (list.isEmpty())
            return false;

        for (int i = 0; i < list.size() -1; i++)
        {
            KSMetaData ksm1 = Schema.instance.getKSMetaData(list.get(i));
            KSMetaData ksm2 = Schema.instance.getKSMetaData(list.get(i + 1));
            if (!ksm1.strategyClass.equals(ksm2.strategyClass) ||
                    !Iterators.elementsEqual(ksm1.strategyOptions.entrySet().iterator(),
                                             ksm2.strategyOptions.entrySet().iterator()))
                return false;
        }
        return true;
    }

    public List<String> getKeyspaces()
    {
        List<String> keyspaceNamesList = new ArrayList<>(Schema.instance.getKeyspaces());
        return Collections.unmodifiableList(keyspaceNamesList);
    }

    public void updateSnitch(String epSnitchClassName, Boolean dynamic, Integer dynamicUpdateInterval, Integer dynamicResetInterval, Double dynamicBadnessThreshold) throws ClassNotFoundException
    {
        IEndpointSnitch oldSnitch = LocatorConfig.instance.getEndpointSnitch();

        // new snitch registers mbean during construction
        IEndpointSnitch newSnitch;
        try
        {
            newSnitch = FBUtilities.construct(epSnitchClassName, "snitch");
        }
        catch (ConfigurationException e)
        {
            throw new ClassNotFoundException(e.getMessage());
        }
        if (dynamic)
        {
            DatabaseDescriptor.instance.setDynamicUpdateInterval(dynamicUpdateInterval);
            DatabaseDescriptor.instance.setDynamicResetInterval(dynamicResetInterval);
            DatabaseDescriptor.instance.setDynamicBadnessThreshold(dynamicBadnessThreshold);
            newSnitch = new DynamicEndpointSnitch(newSnitch, LocatorConfig.instance);
        }

        // point snitch references to the new instance
        LocatorConfig.instance.setEndpointSnitch(newSnitch);
        for (String ks : Schema.instance.getKeyspaces())
        {
            KeyspaceManager.instance.open(ks).getReplicationStrategy().snitch = newSnitch;
        }

        if (oldSnitch instanceof DynamicEndpointSnitch)
            ((DynamicEndpointSnitch)oldSnitch).unregisterMBean();
    }

    /**
     * Seed data to the endpoints that will be responsible for it at the future
     *
     * @param rangesToStreamByKeyspace keyspaces and data ranges with endpoints included for each
     * @return async Future for whether stream was success
     */
    private Future<StreamState> streamRanges(Map<String, Multimap<Range<Token>, InetAddress>> rangesToStreamByKeyspace)
    {
        // First, we build a list of ranges to stream to each host, per table
        Map<String, Map<InetAddress, List<Range<Token>>>> sessionsToStreamByKeyspace = new HashMap<>();
        for (Map.Entry<String, Multimap<Range<Token>, InetAddress>> entry : rangesToStreamByKeyspace.entrySet())
        {
            String keyspace = entry.getKey();
            Multimap<Range<Token>, InetAddress> rangesWithEndpoints = entry.getValue();

            if (rangesWithEndpoints.isEmpty())
                continue;

            Map<InetAddress, List<Range<Token>>> rangesPerEndpoint = new HashMap<>();
            for (Map.Entry<Range<Token>, InetAddress> endPointEntry : rangesWithEndpoints.entries())
            {
                Range<Token> range = endPointEntry.getKey();
                InetAddress endpoint = endPointEntry.getValue();

                List<Range<Token>> curRanges = rangesPerEndpoint.get(endpoint);
                if (curRanges == null)
                {
                    curRanges = new LinkedList<>();
                    rangesPerEndpoint.put(endpoint, curRanges);
                }
                curRanges.add(range);
            }

            sessionsToStreamByKeyspace.put(keyspace, rangesPerEndpoint);
        }

        StreamPlan streamPlan = new StreamPlan("Unbootstrap", DatabaseDescriptor.instance, Schema.instance,
                                               KeyspaceManager.instance, StreamManager.instance, DBConfig.instance);
        for (Map.Entry<String, Map<InetAddress, List<Range<Token>>>> entry : sessionsToStreamByKeyspace.entrySet())
        {
            String keyspaceName = entry.getKey();
            Map<InetAddress, List<Range<Token>>> rangesPerEndpoint = entry.getValue();

            for (Map.Entry<InetAddress, List<Range<Token>>> rangesEntry : rangesPerEndpoint.entrySet())
            {
                List<Range<Token>> ranges = rangesEntry.getValue();
                InetAddress newEndpoint = rangesEntry.getKey();

                // TODO each call to transferRanges re-flushes, this is potentially a lot of waste
                streamPlan.transferRanges(newEndpoint, keyspaceName, ranges);
            }
        }
        return streamPlan.execute();
    }

    /**
     * Calculate pair of ranges to stream/fetch for given two range collections
     * (current ranges for keyspace and ranges after move to new token)
     *
     * @param current collection of the ranges by current token
     * @param updated collection of the ranges after token is changed
     * @return pair of ranges to stream/fetch for given current and updated range collections
     */
    public Pair<Set<Range<Token>>, Set<Range<Token>>> calculateStreamAndFetchRanges(Collection<Range<Token>> current, Collection<Range<Token>> updated)
    {
        Set<Range<Token>> toStream = new HashSet<>();
        Set<Range<Token>> toFetch  = new HashSet<>();


        for (Range r1 : current)
        {
            boolean intersect = false;
            for (Range r2 : updated)
            {
                if (r1.intersects(r2))
                {
                    // adding difference ranges to fetch from a ring
                    toStream.addAll(r1.subtract(r2));
                    intersect = true;
                }
            }
            if (!intersect)
            {
                toStream.add(r1); // should seed whole old range
            }
        }

        for (Range r2 : updated)
        {
            boolean intersect = false;
            for (Range r1 : current)
            {
                if (r2.intersects(r1))
                {
                    // adding difference ranges to fetch from a ring
                    toFetch.addAll(r2.subtract(r1));
                    intersect = true;
                }
            }
            if (!intersect)
            {
                toFetch.add(r2); // should fetch whole old range
            }
        }

        return Pair.create(toStream, toFetch);
    }

    public void bulkLoad(String directory)
    {
        try
        {
            bulkLoadInternal(directory).get();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public String bulkLoadAsync(String directory)
    {
        return bulkLoadInternal(directory).planId.toString();
    }

    private StreamResultFuture bulkLoadInternal(String directory)
    {
        File dir = new File(directory);

        if (!dir.exists() || !dir.isDirectory())
            throw new IllegalArgumentException("Invalid directory " + directory);

        SSTableLoader.Client client = new SSTableLoader.Client(DatabaseDescriptor.instance, DBConfig.instance)
        {
            public void init(String keyspace)
            {
                try
                {
                    setPartitioner(LocatorConfig.instance.getPartitioner());
                    for (Map.Entry<Range<Token>, List<InetAddress>> entry : getRangeToAddressMap(keyspace).entrySet())
                    {
                        Range<Token> range = entry.getKey();
                        for (InetAddress endpoint : entry.getValue())
                            addRangeForEndpoint(range, endpoint);
                    }
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }

            public CFMetaData getCFMetaData(String keyspace, String cfName)
            {
                return Schema.instance.getCFMetaData(keyspace, cfName);
            }
        };

        SSTableLoader loader = new SSTableLoader(dir, client, new OutputHandler.LogOutput(), DatabaseDescriptor.instance, SSTableReaderFactory.instance);
        return loader.stream();
    }

    public int getExceptionCount()
    {
        return (int)StorageMetrics.exceptions.count();
    }

    public void rescheduleFailedDeletions()
    {
        SSTableDeletingTask.rescheduleFailedTasks();
    }

    /**
     * #{@inheritDoc}
     */
    public void loadNewSSTables(String ksName, String cfName)
    {
        ColumnFamilyStoreManager.instance.loadNewSSTables(ksName, cfName);
    }

    /**
     * #{@inheritDoc}
     */
    public List<String> sampleKeyRange() // do not rename to getter - see CASSANDRA-4452 for details
    {
        List<DecoratedKey> keys = new ArrayList<>();
        for (Keyspace keyspace : KeyspaceManager.instance.nonSystem())
        {
            for (Range<Token> range : LocatorConfig.instance.getPrimaryRangesForEndpoint(keyspace.getName(), DatabaseDescriptor.instance.getBroadcastAddress()))
                keys.addAll(keySamples(keyspace.getColumnFamilyStores(), range));
        }

        List<String> sampledKeys = new ArrayList<>(keys.size());
        for (DecoratedKey key : keys)
            sampledKeys.add(key.getToken().toString());
        return sampledKeys;
    }

    public void rebuildSecondaryIndex(String ksName, String cfName, String... idxNames)
    {
        ColumnFamilyStoreManager.instance.rebuildSecondaryIndex(ksName, cfName, idxNames);
    }

    public void resetLocalSchema() throws IOException
    {
        MigrationManager.instance.resetLocalSchema();
    }

    public void setTraceProbability(double probability)
    {
        Tracing.instance.setTracingProbability(probability);
    }

    public double getTracingProbability()
    {
        return Tracing.instance.getTracingProbability();
    }

    public void disableAutoCompaction(String ks, String... columnFamilies) throws IOException
    {
        for (ColumnFamilyStore cfs : getValidColumnFamilies(true, true, ks, columnFamilies))
        {
            cfs.disableAutoCompaction();
        }
    }

    public void enableAutoCompaction(String ks, String... columnFamilies) throws IOException
    {
        for (ColumnFamilyStore cfs : getValidColumnFamilies(true, true, ks, columnFamilies))
        {
            cfs.enableAutoCompaction();
        }
    }

    /** Returns the name of the cluster */
    public String getClusterName()
    {
        return DatabaseDescriptor.instance.getClusterName();
    }

    /** Returns the cluster partitioner */
    public String getPartitionerName()
    {
        return LocatorConfig.instance.getPartitionerName();
    }

    public int getTombstoneWarnThreshold()
    {
        return DatabaseDescriptor.instance.getTombstoneWarnThreshold();
    }

    public void setTombstoneWarnThreshold(int threshold)
    {
        DatabaseDescriptor.instance.setTombstoneWarnThreshold(threshold);
    }

    public int getTombstoneFailureThreshold()
    {
        return DatabaseDescriptor.instance.getTombstoneFailureThreshold();
    }

    public void setTombstoneFailureThreshold(int threshold)
    {
        DatabaseDescriptor.instance.setTombstoneFailureThreshold(threshold);
    }

    public void setHintedHandoffThrottleInKB(int throttleInKB)
    {
        DatabaseDescriptor.instance.setHintedHandoffThrottleInKB(throttleInKB);
        logger.info(String.format("Updated hinted_handoff_throttle_in_kb to %d", throttleInKB));
    }
}

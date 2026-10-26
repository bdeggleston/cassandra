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
package org.apache.cassandra.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.repair.SymmetricRemoteSyncTask;
import org.apache.cassandra.repair.SyncTask;
import org.apache.cassandra.repair.SyncTasks;
import org.apache.cassandra.replication.MutationTrackingService.KeyspaceShards;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.tracked.ReadReconciliations;
import org.apache.cassandra.service.reads.tracked.TrackedRead;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

import static org.apache.cassandra.replication.MutationTrackingService.TestAccess.createTestKeyspaceShards;
import static org.apache.cassandra.replication.MutationTrackingService.TestAccess.setKeyspaceShardsUnsafe;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class MutationTrackingServiceTest
{
    private static final String TEST_KEYSPACE = "test_ks";
    private static final String TEST_TABLE = "test_table";
    private static final String REPLAY_KEYSPACE = "replay_ks";
    private static final InetAddressAndPort LOCAL = InetAddressAndPort.getByNameUnchecked("127.0.0.1");
    private static final InetAddressAndPort REMOTE = InetAddressAndPort.getByNameUnchecked("127.0.0.2");

    @BeforeClass
    public static void setup() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.prepareServer();
        MutationJournal.start();
        MutationTrackingService.start();
        ClusterMetadataTestHelper.register(REMOTE);
        SchemaLoader.createKeyspace(TEST_KEYSPACE, KeyspaceParams.simple(1, ReplicationType.tracked), SchemaLoader.standardCFMD(TEST_KEYSPACE, TEST_TABLE));
        SchemaLoader.createKeyspace(REPLAY_KEYSPACE, KeyspaceParams.simple(1, ReplicationType.tracked), SchemaLoader.standardCFMD(REPLAY_KEYSPACE, TEST_TABLE));
    }

    @Test
    public void testAlignToShardBoundariesSingleTaskWithinSingleShard()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.create();

        // Create a single shard covering a-z
        Set<Range<Token>> shardRanges = new HashSet<>();
        shardRanges.add(range("a", "z"));
        KeyspaceShards shards =
        createTestKeyspaceShards(TEST_KEYSPACE, shardRanges);
        setKeyspaceShardsUnsafe(service, TEST_KEYSPACE, shards);

        // Input task completely within the shard
        List<SyncTask> inputTasks = Collections.singletonList(createSyncTask(range("d", "m")));

        Keyspace keyspace = Keyspace.open(TEST_KEYSPACE);
        SyncTasks result = service.alignToShardBoundaries(keyspace, inputTasks);

        // Should have one shard entry
        AtomicInteger entries = new AtomicInteger(0);
        result.apply((shardedTask) -> entries.incrementAndGet());
        assertEquals(1, entries.get());

        // Get all tasks from the result
        List<SyncTask> allTasks = new ArrayList<>();
        result.apply((shardedTask) -> allTasks.add(shardedTask.task));

        assertEquals(1, allTasks.size());

        SyncTask resultTask = allTasks.get(0);
        assertEquals(1, resultTask.rangesToSync.size());
        assertTrue(resultTask.rangesToSync.contains(range("d", "m")));

        // Should have a transfer ID assigned
        assertNotNull(resultTask.getTransferId());
    }

    @Test
    public void testAlignToShardBoundariesSingleTaskSpanningMultipleShards()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.create();

        // Create two shards
        Set<Range<Token>> shardRanges = new HashSet<>();
        shardRanges.add(range("a", "m"));
        shardRanges.add(range("m", "z"));
        KeyspaceShards shards =
        createTestKeyspaceShards(TEST_KEYSPACE, shardRanges);
        setKeyspaceShardsUnsafe(service, TEST_KEYSPACE, shards);

        // Input task spans both shards
        List<SyncTask> inputTasks = Collections.singletonList(createSyncTask(range("d", "s")));

        Keyspace keyspace = Keyspace.open(TEST_KEYSPACE);
        SyncTasks result = service.alignToShardBoundaries(keyspace, inputTasks);

        // Should be split into two shard entries
        AtomicInteger entries = new AtomicInteger(0);
        result.apply((shardedTask) -> entries.incrementAndGet());
        assertEquals(2, entries.get());

        // Collect all ranges from all tasks
        Set<Range<Token>> allRanges = new HashSet<>();
        result.apply((shardedTask) -> allRanges.addAll(shardedTask.task.rangesToSync));

        // Should contain the two split pieces
        assertEquals(2, allRanges.size());
        assertTrue(allRanges.contains(range("d", "m")));
        assertTrue(allRanges.contains(range("m", "s")));
    }

    @Test
    public void testAlignToShardBoundariesMultipleTasksAcrossMultipleShards()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.create();

        // Create three shards
        Set<Range<Token>> shardRanges = new HashSet<>();
        shardRanges.add(range("a", "h"));
        shardRanges.add(range("h", "p"));
        shardRanges.add(range("p", "z"));
        KeyspaceShards shards =
        createTestKeyspaceShards(TEST_KEYSPACE, shardRanges);
        setKeyspaceShardsUnsafe(service, TEST_KEYSPACE, shards);

        // Multiple tasks, some spanning shards
        List<SyncTask> inputTasks = Arrays.asList(
        createSyncTask(range("b", "e")),  // Within shard 1
        createSyncTask(range("f", "j")),  // Spans shard 1 and 2
        createSyncTask(range("q", "s"))   // Within shard 3
        );

        Keyspace keyspace = Keyspace.open(TEST_KEYSPACE);
        SyncTasks result = service.alignToShardBoundaries(keyspace, inputTasks);

        // Should have 4 entries (one per sync task)
        AtomicInteger entries = new AtomicInteger(0);
        result.apply((shardedTask) -> entries.incrementAndGet());
        assertEquals(4, entries.get());

        // Collect all ranges from all tasks
        Set<Range<Token>> allRanges = new HashSet<>();
        result.apply((shardedTask) -> allRanges.addAll(shardedTask.task.rangesToSync));

        // Should have split ranges: b-e, f-h (shard 1), h-j (shard 2), q-s (shard 3)
        assertTrue(allRanges.contains(range("b", "e")));
        assertTrue(allRanges.contains(range("f", "h")));
        assertTrue(allRanges.contains(range("h", "j")));
        assertTrue(allRanges.contains(range("q", "s")));
    }

    @Test
    public void testAlignToShardBoundariesTaskWithMultipleRangesSpanningShards()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.create();

        // Create two shards
        Set<Range<Token>> shardRanges = new HashSet<>();
        shardRanges.add(range("a", "m"));
        shardRanges.add(range("m", "z"));
        KeyspaceShards shards =
        createTestKeyspaceShards(TEST_KEYSPACE, shardRanges);
        setKeyspaceShardsUnsafe(service, TEST_KEYSPACE, shards);

        // Single task with multiple ranges spanning both shards
        List<Range<Token>> ranges = Arrays.asList(
        range("b", "e"),  // Shard 1
        range("f", "p")   // Spans both shards
        );

        SyncTask task = createSyncTask(ranges, LOCAL, REMOTE);
        List<SyncTask> inputTasks = Collections.singletonList(task);

        Keyspace keyspace = Keyspace.open(TEST_KEYSPACE);
        SyncTasks result = service.alignToShardBoundaries(keyspace, inputTasks);

        // Should be split into two shard entries
        AtomicInteger entries = new AtomicInteger(0);
        result.forEach((shardedTask) -> entries.incrementAndGet());
        assertEquals(2, entries.get());

        // Collect all ranges
        Set<Range<Token>> allRanges = new HashSet<>();
        result.forEach((shardedTask) -> allRanges.addAll(shardedTask.rangesToSync));

        // Should have: b-e, f-m (shard 1), m-p (shard 2)
        assertTrue(allRanges.contains(range("b", "e")));
        assertTrue(allRanges.contains(range("f", "m")));
        assertTrue(allRanges.contains(range("m", "p")));
    }

    @Test
    public void testAlignToShardBoundariesPreservesTaskType()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.create();

        // Create two shards
        Set<Range<Token>> shardRanges = new HashSet<>();
        shardRanges.add(range("a", "m"));
        shardRanges.add(range("m", "z"));
        KeyspaceShards shards =
        createTestKeyspaceShards(TEST_KEYSPACE, shardRanges);
        setKeyspaceShardsUnsafe(service, TEST_KEYSPACE, shards);

        // Task spanning both shards
        List<SyncTask> inputTasks = Collections.singletonList(createSyncTask(range("d", "s")));

        Keyspace keyspace = Keyspace.open(TEST_KEYSPACE);
        SyncTasks result = service.alignToShardBoundaries(keyspace, inputTasks);

        // All resulting tasks should be the same type as the input
        result.apply((shardedTask) -> assertTrue("Task should be SymmetricRemoteSyncTask", shardedTask.task instanceof SymmetricRemoteSyncTask));
    }

    @Test
    public void testOffsetsForAKeyspaceThatNoLongerExistsAreDropped()
    {
        String dropped = "keyspace_this_node_drops";
        SchemaLoader.createKeyspace(dropped, KeyspaceParams.simple(1, ReplicationType.tracked), SchemaLoader.standardCFMD(dropped, TEST_TABLE));

        MutationTrackingService service = MutationTrackingService.TestAccess.create();
        ClusterMetadata created = ClusterMetadata.current();
        MutationTrackingService.TestAccess.onNewClusterMetadata(service, null, created);
        assertNotNull(MutationTrackingService.TestAccess.getKeyspaceShards(service, dropped));
        assertTrue(MutationTrackingService.TestAccess.countLogsFor(service, dropped) > 0);

        SchemaTestUtil.dropKeyspaceIfExist(dropped, true);
        ClusterMetadata afterDrop = ClusterMetadata.current();
        MutationTrackingService.TestAccess.onNewClusterMetadata(service, created, afterDrop);

        // The flush triggered by dropping the tables still needs the logs' shards until the change is committed
        assertTrue(MutationTrackingService.TestAccess.countLogsFor(service, dropped) > 0);
        MutationTrackingService.TestAccess.removeDroppedLogs(service);

        CoordinatorLogId logId = CoordinatorLogId.fromLong(CoordinatorLogId.asLong(1, 1));
        Offsets.Immutable offsets = new Offsets.Immutable(logId, new int[]{ 1, 1 });
        Range<Token> range = range("a", "z");

        service.updateReplicatedOffsets(dropped, 1L, range, new Participants(List.of(1)), Collections.singletonList(offsets), true, REMOTE);

        assertNull(MutationTrackingService.TestAccess.getKeyspaceShards(service, dropped));
        assertEquals(0, MutationTrackingService.TestAccess.countLogsFor(service, dropped));
    }

    /**
     * Replicas treat unknown coordinator logs in received summaries as locally missing.
     */
    @Test
    public void testUnknownShardSummaryTreatedAsLocallyMissing()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.create();

        // Shard 1 covers range a-m replicated by node 1.
        Set<Range<Token>> shardRanges = Collections.singleton(range("a", "m"));
        KeyspaceShards shards = createTestKeyspaceShards(TEST_KEYSPACE, shardRanges);
        setKeyspaceShardsUnsafe(service, TEST_KEYSPACE, shards);

        // Foreign coordinator log belongs to host 2 on a foreign shard that node 1 does not replicate.
        CoordinatorLogId foreignLogId = new CoordinatorLogId(2, 1);
        TableId tableId = Keyspace.open(TEST_KEYSPACE).getColumnFamilyStore(TEST_TABLE).metadata().id;

        MutationSummary.Builder summaryBuilder = new MutationSummary.Builder(tableId);
        summaryBuilder.builderForLog(foreignLogId).unreconciled.add(10, 20);
        MutationSummary fullRemoteSummary = summaryBuilder.build();

        Log2OffsetsMap.Mutable missingMutations = new Log2OffsetsMap.Mutable();
        service.collectLocallyMissingMutations(fullRemoteSummary, missingMutations);
        assertTrue(missingMutations.contains(new ShortMutationId(foreignLogId, 10)));
        assertTrue(missingMutations.contains(new ShortMutationId(foreignLogId, 20)));

        try
        {
            service.validateSummaryForParticipant(fullRemoteSummary, 1);
            Assert.fail("Expected IllegalStateException when validating summary with foreign shard for node 1");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains("is not replicated by node 1"));
        }
    }

    /**
     * Replicas reject pushing missing mutations to non-participating nodes.
     */
    @Test
    public void testNonParticipantCollectRemotelyMissingThrowsException()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.create();

        Set<Range<Token>> shardRanges = Collections.singleton(range("a", "m"));
        KeyspaceShards shards = createTestKeyspaceShards(TEST_KEYSPACE, shardRanges);
        setKeyspaceShardsUnsafe(service, TEST_KEYSPACE, shards);

        // Shard was created with localNodeId 1 as participant. Node 2 is not a participant.
        Shard shard = shards.get(range("a", "m"), Epoch.EMPTY.getEpoch());
        CoordinatorLog log = shard.currentLocalLog();
        Offsets offsets = new Offsets.Mutable(log.logId);

        Node2OffsetsMap into = new Node2OffsetsMap();
        org.agrona.collections.IntArrayList remoteNodes = new org.agrona.collections.IntArrayList();
        remoteNodes.addInt(2); // node 2 is not a participant
        try
        {
            service.collectRemotelyMissingMutations(offsets, remoteNodes, into);
            Assert.fail("Expected IllegalStateException when pushing to non-participating host");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains("is not a participant of shard"));
        }
    }

    /**
     * Summaries that include foreign shards fail validation for non-participating nodes.
     */
    @Test
    public void testSummaryValidationFailsForNonParticipatingShards()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.create();

        AtomicInteger hostLogId = new AtomicInteger(0);
        LongSupplier logId1 = () -> CoordinatorLogId.asLong(1, hostLogId.getAndIncrement());
        LongSupplier logId2 = () -> CoordinatorLogId.asLong(2, hostLogId.getAndIncrement());

        Range<Token> range1 = range("a", "m");
        Shard shard1 = new Shard(1, TEST_KEYSPACE, 1L, range1, new Participants(List.of(1)), logId1, (s, l) -> {});
        shard1.currentLocalLog().reconciledOffsets.add(1);

        Range<Token> range2 = range("m", "z");
        Shard shard2 = new Shard(2, TEST_KEYSPACE, 1L, range2, new Participants(List.of(2)), logId2, (s, l) -> {});
        shard2.currentLocalLog().reconciledOffsets.add(2);

        KeyspaceShards keyspaceShards = createTestKeyspaceShards(TEST_KEYSPACE, List.of(shard1, shard2));
        setKeyspaceShardsUnsafe(service, TEST_KEYSPACE, keyspaceShards);

        TableId tableId = Keyspace.open(TEST_KEYSPACE).getColumnFamilyStore(TEST_TABLE).metadata().id;
        AbstractBounds<PartitionPosition> fullRange = Range.makeRowRange(range("a", "z"));

        MutationSummary summaryCoveringBothShards = service.createSummaryForRange(fullRange, tableId, false);
        assertEquals(2, summaryCoveringBothShards.size());

        // Verify validation fails when sending summary to node 1 with foreign shard 2
        try
        {
            service.validateSummaryForParticipant(summaryCoveringBothShards, 1);
            Assert.fail("Expected IllegalStateException for node 1");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains("is not replicated by node 1"));
        }

        // Verify validation fails when sending summary to node 2 with foreign shard 1
        try
        {
            service.validateSummaryForParticipant(summaryCoveringBothShards, 2);
            Assert.fail("Expected IllegalStateException for node 2");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains("is not replicated by node 2"));
        }

        // Sub-range for shard 1 validates for participant 1, but fails for participant 2
        MutationSummary shard1Summary = service.createSummaryForRange(Range.makeRowRange(range1), tableId, false);
        assertEquals(1, shard1Summary.size());
        assertEquals(shard1.currentLocalLog().logId, shard1Summary.get(0).logId());
        service.validateSummaryForParticipant(shard1Summary, 1);
        try
        {
            service.validateSummaryForParticipant(shard1Summary, 2);
            Assert.fail("Expected IllegalStateException for node 2 on shard 1 summary");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains("is not replicated by node 2"));
        }

        // Sub-range for shard 2 validates for participant 2, but fails for participant 1
        MutationSummary shard2Summary = service.createSummaryForRange(Range.makeRowRange(range2), tableId, false);
        assertEquals(1, shard2Summary.size());
        assertEquals(shard2.currentLocalLog().logId, shard2Summary.get(0).logId());
        service.validateSummaryForParticipant(shard2Summary, 2);
        try
        {
            service.validateSummaryForParticipant(shard2Summary, 1);
            Assert.fail("Expected IllegalStateException for node 1 on shard 2 summary");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains("is not replicated by node 1"));
        }
    }

    @Test
    public void testNextHostLogIdFailsBeforeLoaded()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.createUnloaded();
        try
        {
            MutationTrackingService.TestAccess.nextHostLogId(service);
            Assert.fail("Expected IllegalStateException when allocating log ID before counter is loaded");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains("Host log id counter has not been loaded"));
        }
    }

    @Test
    public void testStartupGuardBumpsRegressedCounter()
    {
        QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (keyspace_name, range_start, range_end, since_epoch, host_id, host_log_id) VALUES (?, ?, ?, ?, ?, ?)",
                                                     SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.COORDINATOR_LOGS),
                                       "test_ks", "a", "z", 1L, 1, 56);
        QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (keyspace_name, range_start, range_end, since_epoch, host_id, host_log_id) VALUES (?, ?, ?, ?, ?, ?)",
                                                     SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.COORDINATOR_LOGS),
                                       "test_ks", "a", "z", 1L, 2000, 100);

        assertEquals(56, MutationTrackingService.loadMaxCoordinatorLogIdFromSystemTable(1));
        assertEquals(100, MutationTrackingService.loadMaxCoordinatorLogIdFromSystemTable(2000));

        // Stored counter is 6 (regressed compared to 56)
        MutationTrackingService.persistHostLogIdToSystemTable(6);
        assertEquals(6, MutationTrackingService.loadHostLogIdFromSystemTable());

        MutationTrackingService service = MutationTrackingService.TestAccess.createUnloaded();
        ClusterMetadata metadata = ClusterMetadata.current();
        MutationTrackingService.TestAccess.initHostLogId(service, metadata);

        // Counter bumped to 56 and persisted
        assertEquals(56, MutationTrackingService.TestAccess.getPrevHostLogId(service));
        assertEquals(56, MutationTrackingService.loadHostLogIdFromSystemTable());

        // When stored is higher, counter does not regress
        MutationTrackingService.persistHostLogIdToSystemTable(70);
        MutationTrackingService service2 = MutationTrackingService.TestAccess.createUnloaded();
        MutationTrackingService.TestAccess.initHostLogId(service2, metadata);
        assertEquals(70, MutationTrackingService.TestAccess.getPrevHostLogId(service2));
        assertEquals(70, MutationTrackingService.loadHostLogIdFromSystemTable());
    }

    @Test
    public void testAlignToShardBoundariesIgnoresSealedShards()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.create();

        int localNodeId = 1;
        AtomicInteger hostLogId = new AtomicInteger(0);
        LongSupplier logId = () -> CoordinatorLogId.asLong(localNodeId, hostLogId.getAndIncrement());
        Participants participants = new Participants(List.of(localNodeId));

        Shard sealedShard = new Shard(localNodeId, TEST_KEYSPACE, 1L, range("a", "z"), participants,
                                      Shard.State.SEALED, Collections.emptyList(), logId, (s, l) -> {});
        Shard activeShard = new Shard(localNodeId, TEST_KEYSPACE, 2L, range("a", "z"), participants,
                                      Shard.State.ACTIVE, Collections.emptyList(), logId, (s, l) -> {});

        List<Shard> shardList = Arrays.asList(sealedShard, activeShard);
        MutationTrackingService.KeyspaceShards shards =
            MutationTrackingService.TestAccess.createTestKeyspaceShards(TEST_KEYSPACE, shardList);
        MutationTrackingService.TestAccess.setKeyspaceShards(service, TEST_KEYSPACE, shards);

        List<SyncTask> inputTasks = Collections.singletonList(createSyncTask(range("d", "m")));
        Keyspace keyspace = Keyspace.open(TEST_KEYSPACE);
        SyncTasks result = service.alignToShardBoundaries(keyspace, inputTasks);

        AtomicInteger entries = new AtomicInteger(0);
        result.apply(shardedTask -> {
            entries.incrementAndGet();
            assertEquals(activeShard.sinceEpoch, shardedTask.sinceEpoch);
            assertEquals(range("d", "m"), shardedTask.range);
            assertNotNull(shardedTask.task.getTransferId());
        });
        assertEquals(1, entries.get());
    }

    @Test
    public void testRegisterRemoteLogPopulatesLog2ShardMap()
    {
        CoordinatorLogId remoteLogId = new CoordinatorLogId(999, 1);
        Range<Token> tokenRange = range("a", "m");
        int localNodeId = ClusterMetadata.current().myNodeId().id();
        Participants participants = new Participants(List.of(localNodeId, 999));
        ShardMetadata metadata = new ShardMetadata(TEST_KEYSPACE, 1L, tokenRange, participants);

        Shard shard = MutationTrackingService.instance().registerRemoteLog(remoteLogId, metadata);
        assertNotNull(shard);
        assertEquals(TEST_KEYSPACE, shard.keyspace);
        assertEquals(tokenRange, shard.range);

        // Verify the log was added to the shard and indexed in log2ShardMap
        Shard resolvedShard = MutationTrackingService.instance().getShard(remoteLogId);
        assertEquals(shard, resolvedShard);
    }

    @Test
    public void testResolveRemoteLogAsyncDeduplication()
    {
        CoordinatorLogId logId = new CoordinatorLogId(888, 1);
        Range<Token> tokenRange = range("a", "m");
        int localNodeId = ClusterMetadata.current().myNodeId().id();
        Participants participants = new Participants(List.of(localNodeId, 888));
        ShardMetadata metadata = new ShardMetadata(TEST_KEYSPACE, 1L, tokenRange, participants);

        EndpointState epState = new EndpointState(new HeartBeatState(1, 1));
        Gossiper.instance.endpointStateMap.put(REMOTE, epState);
        Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.realMarkAlive(REMOTE, epState) );
        try
        {
            // Multiple calls before metadata resolution should return the exact same promise instance
            AsyncPromise<Shard> promise1 = MutationTrackingService.instance().resolveRemoteLogAsync(logId, TEST_KEYSPACE, List.of(REMOTE));
            AsyncPromise<Shard> promise2 = MutationTrackingService.instance().resolveRemoteLogAsync(logId, TEST_KEYSPACE, List.of(REMOTE));

            assertSame(promise1, promise2);

            // Announcing or registering the remote log completes the promise
            Shard shard = MutationTrackingService.instance().registerRemoteLog(logId, metadata);
            assertTrue(promise1.isSuccess());
            assertEquals(shard, promise1.getNow());

            // Subsequent call returns an already completed promise for the resolved shard
            AsyncPromise<Shard> promise3 = MutationTrackingService.instance().resolveRemoteLogAsync(logId, TEST_KEYSPACE, List.of(REMOTE));
            assertTrue(promise3.isSuccess());
            assertEquals(shard, promise3.getNow());
        }
        finally
        {
            Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.removeEndpoint(REMOTE) );
            Gossiper.instance.endpointStateMap.remove(REMOTE);
        }
    }

    @Test
    public void testEnsureLogKnownOrResubmit()
    {
        CoordinatorLogId logId = new CoordinatorLogId(777, 1);
        MutationId mutationId = new MutationId(logId.asLong(), MutationId.sequenceId(0, 100));
        TableMetadata tableMetadata = Schema.instance.getTableMetadata(TEST_KEYSPACE, TEST_TABLE);
        Mutation mutation = new RowUpdateBuilder(tableMetadata, 0, "key1")
                            .clustering("name")
                            .add("val", "value")
                            .build()
                            .withMutationId(mutationId);

        Message<Mutation> message = Message.out(Verb.MUTATION_REQ, mutation);

        EndpointState epState = new EndpointState(new HeartBeatState(1, 1));
        Gossiper.instance.endpointStateMap.put(REMOTE, epState);
        Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.realMarkAlive(REMOTE, epState));
        try
        {
            // First call with unknown logId should return false and defer
            boolean immediate = MutationTrackingService.instance().ensureLogKnownOrResubmit(mutation, message);
            assertFalse("Expected deferral for unknown log ID", immediate);

            // Now register the log
            Range<Token> tokenRange = range("a", "m");
            int localNodeId = ClusterMetadata.current().myNodeId().id();
            Participants participants = new Participants(List.of(localNodeId, 777));
            ShardMetadata metadata = new ShardMetadata(TEST_KEYSPACE, 1L, tokenRange, participants);
            MutationTrackingService.instance().registerRemoteLog(logId, metadata);

            // Subsequent call with now-known logId should return true immediately
            boolean knownImmediate = MutationTrackingService.instance().ensureLogKnownOrResubmit(mutation, message);
            assertTrue("Expected immediate true for known log ID", knownImmediate);
        }
        finally
        {
            Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.removeEndpoint(REMOTE));
            Gossiper.instance.endpointStateMap.remove(REMOTE);
        }
    }

    @Test
    public void testNewShardIsPersistedToSystemTables()
    {
        CoordinatorLogId logId = new CoordinatorLogId(889, 1);
        Range<Token> tokenRange = range("m", "z");
        int localNodeId = ClusterMetadata.current().myNodeId().id();
        Participants participants = new Participants(List.of(localNodeId, 889));
        long sinceEpoch = 2L;
        ShardMetadata metadata = new ShardMetadata(TEST_KEYSPACE, sinceEpoch, tokenRange, participants);

        MutationTrackingService.instance().registerRemoteLog(logId, metadata);

        LongSupplier logIdProvider = () -> CoordinatorLogId.asLong(localNodeId, 999);
        ArrayList<Shard> loadedShards = Shard.loadFromSystemTables(localNodeId, logIdProvider, (s, l) -> {});
        boolean found = loadedShards.stream().anyMatch(s -> s.keyspace.equals(TEST_KEYSPACE)
                                                            && s.sinceEpoch == sinceEpoch
                                                            && s.range.equals(tokenRange)
                                                            && s.participants.equals(participants));
        assertTrue(found);
    }

    private static Token tk(String key)
    {
        return new ByteOrderedPartitioner.BytesToken(ByteBufferUtil.bytes(key));
    }

    private static Range<Token> range(String left, String right)
    {
        return new Range<>(tk(left), tk(right));
    }

    private static SyncTask createSyncTask(Range<Token> range)
    {
        return createSyncTask(Collections.singletonList(range), LOCAL, REMOTE);
    }

    private static SyncTask createSyncTask(List<Range<Token>> ranges, InetAddressAndPort local, InetAddressAndPort remote)
    {
        SharedContext ctx = SharedContext.Global.instance;
        TimeUUID sessionId = TimeUUID.Generator.nextTimeUUID();
        RepairJobDesc desc = new RepairJobDesc(sessionId, TimeUUID.Generator.nextTimeUUID(),TEST_KEYSPACE, TEST_TABLE, ranges);
        return new SymmetricRemoteSyncTask(ctx, desc, local, remote, ranges, PreviewKind.NONE, null);
    }

    @Test
    public void testTrackedRangeReadCommandWithExcludingBounds()
    {
        Keyspace keyspace = Keyspace.open(TEST_KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(TEST_TABLE);
        TableMetadata table = cfs.metadata();

        PartitionPosition left = tk("b").maxKeyBound();
        PartitionPosition right = tk("m").minKeyBound();
        ExcludingBounds<PartitionPosition> excludingBounds = new ExcludingBounds<>(left, right);

        DataRange dataRange = new DataRange(excludingBounds, new ClusteringIndexSliceFilter(Slices.ALL, false));
        PartitionRangeReadCommand cmd = PartitionRangeReadCommand.create(table, FBUtilities.nowInSeconds(), ColumnFilter.all(table), RowFilter.none(), DataLimits.NONE, dataRange);

        MutationSummary summary = cmd.createMutationSummary(false);
        assertNotNull(summary);
    }

    @Test
    public void testAugmentingOffsetsFiltersLocallySealedShard()
    {
        int remoteNodeId = ClusterMetadata.current().directory.peerId(REMOTE).id();
        CoordinatorLogId logId = new CoordinatorLogId(remoteNodeId, 555);
        Range<Token> tokenRange = range("a", "m");
        int localNodeId = ClusterMetadata.current().myNodeId().id();
        Participants participants = new Participants(List.of(localNodeId, remoteNodeId));
        ShardMetadata metadata = new ShardMetadata(TEST_KEYSPACE, 1L, tokenRange, participants);

        Shard shard = MutationTrackingService.instance().registerRemoteLog(logId, metadata);
        shard.markSealing();
        shard.markSealed();
        assertTrue(shard.isSealed());

        TableId tableId = TableId.generate();
        Offsets.Immutable.Builder builder = new Offsets.Immutable.Builder(logId);
        builder.add(1);
        builder.add(2);
        builder.add(3);
        Offsets.Immutable remoteReconciled = builder.build();

        MutationSummary.Builder summaryBuilder = new MutationSummary.Builder(tableId);
        summaryBuilder.builderForLog(logId).reconciled.addAll(remoteReconciled);
        MutationSummary remoteSummary = summaryBuilder.build();

        // 1. Shard was sealed BEFORE read started -> omitted from local summary -> excluded from augmenting
        MutationSummary localSummaryOmitted = new MutationSummary.Builder(tableId).build();

        TrackedRead.Id readId = TrackedRead.Id.nextId();
        int[] summaryNodes = new int[]{ remoteNodeId };

        ReadReconciliations.TestAccess.Coordinator coordinator =
            ReadReconciliations.TestAccess.createCoordinator(readId, localNodeId, summaryNodes);

        coordinator.acceptLocalSummary(localSummaryOmitted);
        coordinator.acceptRemoteSummary(remoteSummary, remoteNodeId);

        Log2OffsetsMap.Mutable augmenting = coordinator.augmentingOffsets();
        assertTrue("Augmenting offsets should exclude sealed shard logs omitted from local summary: " + augmenting,
                   augmenting.isEmpty());

        // 2. Shard was in local summary with [1, 2] and sealed with write 3 completing during sealing
        Offsets.Immutable.Builder finalOffsetsBuilder = new Offsets.Immutable.Builder(logId);
        finalOffsetsBuilder.add(1);
        finalOffsetsBuilder.add(2);
        finalOffsetsBuilder.add(3);
        shard.updateReplicatedOffsets(List.of(finalOffsetsBuilder.build()), false, FBUtilities.getBroadcastAddressAndPort());

        MutationSummary.Builder localPresentBuilder = new MutationSummary.Builder(tableId);
        Offsets.Immutable.Builder localOffsets = new Offsets.Immutable.Builder(logId);
        localOffsets.add(1);
        localOffsets.add(2);
        localPresentBuilder.builderForLog(logId).reconciled.addAll(localOffsets.build());
        MutationSummary localSummaryPresent = localPresentBuilder.build();

        TrackedRead.Id readId2 = TrackedRead.Id.nextId();
        ReadReconciliations.TestAccess.Coordinator coordinator2 =
            ReadReconciliations.TestAccess.createCoordinator(readId2, localNodeId, summaryNodes);

        coordinator2.acceptLocalSummary(localSummaryPresent);
        coordinator2.acceptRemoteSummary(remoteSummary, remoteNodeId);

        Log2OffsetsMap.Mutable augmenting2 = coordinator2.augmentingOffsets();
        assertEquals("Augmenting offsets should include write 3 from final sealed data",
                     1, augmenting2.idCount());
        assertTrue("Augmenting offsets should retain missing offset 3",
                   augmenting2.contains(new ShortMutationId(logId, 3)));

        // 3. Shard sealed with NO new writes beyond local summary -> remote offsets excluded
        CoordinatorLogId logIdNoNewWrites = new CoordinatorLogId(remoteNodeId, 557);
        Shard shardNoNewWrites = MutationTrackingService.instance().registerRemoteLog(logIdNoNewWrites, metadata);
        Offsets.Immutable.Builder noNewOffsets = new Offsets.Immutable.Builder(logIdNoNewWrites);
        noNewOffsets.add(1);
        noNewOffsets.add(2);
        shardNoNewWrites.updateReplicatedOffsets(List.of(noNewOffsets.build()), false, FBUtilities.getBroadcastAddressAndPort());
        shardNoNewWrites.markSealing();
        shardNoNewWrites.markSealed();

        MutationSummary.Builder localNoNewBuilder = new MutationSummary.Builder(tableId);
        localNoNewBuilder.builderForLog(logIdNoNewWrites).reconciled.addAll(noNewOffsets.build());
        MutationSummary localSummaryNoNew = localNoNewBuilder.build();

        MutationSummary.Builder remoteSealedBuilder = new MutationSummary.Builder(tableId);
        Offsets.Immutable.Builder remoteNoNewOffsets = new Offsets.Immutable.Builder(logIdNoNewWrites);
        remoteNoNewOffsets.add(1);
        remoteNoNewOffsets.add(2);
        remoteNoNewOffsets.add(3);
        remoteSealedBuilder.builderForLog(logIdNoNewWrites).reconciled.addAll(remoteNoNewOffsets.build());
        remoteSealedBuilder.updateMaxSealedEpoch(tokenRange, metadata.sinceEpoch);
        MutationSummary remoteSummaryWithSealedEpoch = remoteSealedBuilder.build();

        TrackedRead.Id readId3 = TrackedRead.Id.nextId();
        ReadReconciliations.TestAccess.Coordinator coordinator3 =
            ReadReconciliations.TestAccess.createCoordinator(readId3, localNodeId, summaryNodes);

        coordinator3.acceptLocalSummary(localSummaryNoNew);
        coordinator3.acceptRemoteSummary(remoteSummaryWithSealedEpoch, remoteNodeId);

        Log2OffsetsMap.Mutable augmenting3 = coordinator3.augmentingOffsets();
        assertTrue("Augmenting offsets should exclude logs when final sealed data has no new writes: " + augmenting3,
                   augmenting3.isEmpty());

        // 4. Range read straddles two shard ranges: Range 1 has sealed epoch, Range 2 is active with lower sinceEpoch
        Range<Token> tokenRange2 = range("m", "z");
        CoordinatorLogId logIdOtherRange = new CoordinatorLogId(remoteNodeId, 556);
        ShardMetadata metadataOtherRange = new ShardMetadata(TEST_KEYSPACE, 1L, tokenRange2, participants);
        Shard shardOtherRange = MutationTrackingService.instance().registerRemoteLog(logIdOtherRange, metadataOtherRange);

        Offsets.Immutable.Builder otherOffsetsBuilder = new Offsets.Immutable.Builder(logIdOtherRange);
        otherOffsetsBuilder.add(10);
        otherOffsetsBuilder.add(20);
        Offsets.Immutable otherRemoteReconciled = otherOffsetsBuilder.build();

        // Local summary has log from range 2 with offset 10 (missing 20)
        MutationSummary.Builder localStraddleBuilder = new MutationSummary.Builder(tableId);
        Offsets.Immutable.Builder localOtherOffsets = new Offsets.Immutable.Builder(logIdOtherRange);
        localOtherOffsets.add(10);
        localStraddleBuilder.builderForLog(logIdOtherRange).reconciled.addAll(localOtherOffsets.build());
        MutationSummary localStraddleSummary = localStraddleBuilder.build();

        // Remote summary has sealed epoch 100 on Range 1, but reports Range 2 with offsets 10, 20
        MutationSummary.Builder remoteStraddleBuilder = new MutationSummary.Builder(tableId);
        remoteStraddleBuilder.updateMaxSealedEpoch(tokenRange, 100L); // range 1 sealed at epoch 100
        remoteStraddleBuilder.builderForLog(logIdOtherRange).reconciled.addAll(otherRemoteReconciled);
        MutationSummary remoteStraddleSummary = remoteStraddleBuilder.build();

        TrackedRead.Id readId4 = TrackedRead.Id.nextId();
        ReadReconciliations.TestAccess.Coordinator coordinator4 =
            ReadReconciliations.TestAccess.createCoordinator(readId4, localNodeId, summaryNodes);

        coordinator4.acceptLocalSummary(localStraddleSummary);
        coordinator4.acceptRemoteSummary(remoteStraddleSummary, remoteNodeId);

        Log2OffsetsMap.Mutable augmenting4 = coordinator4.augmentingOffsets();
        assertEquals("Range 2 log should NOT be excluded despite Range 1 having a high sealed epoch",
                     1, augmenting4.idCount());
        assertTrue("Augmenting offsets should retain missing offset 20 for Range 2",
                   augmenting4.contains(new ShortMutationId(logIdOtherRange, 20)));
    }

    @Test
    public void testSyncCoordinatorWithDuplicateShardsOnSameRange() throws Exception
    {
        int localNodeId = 1;
        AtomicInteger hostLogId = new AtomicInteger(0);
        LongSupplier logId = () -> CoordinatorLogId.asLong(localNodeId, hostLogId.getAndIncrement());
        Participants participants = new Participants(List.of(localNodeId));

        Range<Token> shardRange = range("a", "z");
        Shard sealedShard = new Shard(localNodeId, TEST_KEYSPACE, 1L, shardRange, participants,
                                      Shard.State.SEALED, Collections.emptyList(), logId, (s, l) -> {});
        Shard activeShard = new Shard(localNodeId, TEST_KEYSPACE, 2L, shardRange, participants,
                                      Shard.State.ACTIVE, Collections.emptyList(), logId, (s, l) -> {});

        List<Shard> shardList = Arrays.asList(sealedShard, activeShard);
        MutationTrackingService.KeyspaceShards shards =
            MutationTrackingService.TestAccess.createTestKeyspaceShards(TEST_KEYSPACE, shardList);
        MutationTrackingService.TestAccess.setKeyspaceShards(MutationTrackingService.instance(), TEST_KEYSPACE, shards);

        RepairJobDesc desc = new RepairJobDesc(TimeUUID.Generator.nextTimeUUID(),
                                               TimeUUID.Generator.nextTimeUUID(),
                                               TEST_KEYSPACE, "", List.of(shardRange));
        MutationTrackingSyncCoordinator coordinator = new MutationTrackingSyncCoordinator(
            SharedContext.Global.instance, desc, null, ClusterMetadata.current());
        coordinator.start();
        coordinator.awaitCompletion();

        Map<Range<Token>, Map<CoordinatorLogId, Offsets.Immutable>> witnessed =
            MutationTrackingService.instance().collectWitnessedOffsetsForRanges(TEST_KEYSPACE, List.of(shardRange), null);
        assertEquals(1, witnessed.size());
        assertTrue(witnessed.containsKey(shardRange));
    }

    @Test
    public void testSyncCoordinatorWithMultipleUnsealedOverlappingShards() throws Exception
    {
        int localNodeId = 1;
        AtomicInteger hostLogId = new AtomicInteger(0);
        LongSupplier logId = () -> CoordinatorLogId.asLong(localNodeId, hostLogId.getAndIncrement());
        Participants participants = new Participants(List.of(localNodeId));

        Range<Token> fullRange = range("a", "z");
        Range<Token> subRange1 = range("a", "m");
        Range<Token> subRange2 = range("m", "z");

        // Old sealing shard covering full range (a, z], newer active shards covering (a, m] and (m, z]
        Shard sealingShard = new Shard(localNodeId, TEST_KEYSPACE, 1L, fullRange, participants,
                                       Shard.State.SEALING, Collections.emptyList(), logId, (s, l) -> {});
        Shard activeShard1 = new Shard(localNodeId, TEST_KEYSPACE, 2L, subRange1, participants,
                                       Shard.State.ACTIVE, Collections.emptyList(), logId, (s, l) -> {});
        Shard activeShard2 = new Shard(localNodeId, TEST_KEYSPACE, 2L, subRange2, participants,
                                       Shard.State.ACTIVE, Collections.emptyList(), logId, (s, l) -> {});

        List<Shard> shardList = Arrays.asList(sealingShard, activeShard1, activeShard2);
        MutationTrackingService.KeyspaceShards shards =
            MutationTrackingService.TestAccess.createTestKeyspaceShards(TEST_KEYSPACE, shardList);
        MutationTrackingService.TestAccess.setKeyspaceShards(MutationTrackingService.instance(), TEST_KEYSPACE, shards);

        RepairJobDesc desc = new RepairJobDesc(TimeUUID.Generator.nextTimeUUID(),
                                               TimeUUID.Generator.nextTimeUUID(),
                                               TEST_KEYSPACE, "", List.of(fullRange));
        MutationTrackingSyncCoordinator coordinator = new MutationTrackingSyncCoordinator(
            SharedContext.Global.instance, desc, null, ClusterMetadata.current());
        coordinator.start();
        coordinator.awaitCompletion();

        Map<Range<Token>, Map<CoordinatorLogId, Offsets.Immutable>> witnessed =
            MutationTrackingService.instance().collectWitnessedOffsetsForRanges(TEST_KEYSPACE, List.of(fullRange), null);
        assertEquals(1, witnessed.size());
        assertTrue(witnessed.containsKey(fullRange));
    }

    private static Mutation replayMutation(CoordinatorLogId logId, int offset)
    {
        TableMetadata tableMetadata = Schema.instance.getTableMetadata(REPLAY_KEYSPACE, TEST_TABLE);
        return new RowUpdateBuilder(tableMetadata, 0, "key1")
               .clustering("name")
               .add("val", "value")
               .build()
               .withMutationId(new MutationId(logId.asLong(), MutationId.sequenceId(0, offset)));
    }

    /**
     * Two shard generations over the same range, each with its own primary log. The older one is sealed.
     */
    private static Shard[] installTwoShardGenerations(int hostLogIdBase)
    {
        int localNodeId = ClusterMetadata.current().myNodeId().id();
        AtomicInteger hostLogId = new AtomicInteger(hostLogIdBase);
        LongSupplier logIds = () -> CoordinatorLogId.asLong(localNodeId, hostLogId.getAndIncrement());
        Participants participants = new Participants(List.of(localNodeId));
        Range<Token> range = range("a", "z");

        MutationTrackingService service = MutationTrackingService.instance();
        Shard older = new Shard(localNodeId, REPLAY_KEYSPACE, 8L, range, participants, Shard.State.SEALED, Collections.emptyList(), logIds, (s, l) -> {});
        Shard newer = new Shard(localNodeId, REPLAY_KEYSPACE, 16L, range, participants, Shard.State.ACTIVE, Collections.emptyList(), logIds, (s, l) -> {});
        MutationTrackingService.TestAccess.setKeyspaceShards(service, REPLAY_KEYSPACE, createTestKeyspaceShards(REPLAY_KEYSPACE, Arrays.asList(older, newer)));
        return new Shard[]{ older, newer };
    }

    @Test
    public void testReplayRegistersMutationWithShardOwningTheLog()
    {
        Shard[] shards = installTwoShardGenerations(10_000);
        Shard older = shards[0];
        Shard newer = shards[1];
        CoordinatorLogId olderLogId = older.currentLocalLog().logId;
        CoordinatorLogId newerLogId = newer.currentLocalLog().logId;

        assertTrue(MutationTrackingService.instance().startWritingForReplay(replayMutation(olderLogId, 1)));

        assertNotNull(older.getLogNullable(olderLogId));
        assertNull("Replay copied the older log into the newer shard", newer.getLogNullable(olderLogId));
        assertNull(older.getLogNullable(newerLogId));
        assertSame(older, MutationTrackingService.instance().getShard(olderLogId));
        assertSame(newer, MutationTrackingService.instance().getShard(newerLogId));
    }

    @Test
    public void testReplayRejectsLogOwnedByShardOfAnotherKeyspace()
    {
        Shard older = installTwoShardGenerations(40_000)[0];
        TableMetadata otherKeyspaceTable = Schema.instance.getTableMetadata(TEST_KEYSPACE, TEST_TABLE);
        Mutation mutation = new RowUpdateBuilder(otherKeyspaceTable, 0, "key1")
                            .clustering("name")
                            .add("val", "value")
                            .build()
                            .withMutationId(new MutationId(older.currentLocalLog().logId.asLong(), MutationId.sequenceId(0, 1)));
        try
        {
            MutationTrackingService.instance().startWritingForReplay(mutation);
            Assert.fail("Expected a log owned by a shard of another keyspace to be rejected");
        }
        catch (IllegalStateException expected)
        {
            // expected
        }
    }

    @Test
    public void testReplayOfUnknownLogIsNotRegistered()
    {
        Shard[] shards = installTwoShardGenerations(20_000);
        int localNodeId = ClusterMetadata.current().myNodeId().id();
        CoordinatorLogId unknown = new CoordinatorLogId(localNodeId, 29_999);

        assertFalse(MutationTrackingService.instance().startWritingForReplay(replayMutation(unknown, 1)));

        for (Shard shard : shards)
            assertNull(shard.getLogNullable(unknown));
        assertNull(MutationTrackingService.instance().getShardNullable(unknown));
    }

    @Test
    public void testRegisteringLogOwnedByAnotherShardIsRejected()
    {
        int remoteNodeId = ClusterMetadata.current().directory.peerId(REMOTE).id();
        int localNodeId = ClusterMetadata.current().myNodeId().id();
        Participants participants = new Participants(List.of(localNodeId, remoteNodeId));
        Range<Token> range = range("b", "c");
        CoordinatorLogId logId = new CoordinatorLogId(remoteNodeId, 31_000);

        Shard owner = MutationTrackingService.instance().registerRemoteLog(logId, new ShardMetadata(TEST_KEYSPACE, 30L, range, participants));
        try
        {
            MutationTrackingService.instance().registerRemoteLog(logId, new ShardMetadata(TEST_KEYSPACE, 31L, range, participants));
            Assert.fail("Expected registration with a second shard to be rejected");
        }
        catch (IllegalStateException expected)
        {
            // expected
        }
        assertSame(owner, MutationTrackingService.instance().getShard(logId));
        Shard other = MutationTrackingService.instance().getShardNullable(TEST_KEYSPACE, 31L, range);
        assertTrue(other == null || other.getLogNullable(logId) == null);
    }

    @Test
    public void testOffsetsForUnknownLogRegisterItWithTheSendersShard()
    {
        int remoteNodeId = ClusterMetadata.current().directory.peerId(REMOTE).id();
        int localNodeId = ClusterMetadata.current().myNodeId().id();
        Participants participants = new Participants(List.of(localNodeId, remoteNodeId));
        Range<Token> range = range("c", "d");
        CoordinatorLogId logId = new CoordinatorLogId(remoteNodeId, 32_000);
        assertNull(MutationTrackingService.instance().getShardNullable(logId));

        Offsets.Immutable offsets = new Offsets.Immutable(logId, new int[]{ 1, 3 });
        MutationTrackingService.instance().updateReplicatedOffsets(TEST_KEYSPACE, 40L, range, participants, List.of(offsets), false, REMOTE);

        Shard shard = MutationTrackingService.instance().getShard(logId);
        assertEquals(40L, shard.sinceEpoch);
        assertEquals(range, shard.range);
        assertNotNull(shard.getLogNullable(logId));
    }

    @Test
    public void testOffsetsForLogOwnedByAnotherShardAreDiscarded()
    {
        int remoteNodeId = ClusterMetadata.current().directory.peerId(REMOTE).id();
        int localNodeId = ClusterMetadata.current().myNodeId().id();
        Participants participants = new Participants(List.of(localNodeId, remoteNodeId));
        Range<Token> range = range("d", "e");
        CoordinatorLogId logId = new CoordinatorLogId(remoteNodeId, 33_000);
        Shard owner = MutationTrackingService.instance().registerRemoteLog(logId, new ShardMetadata(TEST_KEYSPACE, 50L, range, participants));

        Offsets.Immutable offsets = new Offsets.Immutable(logId, new int[]{ 1, 3 });
        MutationTrackingService.instance().updateReplicatedOffsets(TEST_KEYSPACE, 51L, range, participants, List.of(offsets), false, REMOTE);

        assertSame(owner, MutationTrackingService.instance().getShard(logId));
        Shard sender = MutationTrackingService.instance().getShardNullable(TEST_KEYSPACE, 51L, range);
        assertNotNull(sender);
        assertNull("Offsets for a log owned by another shard created a copy", sender.getLogNullable(logId));
    }

    @Test
    public void testConcurrentRegistrationOfOneLogCreatesOneLog() throws Exception
    {
        int remoteNodeId = ClusterMetadata.current().directory.peerId(REMOTE).id();
        int localNodeId = ClusterMetadata.current().myNodeId().id();
        Participants participants = new Participants(List.of(localNodeId, remoteNodeId));
        Range<Token> range = range("e", "f");
        // create the shard first, so the threads race only on registering the log
        MutationTrackingService.instance().registerRemoteLog(new CoordinatorLogId(remoteNodeId, 49), new ShardMetadata(TEST_KEYSPACE, 60L, range, participants));
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try
        {
            for (int i = 0; i < 20; i++)
            {
                CoordinatorLogId logId = new CoordinatorLogId(remoteNodeId, 50 + i);
                ShardMetadata metadata = new ShardMetadata(TEST_KEYSPACE, 60L, range, participants);
                CyclicBarrier barrier = new CyclicBarrier(4);
                List<Future<Shard>> futures = new ArrayList<>();
                for (int t = 0; t < 4; t++)
                {
                    futures.add(executor.submit(() -> {
                        barrier.await();
                        return MutationTrackingService.instance().registerRemoteLog(logId, metadata);
                    }));
                }
                Shard shard = futures.get(0).get(1, TimeUnit.MINUTES);
                for (Future<Shard> future : futures)
                    assertSame(shard, future.get(1, TimeUnit.MINUTES));
                assertSame(shard.getLogNullable(logId), shard.registerLog(logId));
            }
        }
        finally
        {
            executor.shutdownNow();
        }
    }

    @Test
    public void testConcurrentRegistrationWithTwoShardsLeavesNoRowForTheLoser() throws Exception
    {
        int remoteNodeId = ClusterMetadata.current().directory.peerId(REMOTE).id();
        int localNodeId = ClusterMetadata.current().myNodeId().id();
        Participants participants = new Participants(List.of(localNodeId, remoteNodeId));
        Range<Token> range = range("f", "g");
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try
        {
            for (int i = 0; i < 20; i++)
            {
                CoordinatorLogId logId = new CoordinatorLogId(remoteNodeId, 80 + i);
                long epochA = 100L + 2 * i;
                long epochB = epochA + 1;
                // create both shards first, so the threads race only on registering the log
                MutationTrackingService.instance().registerRemoteLog(new CoordinatorLogId(remoteNodeId, 1000 + 2 * i), new ShardMetadata(TEST_KEYSPACE, epochA, range, participants));
                MutationTrackingService.instance().registerRemoteLog(new CoordinatorLogId(remoteNodeId, 1001 + 2 * i), new ShardMetadata(TEST_KEYSPACE, epochB, range, participants));
                CyclicBarrier barrier = new CyclicBarrier(2);
                List<Future<Shard>> futures = new ArrayList<>();
                for (long epoch : new long[]{ epochA, epochB })
                {
                    ShardMetadata metadata = new ShardMetadata(TEST_KEYSPACE, epoch, range, participants);
                    futures.add(executor.submit(() -> {
                        barrier.await();
                        try
                        {
                            return MutationTrackingService.instance().registerRemoteLog(logId, metadata);
                        }
                        catch (IllegalStateException e)
                        {
                            return null;
                        }
                    }));
                }
                Shard first = futures.get(0).get(1, TimeUnit.MINUTES);
                Shard second = futures.get(1).get(1, TimeUnit.MINUTES);
                assertTrue("Exactly one registration should win", (first == null) != (second == null));

                Shard winner = MutationTrackingService.instance().getShard(logId);
                for (long epoch : new long[]{ epochA, epochB })
                {
                    long rows = CoordinatorLog.loadFromSystemTable(TEST_KEYSPACE, epoch, range, localNodeId).stream().filter(log -> log.logId.equals(logId)).count();
                    assertEquals("Persisted logs for epoch " + epoch, epoch == winner.sinceEpoch ? 1 : 0, rows);
                }
            }
        }
        finally
        {
            executor.shutdownNow();
        }
    }
}

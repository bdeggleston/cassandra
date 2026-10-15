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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.locator.EndpointsForRange;
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
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tcm.ClusterMetadata;

import org.apache.cassandra.utils.ByteBufferUtil;
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
    private static final InetAddressAndPort LOCAL = InetAddressAndPort.getByNameUnchecked("127.0.0.1");
    private static final InetAddressAndPort REMOTE = InetAddressAndPort.getByNameUnchecked("127.0.0.2");

    @BeforeClass
    public static void setup() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.prepareServer();
        ClusterMetadataTestHelper.register(REMOTE);
        SchemaLoader.createKeyspace(TEST_KEYSPACE, KeyspaceParams.simple(1, ReplicationType.tracked), SchemaLoader.standardCFMD(TEST_KEYSPACE, TEST_TABLE));
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
        QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (keyspace_name, range_start, range_end, host_id, host_log_id) VALUES (?, ?, ?, ?, ?)",
                                                     SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.COORDINATOR_LOGS),
                                       "test_ks", "a", "z", 1, 56);
        QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (keyspace_name, range_start, range_end, host_id, host_log_id) VALUES (?, ?, ?, ?, ?)",
                                                     SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.COORDINATOR_LOGS),
                                       "test_ks", "a", "z", 2, 100);

        assertEquals(56, MutationTrackingService.loadMaxCoordinatorLogIdFromSystemTable(1));
        assertEquals(100, MutationTrackingService.loadMaxCoordinatorLogIdFromSystemTable(2));

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
}

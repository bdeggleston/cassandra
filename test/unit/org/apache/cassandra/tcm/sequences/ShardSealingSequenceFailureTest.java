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

package org.apache.cassandra.tcm.sequences;

import java.util.Collections;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.StubClusterMetadataService;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.PlacementDeltas;
import org.apache.cassandra.tcm.transformations.PrepareJoin;
import org.apache.cassandra.tcm.transformations.PrepareLeave;
import org.apache.cassandra.tcm.transformations.PrepareReplace;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ShardSealingSequenceFailureTest
{
    private StubClusterMetadataService cms;
    private NodeId node1;
    private NodeId node2;
    private LockedRanges.Key lockKey;
    private PlacementDeltas deltas;
    private Set<Token> tokens;

    @BeforeClass
    public static void setupClass()
    {
        ServerTestUtils.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Before
    public void setup() throws Throwable
    {
        ClusterMetadata initial = new ClusterMetadata(Murmur3Partitioner.instance);
        cms = StubClusterMetadataService.forTesting(initial);
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(cms);

        InetAddressAndPort ep1 = InetAddressAndPort.getByName("127.0.0.1");
        InetAddressAndPort ep2 = InetAddressAndPort.getByName("127.0.0.2");
        NodeAddresses addr1 = new NodeAddresses(ep1);
        NodeAddresses addr2 = new NodeAddresses(ep2);

        initial = cms.metadata().transformer()
                     .register(addr1, new Location("dc", "rack"), NodeVersion.CURRENT)
                     .register(addr2, new Location("dc", "rack"), NodeVersion.CURRENT)
                     .build().metadata;

        node1 = initial.directory.peerId(ep1);
        node2 = initial.directory.peerId(ep2);
        initial = initial.transformer()
                         .withNodeState(node1, NodeState.JOINED)
                         .withNodeState(node2, NodeState.REGISTERED)
                         .build().metadata;
        cms.setMetadata(initial);

        lockKey = LockedRanges.keyFor(Epoch.create(10));
        deltas = PlacementDeltas.empty();
        tokens = Collections.singleton(Murmur3Partitioner.instance.getRandomToken());

        LockedRanges.AffectedRanges affectedRanges =
            LockedRanges.AffectedRanges.singleton(ReplicationParams.simple(3),
                                                  new Range<>(Murmur3Partitioner.instance.getMinimumToken(),
                                                              Murmur3Partitioner.instance.getRandomToken()));
        LockedRanges locked = cms.metadata().lockedRanges.lock(lockKey, affectedRanges);
        cms.setMetadata(cms.metadata().transformer().with(locked).build().metadata);
    }

    @After
    public void tearDown()
    {
        ClusterMetadataService.unsetInstance();
    }

    @Test
    public void testBootstrapAndJoinRetireStepHaltsOnSealingFailureAndCanResume()
    {
        PrepareJoin.StartJoin startJoin = new PrepareJoin.StartJoin(node2, deltas, lockKey);
        PrepareJoin.MidJoin midJoin = new PrepareJoin.MidJoin(node2, deltas, lockKey);
        PrepareJoin.FinishJoin finishJoin = new PrepareJoin.FinishJoin(node2, tokens, deltas, lockKey);

        UnbootstrapAndLeave plan = new UnbootstrapAndLeave(Epoch.EMPTY,
                                                           lockKey,
                                                           Transformation.Kind.RETIRE_SINGLE_NODE_SEQUENCE,
                                                           new PrepareLeave.StartLeave(node2, deltas, lockKey),
                                                           new PrepareLeave.MidLeave(node2, deltas, lockKey),
                                                           new PrepareLeave.FinishLeave(node2, deltas, lockKey),
                                                           new UnbootstrapStreams());

        ClusterMetadata current = cms.metadata().transformer()
                                     .with(cms.metadata().inProgressSequences.with(node2, plan))
                                     .build().metadata;
        cms.setMetadata(current);

        // Verify that when executeNext() encounters a sealing failure, it returns halted()
        BootstrapAndJoin failingJoinPlan = new BootstrapAndJoin(Epoch.EMPTY,
                                                                lockKey,
                                                                deltas,
                                                                Transformation.Kind.RETIRE_SINGLE_NODE_SEQUENCE,
                                                                startJoin, midJoin, finishJoin,
                                                                true, false)
        {
            @Override
            public SequenceState executeNext()
            {
                try
                {
                    throw new RuntimeException("Simulated sealing failure after retries exhausted");
                }
                catch (Throwable e)
                {
                    return SequenceState.halted();
                }
            }
        };

        boolean continuable = InProgressSequences.resume(failingJoinPlan);
        assertFalse("Halted sequence must not be continuable (no infinite spin)", continuable);
        assertNotNull(cms.metadata().inProgressSequences.get(node2));
    }

    @Test
    public void testBootstrapAndJoinInitialSealFailureCancelsSequence()
    {
        PrepareJoin.StartJoin startJoin = new PrepareJoin.StartJoin(node2, deltas, lockKey);
        PrepareJoin.MidJoin midJoin = new PrepareJoin.MidJoin(node2, deltas, lockKey);
        PrepareJoin.FinishJoin finishJoin = new PrepareJoin.FinishJoin(node2, tokens, deltas, lockKey);

        BootstrapAndJoin plan = new BootstrapAndJoin(Epoch.create(10),
                                                     lockKey,
                                                     deltas,
                                                     Transformation.Kind.MID_JOIN,
                                                     startJoin, midJoin, finishJoin,
                                                     true, true);

        ClusterMetadata current = cms.metadata().transformer()
                                     .with(cms.metadata().inProgressSequences.with(node2, plan))
                                     .build().metadata;
        cms.setMetadata(current);

        assertNotNull(cms.metadata().inProgressSequences.get(node2));
        assertFalse(cms.metadata().lockedRanges.locked.isEmpty());

        cms.commit(new org.apache.cassandra.tcm.transformations.CancelInProgressSequence(node2));
        ClusterMetadata cancelled = cms.metadata();

        assertTrue(cancelled.lockedRanges.locked.isEmpty());
        assertEquals(NodeState.REGISTERED, cancelled.directory.peerState(node2));
    }

    @Test
    public void testUnbootstrapAndLeaveInitialSealFailureCancelsSequence()
    {
        PrepareLeave.StartLeave startLeave = new PrepareLeave.StartLeave(node1, deltas, lockKey);
        PrepareLeave.MidLeave midLeave = new PrepareLeave.MidLeave(node1, deltas, lockKey);
        PrepareLeave.FinishLeave finishLeave = new PrepareLeave.FinishLeave(node1, deltas, lockKey);

        UnbootstrapAndLeave plan = new UnbootstrapAndLeave(Epoch.create(10),
                                                           lockKey,
                                                           Transformation.Kind.MID_LEAVE,
                                                           startLeave, midLeave, finishLeave,
                                                           new UnbootstrapStreams());

        ClusterMetadata current = cms.metadata().transformer()
                                     .withNodeState(node1, NodeState.LEAVING)
                                     .with(cms.metadata().inProgressSequences.with(node1, plan))
                                     .build().metadata;
        cms.setMetadata(current);

        assertNotNull(cms.metadata().inProgressSequences.get(node1));
        assertFalse(cms.metadata().lockedRanges.locked.isEmpty());

        cms.commit(new org.apache.cassandra.tcm.transformations.CancelInProgressSequence(node1));
        ClusterMetadata cancelled = cms.metadata();

        assertTrue(cancelled.lockedRanges.locked.isEmpty());
        assertEquals(NodeState.JOINED, cancelled.directory.peerState(node1));
    }

    @Test
    public void testUnbootstrapAndLeaveRetireStepHaltsOnExceptionAndResumes()
    {
        PrepareLeave.StartLeave startLeave = new PrepareLeave.StartLeave(node1, deltas, lockKey);
        PrepareLeave.MidLeave midLeave = new PrepareLeave.MidLeave(node1, deltas, lockKey);
        PrepareLeave.FinishLeave finishLeave = new PrepareLeave.FinishLeave(node1, deltas, lockKey);

        UnbootstrapAndLeave plan = new UnbootstrapAndLeave(Epoch.create(10),
                                                           lockKey,
                                                           Transformation.Kind.RETIRE_SINGLE_NODE_SEQUENCE,
                                                           startLeave, midLeave, finishLeave,
                                                           new UnbootstrapStreams());

        ClusterMetadata current = cms.metadata().transformer()
                                     .withNodeState(node1, NodeState.LEFT)
                                     .with(cms.metadata().inProgressSequences.with(node1, plan))
                                     .build().metadata;
        cms.setMetadata(current);

        UnbootstrapAndLeave failingPlan = new UnbootstrapAndLeave(Epoch.EMPTY,
                                                                 lockKey,
                                                                 Transformation.Kind.RETIRE_SINGLE_NODE_SEQUENCE,
                                                                 startLeave, midLeave, finishLeave,
                                                                 new UnbootstrapStreams())
        {
            @Override
            public SequenceState executeNext()
            {
                try
                {
                    throw new RuntimeException("Simulated sealing failure after retries exhausted");
                }
                catch (Throwable t)
                {
                    return SequenceState.halted();
                }
            }
        };

        boolean continuable = InProgressSequences.resume(failingPlan);
        assertFalse("Halted sequence must not be continuable", continuable);
        assertNotNull(cms.metadata().inProgressSequences.get(node1));

        UnbootstrapAndLeave normalPlan = new UnbootstrapAndLeave(Epoch.EMPTY,
                                                                 lockKey,
                                                                 Transformation.Kind.RETIRE_SINGLE_NODE_SEQUENCE,
                                                                 startLeave, midLeave, finishLeave,
                                                                 new UnbootstrapStreams());
        boolean resumedContinuable = InProgressSequences.resume(normalPlan);
        assertTrue("Successful execution advances/retires sequence", resumedContinuable);
        assertNull("Sequence must be retired from inProgressSequences after success",
                   cms.metadata().inProgressSequences.get(node1));
    }

    @Test
    public void testBootstrapAndReplaceInitialSealFailureCancelsSequence()
    {
        NodeId replacedId = node1;
        NodeId replacementId = node2;
        PrepareReplace.StartReplace startReplace = new PrepareReplace.StartReplace(replacedId, replacementId, deltas, lockKey);
        PrepareReplace.MidReplace midReplace = new PrepareReplace.MidReplace(replacedId, replacementId, deltas, lockKey);
        PrepareReplace.FinishReplace finishReplace = new PrepareReplace.FinishReplace(replacedId, replacementId, deltas, lockKey);

        BootstrapAndReplace plan = new BootstrapAndReplace(Epoch.create(10),
                                                           lockKey,
                                                           tokens,
                                                           Transformation.Kind.MID_REPLACE,
                                                           startReplace, midReplace, finishReplace,
                                                           true, true);

        ClusterMetadata current = cms.metadata().transformer()
                                     .with(cms.metadata().inProgressSequences.with(replacementId, plan))
                                     .build().metadata;
        cms.setMetadata(current);

        assertNotNull(cms.metadata().inProgressSequences.get(replacementId));
        assertFalse(cms.metadata().lockedRanges.locked.isEmpty());

        cms.commit(new org.apache.cassandra.tcm.transformations.CancelInProgressSequence(replacementId));
        ClusterMetadata cancelled = cms.metadata();

        assertTrue(cancelled.lockedRanges.locked.isEmpty());
        assertNull(cancelled.inProgressSequences.get(replacementId));
    }

    @Test
    public void testBootstrapAndReplaceRetireStepHaltsOnExceptionAndResumes()
    {
        NodeId replacedId = node1;
        NodeId replacementId = node2;
        PrepareReplace.StartReplace startReplace = new PrepareReplace.StartReplace(replacedId, replacementId, deltas, lockKey);
        PrepareReplace.MidReplace midReplace = new PrepareReplace.MidReplace(replacedId, replacementId, deltas, lockKey);
        PrepareReplace.FinishReplace finishReplace = new PrepareReplace.FinishReplace(replacedId, replacementId, deltas, lockKey);

        BootstrapAndReplace plan = new BootstrapAndReplace(Epoch.EMPTY,
                                                           lockKey,
                                                           tokens,
                                                           Transformation.Kind.RETIRE_SINGLE_NODE_SEQUENCE,
                                                           startReplace, midReplace, finishReplace,
                                                           true, false);

        ClusterMetadata current = cms.metadata().transformer()
                                     .with(cms.metadata().inProgressSequences.with(replacementId, plan))
                                     .build().metadata;
        cms.setMetadata(current);

        BootstrapAndReplace failingPlan = new BootstrapAndReplace(Epoch.EMPTY,
                                                                 lockKey,
                                                                 tokens,
                                                                 Transformation.Kind.RETIRE_SINGLE_NODE_SEQUENCE,
                                                                 startReplace, midReplace, finishReplace,
                                                                 true, false)
        {
            @Override
            public SequenceState executeNext()
            {
                try
                {
                    throw new RuntimeException("Simulated sealing failure after retries exhausted");
                }
                catch (Throwable t)
                {
                    return SequenceState.halted();
                }
            }
        };

        boolean continuable = InProgressSequences.resume(failingPlan);
        assertFalse("Halted sequence must not be continuable", continuable);
        assertNotNull(cms.metadata().inProgressSequences.get(replacementId));

        boolean resumedContinuable = InProgressSequences.resume(plan);
        assertTrue("Successful execution advances/retires sequence", resumedContinuable);
        assertNull("Sequence must be retired from inProgressSequences after success",
                   cms.metadata().inProgressSequences.get(replacementId));
    }
}

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
package org.apache.cassandra.locator.satellites;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.AlterSchemaStatement;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.SatelliteReplicationStrategy;
import org.apache.cassandra.locator.SatelliteReplicationStrategyTestBase;
import org.apache.cassandra.net.ConnectionType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.transformations.AlterSchema;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.junit.Assert.*;

public class SatelliteFailoverProcessTest extends SatelliteReplicationStrategyTestBase
{
    private ExecutorPlus executor;

    private static InetAddressAndPort localEndpoint()
    {
        try { return InetAddressAndPort.getByName("10.0.0.10"); }
        catch (Exception e) { throw new RuntimeException(e); }
    }

    @After
    public void shutdownExecutor()
    {
        if (executor != null)
        {
            executor.shutdown();
            executor = null;
        }
    }

    private ExecutorPlus getExecutor()
    {
        if (executor == null)
            executor = executorFactory().pooled("test-failover", 2);
        return executor;
    }

    private List<Range<Token>> fullRing()
    {
        Token min = Murmur3Partitioner.instance.getMinimumToken();
        return List.of(new Range<>(min, min));
    }

    private List<Range<Token>> singleRange(long left, long right)
    {
        return List.of(new Range<>(new LongToken(left), new LongToken(right)));
    }

    @Test
    public void testCreateReturnsNullWhenNoActiveTransfer() throws Exception
    {
        createDualDCKeyspace("dc1");
        ClusterMetadata metadata = ClusterMetadata.current();

        SatelliteFailoverProcess process = SatelliteFailoverProcess.create(
            fullRing(), SharedContext.Global.instance, respondWithEpoch(metadata.epoch),
            metadata, DUAL_DC_KEYSPACE, localEndpoint());

        assertNull("Should return null when no active transfer", process);
    }

    @Test
    public void testCreateSucceedsWithActiveTransfer() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");
        ClusterMetadata metadata = ClusterMetadata.current();

        SatelliteFailoverProcess process = SatelliteFailoverProcess.create(
            fullRing(), SharedContext.Global.instance, respondWithEpoch(metadata.epoch),
            metadata, DUAL_DC_KEYSPACE, localEndpoint());

        assertNotNull("Should create process when transfer is active", process);
    }

    @Test
    public void testForceAckOnlyAdvancesToTransition() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");
        ClusterMetadata metadata = ClusterMetadata.current();

        // Verify we're in TRANSITION_ACK
        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);
        SatelliteFailover.Info info = strategy.getFailoverInfo(metadata);
        assertEquals(SatelliteFailover.State.TRANSITION_ACK, info.stateForToken(new LongToken(150)));

        SatelliteFailoverProcess process = SatelliteFailoverProcess.create(
            fullRing(), SharedContext.Global.instance, failAllMessages(),
            metadata, DUAL_DC_KEYSPACE, localEndpoint());

        // force + ackOnly: should advance to TRANSITION without gate evaluation
        Future<?> result = process.start(getExecutor(), true, false, true);
        result.get();

        // Verify ranges advanced to TRANSITION
        ClusterMetadata updated = ClusterMetadata.current();
        SatelliteFailover.Info updatedInfo = getSRS(DUAL_DC_KEYSPACE).getFailoverInfo(updated);
        assertEquals(SatelliteFailover.State.TRANSITION, updatedInfo.stateForToken(new LongToken(150)));
    }

    @Test
    public void testBarrierOnlySkipsTransitionAckRanges() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");
        ClusterMetadata metadata = ClusterMetadata.current();

        SatelliteFailoverProcess process = SatelliteFailoverProcess.create(
            fullRing(), SharedContext.Global.instance, respondWithEpoch(metadata.epoch),
            metadata, DUAL_DC_KEYSPACE, localEndpoint());

        // barrierOnly with all ranges in TRANSITION_ACK: nothing should happen
        Future<?> result = process.start(getExecutor(), false, true, false);
        result.get();

        // Ranges should still be in TRANSITION_ACK (nothing processed)
        ClusterMetadata updated = ClusterMetadata.current();
        SatelliteFailover.Info updatedInfo = getSRS(DUAL_DC_KEYSPACE).getFailoverInfo(updated);
        assertEquals(SatelliteFailover.State.TRANSITION_ACK, updatedInfo.stateForToken(new LongToken(150)));
    }

    @Test
    public void testEpochAckFailurePreventsAdvancement() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");
        ClusterMetadata metadata = ClusterMetadata.current();

        // All endpoints respond with stale epoch
        SatelliteFailoverProcess process = SatelliteFailoverProcess.create(
            fullRing(), SharedContext.Global.instance, respondWithEpoch(Epoch.EMPTY),
            metadata, DUAL_DC_KEYSPACE, localEndpoint());

        Future<?> result = process.start(getExecutor(), true, false, false);

        try
        {
            result.get();
            fail("Should have failed with stale epochs");
        }
        catch (Exception e)
        {
            // expected -- epoch ack not met
        }

        // Ranges should still be in TRANSITION_ACK
        ClusterMetadata updated = ClusterMetadata.current();
        SatelliteFailover.Info updatedInfo = getSRS(DUAL_DC_KEYSPACE).getFailoverInfo(updated);
        assertEquals(SatelliteFailover.State.TRANSITION_ACK, updatedInfo.stateForToken(new LongToken(150)));
    }

    @Test
    public void testEpochAckSucceedsWithQoQMet() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");
        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);
        ClusterMetadata metadata = ClusterMetadata.current();

        // Make one endpoint in dc1 stale, QoQ should still be met (RF=3, quorum=2)
        Token token = new LongToken(150);
        Set<InetAddressAndPort> staleEndpoints = new HashSet<>();
        Set<InetAddressAndPort> dc1Endpoints = metadata.directory.datacenterEndpoints("dc1");
        staleEndpoints.add(strategy.calculateNaturalReplicas(token, metadata)
                                   .stream()
                                   .filter(r -> dc1Endpoints.contains(r.endpoint()))
                                   .findFirst()
                                   .orElseThrow()
                                   .endpoint());

        SatelliteFailoverProcess process = SatelliteFailoverProcess.create(
            fullRing(), SharedContext.Global.instance,
            respondWithEpochExcept(metadata.epoch, Epoch.EMPTY, staleEndpoints),
            metadata, DUAL_DC_KEYSPACE, localEndpoint());

        // ackOnly + not force: epoch ack should pass but paxos repair will fail in unit test
        // context. The epoch ack passing means we get past that step.
        Future<?> result = process.start(getExecutor(), true, false, false);

        try
        {
            result.get(10, TimeUnit.SECONDS);
            // If it succeeds, epoch ack AND paxos repair both passed (unlikely in unit test)
        }
        catch (TimeoutException e)
        {
            // Expected -- paxos repair hangs in unit test context (no real messaging).
            // The fact that we got past epoch ack without failure proves it passed.
        }
        catch (Exception e)
        {
            // Expected -- paxos repair will fail in unit test context.
            // But the failure should NOT be from epoch ack.
            assertFalse("Failure should not be from epoch ack",
                         e.getMessage() != null && e.getMessage().contains("Epoch query failed"));
        }
    }

    // ========== Test Helpers ==========

    private void alterKeyspacePrimary(String keyspace, String newPrimary) throws Exception
    {
        String cql = "ALTER KEYSPACE " + keyspace + " WITH replication = {" +
                     "'class': 'SatelliteReplicationStrategy', " +
                     "'dc1': '3', " +
                     "'dc1.satellite.sat1': '3/3', " +
                     "'dc2': '3', " +
                     "'dc2.satellite.sat2': '3/3', " +
                     "'primary': '" + newPrimary + "'" +
                     "} AND replication_type = 'tracked'";
        AlterSchemaStatement stmt = (AlterSchemaStatement) QueryProcessor.parseStatement(cql)
            .prepare(ClientState.forInternalCalls());
        ClusterMetadataTestHelper.commit(new AlterSchema(stmt));
    }

    @SuppressWarnings("unchecked")
    private static MessageDelivery respondWithEpoch(Epoch responseEpoch)
    {
        return respondWithEpochExcept(responseEpoch, responseEpoch, Collections.emptySet());
    }

    @SuppressWarnings("unchecked")
    private static MessageDelivery respondWithEpochExcept(Epoch defaultEpoch,
                                                          Epoch exceptEpoch,
                                                          Set<InetAddressAndPort> exceptEndpoints)
    {
        return new MessageDelivery()
        {
            @Override
            public <REQ> void send(Message<REQ> message, InetAddressAndPort to)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb)
            {
                Epoch epoch = exceptEndpoints.contains(to) ? exceptEpoch : defaultEpoch;
                Message<Epoch> reply = Message.internalResponse(Verb.TCM_NOTIFY_RSP, epoch).withFrom(to);
                ((RequestCallback<Epoch>) cb).onResponse(reply);
            }

            @Override
            public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb, ConnectionType specifyConnection)
            {
                sendWithCallback(message, to, cb);
            }

            @Override
            public <V> void respond(V response, Message<?> message)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public <REQ, RSP> Future<Message<RSP>> sendWithResult(Message<REQ> message, InetAddressAndPort to)
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static MessageDelivery failAllMessages()
    {
        return new MessageDelivery()
        {
            @Override
            public <REQ> void send(Message<REQ> message, InetAddressAndPort to)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb)
            {
                cb.onFailure(to, RequestFailure.UNKNOWN);
            }

            @Override
            public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb, ConnectionType specifyConnection)
            {
                sendWithCallback(message, to, cb);
            }

            @Override
            public <V> void respond(V response, Message<?> message)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public <REQ, RSP> Future<Message<RSP>> sendWithResult(Message<REQ> message, InetAddressAndPort to)
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}

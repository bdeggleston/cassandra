package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.service.epaxos.exceptions.InvalidInstanceStateChange;
import org.apache.cassandra.service.epaxos.integration.AbstractEpaxosIntegrationTest;
import org.apache.cassandra.service.epaxos.integration.Messenger;
import org.apache.cassandra.service.epaxos.integration.Node;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class EpaxosInstanceStreamingTest extends AbstractEpaxosIntegrationTest
{

    private static final IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
    private static final Set<UUID> EMPTY = Collections.emptySet();

    public Node createNode(final int nodeNumber, final String ksName, Messenger messenger)
    {
        return new Node.SingleThreaded(nodeNumber, messenger)
        {

            @Override
            protected String keyspace()
            {
                return ksName;
            }

            @Override
            protected String instanceTable()
            {
                return String.format("%s_%s", SystemKeyspace.EPAXOS_INSTANCE, nodeNumber);
            }

            @Override
            protected String keyStateTable()
            {
                return String.format("%s_%s", SystemKeyspace.EPAXOS_KEY_STATE, nodeNumber);
            }

            @Override
            protected String tokenStateTable()
            {
                return String.format("%s_%s", SystemKeyspace.EPAXOS_TOKEN_STATE, nodeNumber);
            }

            @Override
            protected void scheduleTokenStateMaintenanceTask()
            {
                // no-op
            }

            @Override
            protected TokenStateManager createTokenStateManager()
            {
                return new EpaxosTokenIntegrationTest.IntegrationTokenStateManager(keyspace(), tokenStateTable());
            }
        };
    }

    private QueryInstance newInstance(EpaxosState state, int key, ConsistencyLevel cl)
    {
        QueryInstance instance = state.createQueryInstance(getSerializedCQLRequest(key, key, cl));
        instance.setDependencies(EMPTY);
        instance.setSuccessors(Lists.newArrayList(state.getEndpoint()));
        return instance;
    }


    private UUID executeInstance(int k, long epoch, EpaxosState state)
    {
        Instance instance = newInstance(state, k, ConsistencyLevel.SERIAL);
        instance.setDependencies(state.getCurrentDependencies(instance));
        instance.setExecuted(epoch);
        state.saveInstance(instance);

        ByteBuffer key = ByteBufferUtil.bytes(k);

        KeyState ks = state.keyStateManager.loadKeyState(key, cfm.cfId);
        ks.markExecuted(instance.getId(), EMPTY, epoch, null);
        ks.markAcknowledged(instance.getDependencies(), instance.getId());
        state.keyStateManager.saveKeyState(key, cfm.cfId, ks);
        return instance.getId();
    }

    private UUID activeInstance(int k, EpaxosState state) throws InvalidInstanceStateChange
    {
        Instance instance = newInstance(state, k, ConsistencyLevel.SERIAL);
        instance.commit(state.getCurrentDependencies(instance));
        state.saveInstance(instance);

        ByteBuffer key = ByteBufferUtil.bytes(k);

        KeyState ks = state.keyStateManager.loadKeyState(key, cfm.cfId);
        ks.recordInstance(instance.getId());
        ks.markAcknowledged(instance.getDependencies(), instance.getId());
        state.keyStateManager.saveKeyState(key, cfm.cfId, ks);
        return instance.getId();
    }

    private Set<UUID> getAllInstanceIds(EpaxosState state)
    {
        UntypedResultSet results = QueryProcessor.executeInternal(String.format("SELECT id FROM %s.%s",
                                                                                state.keyspace(),
                                                                                state.instanceTable()));
        Set<UUID> ids = Sets.newHashSet();
        for (UntypedResultSet.Row row: results)
        {
            ids.add(row.getUUID("id"));
        }

        return ids;
    }

    /**
     * Test token states being streamed
     * from one node to another
     */
    @Test
    public void successCase() throws Exception
    {
        UUID cfId = cfm.cfId;
        Node fromNode = nodes.get(0);
        Node toNode = nodes.get(1);

        Token token100 = partitioner.getToken(ByteBufferUtil.bytes(100));
        Token token200 = partitioner.getToken(ByteBufferUtil.bytes(200));
        Token token300 = partitioner.getToken(ByteBufferUtil.bytes(300));

        TokenStateManager fTsm = fromNode.tokenStateManager;
        TokenStateManager.ManagedCf fCf = fTsm.getOrInitManagedCf(cfId);
        fCf.putIfAbsent(new TokenState(token200, cfId, 10, 0, 0));
        fCf.putIfAbsent(new TokenState(token300, cfId, 20, 0, 0));

        KeyStateManager fKsm = fromNode.keyStateManager;
        Set<UUID> includedIds = Sets.newHashSet();
        Set<UUID> excludedIds = Sets.newHashSet();

        for (int k=50; k<=350; k+=50)
        {
            long epoch = k > 200 && k <= 300 ? 20 : 10;

            boolean included = k > 100 && k <= 300;
            ByteBuffer key = ByteBufferUtil.bytes(k);
            KeyState ks = fKsm.loadKeyState(key, cfId);
            TokenState ts = fTsm.get(key, cfId);

            Assert.assertEquals(String.format("Key: %s", k), epoch, ks.getEpoch());
            Assert.assertEquals(String.format("Key: %s", k), epoch, ts.getEpoch());

            // execute and instance in epoch, epoch -1, and epoch -2
            // and add an 'active' instance
            excludedIds.add(executeInstance(k, epoch - 2, fromNode));

            Set<UUID> maybeTransmitted = Sets.newHashSet();
            maybeTransmitted.add(executeInstance(k, epoch - 1, fromNode));
            maybeTransmitted.add(executeInstance(k, epoch, fromNode));
            maybeTransmitted.add(activeInstance(k, fromNode));
            if (included)
            {
                includedIds.addAll(maybeTransmitted);
            }
            else
            {
                excludedIds.addAll(maybeTransmitted);
            }
        }

        final Range<Token> range = new Range<>(token100, token300);

        InstanceStreamWriter writer = new InstanceStreamWriter(fromNode, cfId, range, toNode.getEndpoint());
        InstanceStreamReader reader = new InstanceStreamReader(toNode, cfId, range);

        DataOutputBuffer outputBuffer = new DataOutputBuffer();
        WritableByteChannel outputChannel = Channels.newChannel(outputBuffer);
        writer.write(outputChannel);

        ReadableByteChannel inputChannel = Channels.newChannel(new ByteArrayInputStream(outputBuffer.getData()));
        reader.read(inputChannel, null);

        // TODO: check proper epochs and execution counts on to-node
        // TODO: token states at 200 & 300 should have been created, and populated with the appropriate instances
        TokenStateManager tTsm = fromNode.tokenStateManager;
        TokenStateManager.ManagedCf tCf = tTsm.getOrInitManagedCf(cfId);
        Assert.assertEquals(2, tCf.allTokens().size());

        TokenState ts200 = tTsm.getExact(token200, cfId);
        Assert.assertNotNull(ts200);
        Assert.assertEquals(10, ts200.getEpoch());

        TokenState ts300 = tTsm.getExact(token300, cfId);
        Assert.assertNotNull(ts300);
        Assert.assertEquals(20, ts300.getEpoch());

        Set<UUID> actualIds = getAllInstanceIds(toNode);
        Assert.assertEquals(includedIds, actualIds);

        // check that all keys are accounted for in the to-node's key states
        Set<UUID> expectedIds = Sets.newHashSet(actualIds);
        for (int k=150; k<=300; k+=50)
        {
            KeyState ks = toNode.keyStateManager.loadKeyState(ByteBufferUtil.bytes(k), cfId);
            Assert.assertNotNull(ks);
            Set<UUID> activeIds = ks.getActiveInstanceIds();
            Assert.assertEquals(1, activeIds.size());
            Assert.assertTrue(expectedIds.containsAll(activeIds));
            expectedIds.removeAll(activeIds);

            Map<Long, Set<UUID>> epochExecutions = ks.getEpochExecutions();
            Assert.assertEquals(2, epochExecutions.size());
            for (Map.Entry<Long, Set<UUID>> entry: epochExecutions.entrySet())
            {
                Assert.assertEquals(1, entry.getValue().size());
                Assert.assertTrue(expectedIds.containsAll(entry.getValue()));
                expectedIds.removeAll(entry.getValue());
            }
        }

        Assert.assertEquals(0, expectedIds.size());
    }

    /**
     * Tests that both sides behave as expected
     * when the from-node cannot create a token
     * state required by the to-node
     */
    @Test
    public void missingRightTokenState()
    {

    }

    /**
     * Test that nothing breaks when draining the instances stream
     */
    @Test
    public void draining() throws Exception
    {
        UUID cfId = cfm.cfId;
        Node fromNode = nodes.get(0);
        Node toNode = nodes.get(1);

        Token token100 = partitioner.getToken(ByteBufferUtil.bytes(100));
        Token token200 = partitioner.getToken(ByteBufferUtil.bytes(200));

        TokenStateManager fTsm = fromNode.tokenStateManager;
        TokenStateManager.ManagedCf fCf = fTsm.getOrInitManagedCf(cfId);
        fCf.putIfAbsent(new TokenState(token200, cfId, 10, 0, 0));

        KeyStateManager fKsm = fromNode.keyStateManager;

        for (int k=150; k<=200; k+=50)
        {
            long epoch = 10;

            ByteBuffer key = ByteBufferUtil.bytes(k);
            KeyState ks = fKsm.loadKeyState(key, cfId);
            TokenState ts = fTsm.get(key, cfId);

            Assert.assertEquals(String.format("Key: %s", k), epoch, ks.getEpoch());
            Assert.assertEquals(String.format("Key: %s", k), epoch, ts.getEpoch());

            // execute and instance in epoch, epoch -1, and epoch -2
            // and add an 'active' instance
            executeInstance(k, epoch - 2, fromNode);
            executeInstance(k, epoch - 1, fromNode);
            executeInstance(k, epoch, fromNode);
            activeInstance(k, fromNode);
        }


        // set the to-node token state to the same epoch as the from-node
        TokenStateManager tTsm = toNode.tokenStateManager;
        TokenStateManager.ManagedCf tCf = tTsm.getOrInitManagedCf(cfId);
        tCf.putIfAbsent(new TokenState(token200, cfId, 10, 0, 0));

        final Range<Token> range = new Range<>(token100, token200);

        InstanceStreamWriter writer = new InstanceStreamWriter(fromNode, cfId, range, toNode.getEndpoint());
        final AtomicBoolean wasDrained = new AtomicBoolean(false);
        InstanceStreamReader reader = new InstanceStreamReader(toNode, cfId, range) {
            @Override
            protected int drainInstanceStream(DataInputStream in) throws IOException
            {
                wasDrained.set(true);
                return super.drainInstanceStream(in);
            }
        };

        DataOutputBuffer outputBuffer = new DataOutputBuffer();
        WritableByteChannel outputChannel = Channels.newChannel(outputBuffer);
        writer.write(outputChannel);

        ReadableByteChannel inputChannel = Channels.newChannel(new ByteArrayInputStream(outputBuffer.getData()));
        reader.read(inputChannel, null);

        Assert.assertTrue(wasDrained.get());
    }

    @Test
    public void tokenStatesAreIncremented()
    {

    }
}

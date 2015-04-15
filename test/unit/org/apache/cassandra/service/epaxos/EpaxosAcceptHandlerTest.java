package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

/**
 * Tests the accept verb handler
 */
public class EpaxosAcceptHandlerTest extends AbstractEpaxosTest
{
    static final InetAddress LOCAL;
    static final InetAddress LEADER;

    static
    {
        try
        {
            LOCAL = InetAddress.getByAddress(new byte[]{0, 0, 0, 1});
            LEADER = InetAddress.getByAddress(new byte[]{0, 0, 0, 2});
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    private static final Token TOKEN = DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(1234));

    MessageIn<AcceptRequest> createMessage(Instance instance, Instance... missing)
    {
        return MessageIn.create(LEADER,
                                new AcceptRequest(instance, 0, Lists.newArrayList(missing)),
                                Collections.<String, byte[]>emptyMap(),
                                MessagingService.Verb.EPAXOS_ACCEPT,
                                0);
    }

    @Test
    public void existingSuccessCase() throws Exception
    {
        MockVerbHandlerState state = new MockVerbHandlerState();
        AcceptVerbHandler handler = new AcceptVerbHandler(state);
        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));

        state.instanceMap.put(instance.getId(), instance.copy());

        Assert.assertEquals(0, state.missingRecoreded.size());
        Assert.assertEquals(0, state.savedInstances.size());
        Assert.assertEquals(0, state.replies.size());
        Assert.assertEquals(0, state.acknowledgedRecoreded.size());
        Assert.assertEquals(0, state.getCurrentDeps.size());

        Set<UUID> expectedDeps = Sets.newHashSet(instance.getDependencies());
        expectedDeps.add(UUIDGen.getTimeUUID());

        instance.incrementBallot();
        instance.setDependencies(expectedDeps);

        handler.doVerb(createMessage(instance), 0);

        Assert.assertEquals(0, state.missingRecoreded.size());
        Assert.assertEquals(1, state.savedInstances.size());
        Assert.assertEquals(1, state.replies.size());
        Assert.assertEquals(1, state.acknowledgedRecoreded.size());
        Assert.assertEquals(0, state.getCurrentDeps.size());

        MessageOut<AcceptResponse> response = state.replies.get(0);
        Assert.assertTrue(response.payload.success);

        Instance saved = state.savedInstances.get(instance.getId());
        Assert.assertEquals(Instance.State.ACCEPTED, saved.getState());
        Assert.assertEquals(expectedDeps, saved.getDependencies());
    }

    @Test
    public void newInstanceSuccessCase() throws Exception
    {
        MockVerbHandlerState state = new MockVerbHandlerState();
        AcceptVerbHandler handler = new AcceptVerbHandler(state);
        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));

        Assert.assertEquals(0, state.missingRecoreded.size());
        Assert.assertEquals(0, state.savedInstances.size());
        Assert.assertEquals(0, state.replies.size());
        Assert.assertEquals(0, state.acknowledgedRecoreded.size());
        Assert.assertEquals(0, state.getCurrentDeps.size());

        instance.incrementBallot();
        handler.doVerb(createMessage(instance), 0);

        Assert.assertEquals(1, state.missingRecoreded.size());
        Assert.assertEquals(1, state.savedInstances.size());
        Assert.assertEquals(1, state.replies.size());
        Assert.assertEquals(1, state.acknowledgedRecoreded.size());
        Assert.assertEquals(1, state.getCurrentDeps.size());

        MessageOut<AcceptResponse> response = state.replies.get(0);
        Assert.assertTrue(response.payload.success);
    }

    @Test
    public void ballotFailure() throws Exception
    {
        MockVerbHandlerState state = new MockVerbHandlerState();
        AcceptVerbHandler handler = new AcceptVerbHandler(state);
        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));

        state.instanceMap.put(instance.getId(), instance.copy());

        Assert.assertEquals(0, state.missingRecoreded.size());
        Assert.assertEquals(0, state.savedInstances.size());
        Assert.assertEquals(0, state.replies.size());
        Assert.assertEquals(0, state.acknowledgedRecoreded.size());

        handler.doVerb(createMessage(instance), 0);

        Assert.assertEquals(0, state.missingRecoreded.size());
        Assert.assertEquals(0, state.savedInstances.size());
        Assert.assertEquals(1, state.replies.size());
        Assert.assertEquals(0, state.acknowledgedRecoreded.size());

        MessageOut<AcceptResponse> response = state.replies.get(0);
        Assert.assertFalse(response.payload.success);
        Assert.assertEquals(state.instanceMap.get(instance.getId()).getBallot(), response.payload.ballot);
    }

    @Test
    public void depsAcknowledged() throws Exception
    {
        MockVerbHandlerState state = new MockVerbHandlerState();
        AcceptVerbHandler handler = new AcceptVerbHandler(state);

        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.accept(Sets.newHashSet(UUIDGen.getTimeUUID()));

        Assert.assertFalse(state.acknowledgedRecoreded.contains(instance.getId()));

        handler.doVerb(createMessage(instance.copy()), 0);
        Assert.assertTrue(state.acknowledgedRecoreded.contains(instance.getId()));
    }

    @Test
    public void passiveRecord()
    {
        MockVerbHandlerState state = new MockVerbHandlerState();
        AcceptVerbHandler handler = new AcceptVerbHandler(state);
        Assert.assertTrue(handler.canPassiveRecord());
    }

    @Test
    public void placeholderInstances() throws Exception
    {
        MockVerbHandlerState state = new MockVerbHandlerState();
        AcceptVerbHandler handler = new AcceptVerbHandler(state);
        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));
        instance.makePlacehoder();

        state.instanceMap.put(instance.getId(), instance.copy());

        Assert.assertEquals(0, state.missingRecoreded.size());
        Assert.assertEquals(0, state.savedInstances.size());
        Assert.assertEquals(0, state.replies.size());
        Assert.assertEquals(0, state.acknowledgedRecoreded.size());
        Assert.assertEquals(0, state.getCurrentDeps.size());

        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID());
        expectedDeps.add(UUIDGen.getTimeUUID());

        instance.incrementBallot();
        instance.setDependencies(expectedDeps);

        handler.doVerb(createMessage(instance), 0);

        Assert.assertEquals(0, state.missingRecoreded.size());
        Assert.assertEquals(1, state.savedInstances.size());
        Assert.assertEquals(1, state.replies.size());
        Assert.assertEquals(1, state.acknowledgedRecoreded.size());
        Assert.assertEquals(1, state.getCurrentDeps.size());

        MessageOut<AcceptResponse> response = state.replies.get(0);
        Assert.assertTrue(response.payload.success);

        Instance saved = state.savedInstances.get(instance.getId());
        Assert.assertEquals(Instance.State.ACCEPTED, saved.getState());
        Assert.assertEquals(expectedDeps, saved.getDependencies());
    }
}

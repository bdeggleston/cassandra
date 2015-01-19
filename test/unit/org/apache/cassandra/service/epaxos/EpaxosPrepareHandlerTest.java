package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.UUID;

public class EpaxosPrepareHandlerTest extends AbstractEpaxosTest
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

    MessageIn<PrepareRequest> createMessage(Instance instance)
    {
        return createMessage(instance.getId(), instance.getBallot());
    }

    MessageIn<PrepareRequest> createMessage(UUID id, int ballot)
    {
        return MessageIn.create(LEADER,
                                new PrepareRequest(TOKEN, CFID, 0, id, ballot),
                                Collections.<String, byte[]>emptyMap(),
                                MessagingService.Verb.EPAXOS_ACCEPT,
                                0);
    }
    @Test
    public void successCase() throws Exception
    {
        MockVerbHandlerState state = new MockVerbHandlerState();
        PrepareVerbHandler handler = new PrepareVerbHandler(state);

        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));

        state.instanceMap.put(instance.getId(), instance.copy());

        instance.incrementBallot();
        handler.doVerb(createMessage(instance), 0);

        Assert.assertEquals(1, state.savedInstances.size());
        Assert.assertEquals(1, state.replies.size());

        MessageOut<MessageEnvelope<Instance>> message = state.replies.get(0);
        Assert.assertEquals(instance.getBallot(), message.payload.contents.getBallot());
    }

    @Test
    public void unknownInstance() throws Exception
    {
        MockVerbHandlerState state = new MockVerbHandlerState();
        PrepareVerbHandler handler = new PrepareVerbHandler(state);

        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));

        instance.incrementBallot();
        handler.doVerb(createMessage(instance), 0);

        Assert.assertEquals(0, state.savedInstances.size());
        Assert.assertEquals(1, state.replies.size());

        MessageOut<MessageEnvelope<Instance>> message = state.replies.get(0);
        Assert.assertNull(message.payload.contents);
    }

    @Test
    public void ballotFailure() throws Exception
    {
        MockVerbHandlerState state = new MockVerbHandlerState();
        PrepareVerbHandler handler = new PrepareVerbHandler(state);

        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));

        state.instanceMap.put(instance.getId(), instance.copy());
        state.instanceMap.get(instance.getId()).updateBallot(20);

        handler.doVerb(createMessage(instance), 0);

        // response is the same, except the instance isn't saved
        Assert.assertEquals(0, state.savedInstances.size());
        Assert.assertEquals(1, state.replies.size());

        MessageOut<MessageEnvelope<Instance>> message = state.replies.get(0);
        Assert.assertEquals(20, message.payload.contents.getBallot());
    }

    @Test
    public void placeholderInstancesArentReturned() throws Exception
    {
        MockVerbHandlerState state = new MockVerbHandlerState();
        PrepareVerbHandler handler = new PrepareVerbHandler(state);

        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));

        state.instanceMap.put(instance.getId(), instance.copy());
        state.instanceMap.get(instance.getId()).setPlaceholder(true);

        instance.incrementBallot();
        handler.doVerb(createMessage(instance), 0);

        Assert.assertEquals(0, state.savedInstances.size());
        Assert.assertEquals(1, state.replies.size());

        MessageOut<MessageEnvelope<Instance>> message = state.replies.get(0);
        Assert.assertNull(message.payload.contents);
    }

    @Test
    public void passiveRecord()
    {
        MockVerbHandlerState state = new MockVerbHandlerState();
        PrepareVerbHandler handler = new PrepareVerbHandler(state);
        Assert.assertFalse(handler.canPassiveRecord());
    }
}

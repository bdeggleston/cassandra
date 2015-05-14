package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

public class EpaxosAcceptCallbackTest extends AbstractEpaxosTest
{

    public AcceptCallback getCallback(EpaxosState state, Instance instance, Runnable failureCallback)
    {
        return new AcceptCallback(state, instance, state.getParticipants(instance), failureCallback);
    }

    public MessageIn<AcceptResponse> createResponse(InetAddress from, AcceptResponse response)
    {
        return MessageIn.create(from, response, Collections.<String, byte[]>emptyMap(), null, 0);
    }

    @Test
    public void testSuccessCase() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);
        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.setDependencies(expectedDeps);

        AcceptCallback callback = getCallback(state, instance, null);

        callback.response(createResponse(state.localEndpoints.get(0), new AcceptResponse(TOKEN0, CFID, 0, DEFAULT_SCOPE, true, 0)));
        Assert.assertEquals(0, state.commits.size());
        Assert.assertFalse(callback.isCompleted());

        callback.response(createResponse(state.localEndpoints.get(1), new AcceptResponse(TOKEN0, CFID, 0, DEFAULT_SCOPE, true, 0)));
        Assert.assertEquals(1, state.commits.size());
        Assert.assertEquals(2, callback.getNumResponses());
        Assert.assertTrue(callback.isCompleted());

        MockCallbackState.CommitCall commitCall = state.commits.get(0);
        Assert.assertEquals(instance.getId(), commitCall.id);
        Assert.assertEquals(instance.getDependencies(), commitCall.dependencies);
    }

    @Test
    public void noLocalResponse() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);
        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.setDependencies(expectedDeps);

        AcceptCallback callback = getCallback(state, instance, null);

        callback.response(createResponse(state.localEndpoints.get(1), new AcceptResponse(TOKEN0, CFID, 0, DEFAULT_SCOPE, true, 0)));
        Assert.assertEquals(0, state.commits.size());
        Assert.assertFalse(callback.isCompleted());

        callback.response(createResponse(state.localEndpoints.get(2), new AcceptResponse(TOKEN0, CFID, 0, DEFAULT_SCOPE, true, 0)));
        Assert.assertEquals(0, state.commits.size());
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(2, callback.getNumResponses());
    }

    @Test
    public void ballotFailure() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);
        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.setDependencies(expectedDeps);

        Runnable runnable = new Runnable()
        {
            @Override
            public void run()
            {

            }
        };

        AcceptCallback callback = getCallback(state, instance, runnable);

        Assert.assertEquals(0, callback.getNumResponses());
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, state.ballotUpdates.size());

        callback.response(createResponse(state.localEndpoints.get(1), new AcceptResponse(TOKEN0, CFID, 0, DEFAULT_SCOPE, false, 20)));
        Assert.assertEquals(0, callback.getNumResponses());
        Assert.assertTrue(callback.isCompleted());
        Assert.assertEquals(1, state.ballotUpdates.size());

        MockCallbackState.UpdateBallotCall ballotCall = state.ballotUpdates.get(0);
        Assert.assertEquals(instance.getId(), ballotCall.id);
        Assert.assertEquals(20, ballotCall.ballot);
        Assert.assertEquals(runnable, ballotCall.callback);

    }

    /**
     * Check that messages received more than once aren't counted more than once
     */
    @Test
    public void duplicateMessagesIgnored() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);
        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.setDependencies(expectedDeps);

        AcceptCallback callback = getCallback(state, instance, null);

        callback.response(createResponse(state.localEndpoints.get(0), new AcceptResponse(TOKEN0, CFID, 0, DEFAULT_SCOPE, true, 0)));
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(1, callback.getNumResponses());
        Assert.assertEquals(0, state.commits.size());

        callback.response(createResponse(state.localEndpoints.get(0), new AcceptResponse(TOKEN0, CFID, 0, DEFAULT_SCOPE, true, 0)));
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(1, callback.getNumResponses());
        Assert.assertEquals(0, state.commits.size());
    }

    /**
     * Check that messages coming in after a quorum is reached are ignored
     */
    @Test
    public void additionalMessagesAreIgnored() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);
        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.setDependencies(expectedDeps);

        AcceptCallback callback = getCallback(state, instance, null);

        callback.response(createResponse(state.localEndpoints.get(0), new AcceptResponse(TOKEN0, CFID, 0, DEFAULT_SCOPE, true, 0)));
        Assert.assertEquals(0, state.commits.size());
        Assert.assertFalse(callback.isCompleted());

        callback.response(createResponse(state.localEndpoints.get(1), new AcceptResponse(TOKEN0, CFID, 0, DEFAULT_SCOPE, true, 0)));
        Assert.assertEquals(1, state.commits.size());
        Assert.assertEquals(2, callback.getNumResponses());
        Assert.assertTrue(callback.isCompleted());

        callback.response(createResponse(state.localEndpoints.get(2), new AcceptResponse(TOKEN0, CFID, 0, DEFAULT_SCOPE, true, 0)));
        Assert.assertEquals(1, state.commits.size());
        Assert.assertEquals(2, callback.getNumResponses());
        Assert.assertTrue(callback.isCompleted());
    }

    /**
     * Check that messages from remote endpoints are ignored
     */
    @Test
    public void remoteEndpointsArentCounted() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 3);
        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0, ConsistencyLevel.LOCAL_SERIAL));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.setDependencies(expectedDeps);

        AcceptCallback callback = getCallback(state, instance, null);

        callback.response(createResponse(state.localEndpoints.get(0), new AcceptResponse(TOKEN0, CFID, 0, DEFAULT_SCOPE, true, 0)));
        Assert.assertEquals(0, state.commits.size());
        Assert.assertEquals(1, callback.getNumResponses());

        callback.response(createResponse(state.remoteEndpoints.get(1), new AcceptResponse(TOKEN0, CFID, 0, DEFAULT_SCOPE, true, 0)));
        Assert.assertEquals(0, state.commits.size());
        Assert.assertEquals(1, callback.getNumResponses());
    }

}

package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class EpaxosPreacceptCallbackTest extends AbstractEpaxosTest
{
    public PreacceptCallback getCallback(EpaxosState state, Instance instance, Runnable failureCallback, boolean forceAccept)
    {
        try
        {
            return new PreacceptCallback(state, instance, state.getParticipants(instance), failureCallback, forceAccept);
        }
        catch (UnavailableException e)
        {
            throw new AssertionError(e);
        }
    }

    public MessageIn<PreacceptResponse> createResponse(InetAddress from, PreacceptResponse response)
    {
        return MessageIn.create(from, response, Collections.<String, byte[]>emptyMap(), null, 0);
    }

    @Test
    public void fastPathSuccessCase() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);
        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.setDependencies(expectedDeps);

        PreacceptCallback callback = getCallback(state, instance, null, false);

        // sanity checks
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        callback.countLocal();
        Assert.assertEquals(0, state.commits.size());
        Assert.assertFalse(callback.isCompleted());

        callback.response(createResponse(state.localEndpoints.get(1), PreacceptResponse.success(instance.copy())));
        Assert.assertEquals(1, state.commits.size());
        Assert.assertTrue(callback.isCompleted());

        AcceptDecision decision = callback.getAcceptDecision();
        Assert.assertFalse(decision.acceptNeeded);
        Assert.assertEquals(expectedDeps, decision.acceptDeps);
        Assert.assertEquals(Collections.EMPTY_MAP, decision.missingInstances);
    }

    /**
     * Tests that the accept path is chosen if conflicting responses are received
     */
    @Test
    public void slowPathSuccessCase() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);
        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        UUID dep1 = UUIDGen.getTimeUUID();
        UUID dep2 = UUIDGen.getTimeUUID();
        Set<UUID> expectedDeps = Sets.newHashSet(dep1, dep2);
        instance.setDependencies(Sets.newHashSet(dep1));

        PreacceptCallback callback = getCallback(state, instance, null, false);

        // sanity checks
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        callback.countLocal();
        Assert.assertEquals(0, state.accepts.size());
        Assert.assertEquals(0, state.commits.size());
        Assert.assertFalse(callback.isCompleted());

        // respond with a failure, missing the expected dep, and replying with another
        Instance responseInstance = instance.copy();
        responseInstance.setDependencies(Sets.newHashSet(dep2));
        callback.response(createResponse(state.localEndpoints.get(1), PreacceptResponse.failure(responseInstance)));
        Assert.assertEquals(1, state.accepts.size());
        Assert.assertEquals(0, state.commits.size());
        Assert.assertTrue(callback.isCompleted());

        AcceptDecision decision = callback.getAcceptDecision();
        Assert.assertTrue(decision.acceptNeeded);
        Assert.assertEquals(expectedDeps, decision.acceptDeps);
    }

    @Test
    public void forcedAcceptSuccessCase() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);
        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.setDependencies(expectedDeps);

        PreacceptCallback callback = getCallback(state, instance, null, true);

        // sanity checks
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        callback.countLocal();
        Assert.assertEquals(0, state.commits.size());
        Assert.assertFalse(callback.isCompleted());

        // the instance should be accepted, even if the
        // accept  decision says it't not neccesary
        callback.response(createResponse(state.localEndpoints.get(1), PreacceptResponse.success(instance.copy())));
        Assert.assertEquals(1, state.accepts.size());
        Assert.assertTrue(callback.isCompleted());

        AcceptDecision decision = callback.getAcceptDecision();
        Assert.assertFalse(decision.acceptNeeded);
        Assert.assertEquals(expectedDeps, decision.acceptDeps);
        Assert.assertEquals(Collections.EMPTY_MAP, decision.missingInstances);
    }

    /**
     * Should complete until the local node has registered a response
     */
    @Test
    public void noLocalResponse() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);
        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.setDependencies(expectedDeps);

        PreacceptCallback callback = getCallback(state, instance, null, false);

        callback.response(createResponse(state.localEndpoints.get(1), PreacceptResponse.success(instance.copy())));
        Assert.assertEquals(0, state.accepts.size());
        Assert.assertEquals(0, state.commits.size());
        Assert.assertFalse(callback.isCompleted());

        callback.response(createResponse(state.localEndpoints.get(1), PreacceptResponse.success(instance.copy())));
        Assert.assertEquals(0, state.accepts.size());
        Assert.assertEquals(0, state.commits.size());
        Assert.assertFalse(callback.isCompleted());
    }

    @Test
    public void ballotFailure() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);
        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.setDependencies(expectedDeps);

        PreacceptCallback callback = getCallback(state, instance, null, false);

        callback.response(createResponse(state.localEndpoints.get(1), PreacceptResponse.ballotFailure(5)));
        Assert.assertEquals(0, state.accepts.size());
        Assert.assertEquals(0, state.commits.size());
        Assert.assertEquals(1, state.ballotUpdates.size());
        Assert.assertTrue(callback.isCompleted());
    }

    @Test
    public void duplicateMessagesIgnored() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);
        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.setDependencies(expectedDeps);

        PreacceptCallback callback = getCallback(state, instance, null, false);

        // sanity checks
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        callback.countLocal();
        Assert.assertEquals(1, callback.getNumResponses());
        Assert.assertFalse(callback.isCompleted());

        callback.response(createResponse(state.localEndpoints.get(1), PreacceptResponse.success(instance.copy())));
        Assert.assertEquals(2, callback.getNumResponses());
        Assert.assertTrue(callback.isCompleted());

        // should be ignored
        callback.response(createResponse(state.localEndpoints.get(2), PreacceptResponse.success(instance.copy())));
        Assert.assertEquals(2, callback.getNumResponses());
        Assert.assertTrue(callback.isCompleted());
    }

    @Test
    public void additionalMessagesIgnored() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);
        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.setDependencies(expectedDeps);

        PreacceptCallback callback = getCallback(state, instance, null, false);

        callback.response(createResponse(state.localEndpoints.get(1), PreacceptResponse.success(instance.copy())));
        Assert.assertEquals(1, callback.getNumResponses());
        Assert.assertFalse(callback.isCompleted());

        callback.response(createResponse(state.localEndpoints.get(1), PreacceptResponse.success(instance.copy())));
        Assert.assertEquals(1, callback.getNumResponses());
        Assert.assertFalse(callback.isCompleted());
    }

    /**
     * Tests that addMissingInstance is called when missing
     * instances are received
     */
    @Test
    public void receiveMissingInstances() throws Exception
    {

    }

    /**
     * Tests that the correct missing instances are sent
     * to the correct nodes in the case of an accept phase
     */
    @Test
    public void sendMissingInstance() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);
        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        UUID dep1 = UUIDGen.getTimeUUID();
        UUID dep2 = UUIDGen.getTimeUUID();
        instance.setDependencies(Sets.newHashSet(dep1));

        PreacceptCallback callback = getCallback(state, instance, null, false);

        // sanity checks
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        callback.countLocal();
        Assert.assertFalse(callback.isCompleted());

        // respond with a failure, missing the expected dep, and replying with another
        Instance responseInstance = instance.copy();
        responseInstance.setDependencies(Sets.newHashSet(dep2));
        callback.response(createResponse(state.localEndpoints.get(1), PreacceptResponse.failure(responseInstance)));
        Assert.assertTrue(callback.isCompleted());

        // check that the missing instances not retured by the remote
        // node are marked to be included in the accept request
        AcceptDecision decision = callback.getAcceptDecision();
        Map<InetAddress, Set<UUID>> missingInstances = Maps.newHashMap();
        missingInstances.put(state.localEndpoints.get(1), Sets.newHashSet(dep1));
        Assert.assertEquals(missingInstances, decision.missingInstances);
    }
}

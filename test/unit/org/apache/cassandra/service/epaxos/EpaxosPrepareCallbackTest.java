package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

public class EpaxosPrepareCallbackTest extends AbstractEpaxosTest
{

    public PrepareCallback getCallback(EpaxosState state, Instance instance)
    {
        try
        {
            return new PrepareCallback(state,
                                       instance,
                                       state.getParticipants(instance),
                                       new PrepareGroup(state,
                                                        UUIDGen.getTimeUUID(),
                                                        Sets.newHashSet(instance.getId())));
        }
        catch (UnavailableException e)
        {
            throw new AssertionError(e);
        }
    }
    public MessageIn<MessageEnvelope<Instance>> createResponse(InetAddress from, Instance instance)
    {
        return createResponse(from, instance, instance.getToken(), instance.getCfId());
    }

    public MessageIn<MessageEnvelope<Instance>> createResponse(InetAddress from, Instance instance, Token token, UUID cfId)
    {
        return MessageIn.create(from, new MessageEnvelope<>(token, cfId, 0, instance), Collections.<String, byte[]>emptyMap(), null, 0);
    }

    @Test
    public void ballotFailure() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);
        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));

        PrepareCallback callback = getCallback(state, instance);

        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        int expectedBallot = 10;
        Instance responseCopy = instance.copy();
        responseCopy.updateBallot(expectedBallot);

        callback.response(createResponse(state.localEndpoints.get(1), responseCopy));

        Assert.assertTrue(callback.isCompleted());

        Assert.assertEquals(1, state.ballotUpdates.size());
        MockCallbackState.UpdateBallotCall ballotCall = state.ballotUpdates.get(0);

        Assert.assertEquals(instance.getId(), ballotCall.id);
        Assert.assertEquals(expectedBallot, ballotCall.ballot);
        Assert.assertNotNull(ballotCall.callback);
        Assert.assertTrue(ballotCall.callback instanceof PrepareTask);
    }

    @Test
    public void tryPreacceptDecision() throws Exception
    {

    }

    @Test
    public void preacceptDecision() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);

        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        UUID dep1 = UUIDGen.getTimeUUID();
        UUID dep2 = UUIDGen.getTimeUUID();
        instance.preaccept(Sets.newHashSet(dep1));
        instance.incrementBallot();

        PrepareCallback callback = getCallback(state, instance);

        Assert.assertEquals(0, state.preacceptPrepares.size());
        Assert.assertEquals(0, state.preaccepts.size());
        Assert.assertEquals(0, state.accepts.size());
        Assert.assertEquals(0, state.commits.size());

        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        // send a preaccepted response from the leader to prevent trypreaccept
        callback.response(createResponse(state.localEndpoints.get(0), instance.copy()));
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(1, callback.getNumResponses());

        // send another preaccepted response
        callback.response(createResponse(state.localEndpoints.get(1), instance.copy()));
        Assert.assertTrue(callback.isCompleted());
        Assert.assertEquals(2, callback.getNumResponses());

        // check that an prepare preaccept call was made
        Assert.assertEquals(1, state.preacceptPrepares.size());
        Assert.assertEquals(0, state.preaccepts.size());
        Assert.assertEquals(0, state.accepts.size());
        Assert.assertEquals(0, state.commits.size());

        // that the prepare decision calls for an preaccept phase
        PrepareDecision decision = callback.getDecision();
        Assert.assertEquals(Instance.State.PREACCEPTED, decision.state);
        Assert.assertNull(decision.deps);
        Assert.assertEquals(instance.getBallot(), decision.ballot);
        Assert.assertEquals(Collections.EMPTY_LIST, decision.tryPreacceptAttempts);
        Assert.assertFalse(decision.commitNoop);

        // check that the call to preacceptPrepare was well formed
        MockCallbackState.PreacceptPrepareCall call = state.preacceptPrepares.get(0);
        Assert.assertEquals(instance.getId(), call.id);
        Assert.assertFalse(call.noop);
        Assert.assertNotNull(call.failureCallback);
    }

    @Test
    public void acceptDecision() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);

        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        UUID dep1 = UUIDGen.getTimeUUID();
        UUID dep2 = UUIDGen.getTimeUUID();
        instance.preaccept(Sets.newHashSet(dep1));
        instance.incrementBallot();

        PrepareCallback callback = getCallback(state, instance);

        Assert.assertEquals(0, state.preacceptPrepares.size());
        Assert.assertEquals(0, state.preaccepts.size());
        Assert.assertEquals(0, state.accepts.size());
        Assert.assertEquals(0, state.commits.size());

        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        // send a preaccepted response
        callback.response(createResponse(state.localEndpoints.get(0), instance.copy()));
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(1, callback.getNumResponses());

        // send an accepted response with different deps
        Set<UUID> expectedDeps = Sets.newHashSet(dep1, dep2);
        instance.accept(expectedDeps);
        callback.response(createResponse(state.localEndpoints.get(1), instance.copy()));
        Assert.assertTrue(callback.isCompleted());
        Assert.assertEquals(2, callback.getNumResponses());

        // check that an accept call was made
        Assert.assertEquals(0, state.preacceptPrepares.size());
        Assert.assertEquals(0, state.preaccepts.size());
        Assert.assertEquals(1, state.accepts.size());
        Assert.assertEquals(0, state.commits.size());

        // that the prepare decision calls for an accept phase
        PrepareDecision decision = callback.getDecision();
        Assert.assertEquals(Instance.State.ACCEPTED, decision.state);
        Assert.assertEquals(expectedDeps, decision.deps);
        Assert.assertEquals(instance.getBallot(), decision.ballot);
        Assert.assertNull(decision.tryPreacceptAttempts);
        Assert.assertFalse(decision.commitNoop);

        // and that the accept call is well formed
        MockCallbackState.AcceptCall acceptCall = state.accepts.get(0);
        Assert.assertEquals(instance.getId(), acceptCall.id);
        Assert.assertEquals(expectedDeps, acceptCall.decision.acceptDeps);
        Assert.assertNotNull(acceptCall.failureCallback);
    }

    @Test
    public void commitDecision() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);

        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        UUID dep1 = UUIDGen.getTimeUUID();
        UUID dep2 = UUIDGen.getTimeUUID();
        instance.accept(Sets.newHashSet(dep1));
        instance.incrementBallot();

        PrepareCallback callback = getCallback(state, instance);

        Assert.assertEquals(0, state.preacceptPrepares.size());
        Assert.assertEquals(0, state.preaccepts.size());
        Assert.assertEquals(0, state.accepts.size());
        Assert.assertEquals(0, state.commits.size());

        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        // send an accepted response
        callback.response(createResponse(state.localEndpoints.get(0), instance.copy()));
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(1, callback.getNumResponses());

        // send a committed response with different deps
        Set<UUID> expectedDeps = Sets.newHashSet(dep1, dep2);
        instance.commit(expectedDeps);
        callback.response(createResponse(state.localEndpoints.get(1), instance.copy()));
        Assert.assertTrue(callback.isCompleted());
        Assert.assertEquals(2, callback.getNumResponses());

        // check that an accept call was made
        Assert.assertEquals(0, state.preacceptPrepares.size());
        Assert.assertEquals(0, state.preaccepts.size());
        Assert.assertEquals(0, state.accepts.size());
        Assert.assertEquals(1, state.commits.size());

        // that the prepare decision calls for a commit phase
        PrepareDecision decision = callback.getDecision();
        Assert.assertEquals(Instance.State.COMMITTED, decision.state);
        Assert.assertEquals(expectedDeps, decision.deps);
        Assert.assertEquals(instance.getBallot(), decision.ballot);
        Assert.assertNull(decision.tryPreacceptAttempts);
        Assert.assertFalse(decision.commitNoop);

        // and that the commit call is well formed
        MockCallbackState.CommitCall commitCall = state.commits.get(0);
        Assert.assertEquals(instance.getId(), commitCall.id);
        Assert.assertEquals(expectedDeps, commitCall.dependencies);
    }

    @Test
    public void preacceptNoopDecision() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);

        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));
        instance.incrementBallot();

        PrepareCallback callback = getCallback(state, instance);

        Assert.assertEquals(0, state.preacceptPrepares.size());
        Assert.assertEquals(0, state.preaccepts.size());
        Assert.assertEquals(0, state.accepts.size());
        Assert.assertEquals(0, state.commits.size());

        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        // send a preaccepted response from the leader to prevent trypreaccept
        callback.response(createResponse(state.localEndpoints.get(1), null, instance.getToken(), instance.getCfId()));
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(1, callback.getNumResponses());

        // send a null responses for the second reply
        callback.response(createResponse(state.localEndpoints.get(2), null, instance.getToken(), instance.getCfId()));
        Assert.assertTrue(callback.isCompleted());
        Assert.assertEquals(2, callback.getNumResponses());

        // check that an prepare preaccept call was made
        Assert.assertEquals(1, state.preacceptPrepares.size());
        Assert.assertEquals(0, state.preaccepts.size());
        Assert.assertEquals(0, state.accepts.size());
        Assert.assertEquals(0, state.commits.size());

        // that the prepare decision calls for an preaccept phase
        PrepareDecision decision = callback.getDecision();
        Assert.assertEquals(Instance.State.PREACCEPTED, decision.state);
        Assert.assertNull(decision.deps);
        Assert.assertEquals(Collections.EMPTY_LIST, decision.tryPreacceptAttempts);
        Assert.assertTrue(decision.commitNoop);

        // check that the call to preacceptPrepare was well formed
        MockCallbackState.PreacceptPrepareCall call = state.preacceptPrepares.get(0);
        Assert.assertEquals(instance.getId(), call.id);
        Assert.assertTrue(call.noop);
        Assert.assertNotNull(call.failureCallback);
    }

    /**
     * Check that messages received more than once aren't counted more than once
     */
    @Test
    public void duplicateMessagesIgnored() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);

        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));
        instance.incrementBallot();

        PrepareCallback callback = getCallback(state, instance);

        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        // send a preaccepted response
        callback.response(createResponse(state.localEndpoints.get(0), instance.copy()));
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(1, callback.getNumResponses());

        // send a duplicate response
        callback.response(createResponse(state.localEndpoints.get(0), instance.copy()));
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(1, callback.getNumResponses());
    }

    /**
     * Check that messages coming in after a quorum is reached are ignored
     */
    @Test
    public void additionalMessagesAreIgnored() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);

        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));
        instance.incrementBallot();

        PrepareCallback callback = getCallback(state, instance);

        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(0, callback.getNumResponses());

        // send first messages, should change state
        callback.response(createResponse(state.localEndpoints.get(0), instance.copy()));
        Assert.assertFalse(callback.isCompleted());
        Assert.assertEquals(1, callback.getNumResponses());

        // send second messages, should change state
        callback.response(createResponse(state.localEndpoints.get(1), instance.copy()));
        Assert.assertTrue(callback.isCompleted());
        Assert.assertEquals(2, callback.getNumResponses());

        // send third messages, should be ignored
        callback.response(createResponse(state.localEndpoints.get(2), instance.copy()));
        Assert.assertTrue(callback.isCompleted());
        Assert.assertEquals(2, callback.getNumResponses());
    }
}

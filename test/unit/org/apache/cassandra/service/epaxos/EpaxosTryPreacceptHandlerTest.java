package org.apache.cassandra.service.epaxos;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class EpaxosTryPreacceptHandlerTest extends AbstractEpaxosTest
{

    MessageIn<TryPreacceptRequest> createMessage(Instance instance, Set<UUID> deps) throws UnknownHostException
    {
        return MessageIn.create(InetAddress.getByName("127.0.0.1"),
                                new TryPreacceptRequest(instance.getToken(), instance.getCfId(), 0, instance.getScope(),
                                                        instance.getId(),  deps,  instance.getBallot()),
                                Collections.<String, byte[]>emptyMap(),
                                MessagingService.Verb.EPAXOS_TRYPREACCEPT,
                                0);
    }

    private static Instance incrementedCopy(Instance instance)
    {
        Instance copy = instance.copy();
        copy.incrementBallot();
        return copy;
    }

    @Before
    public void setUp()
    {
        clearKeyStates();
        clearTokenStates();
    }

    @Test
    public void successCase() throws Exception
    {
        final List<MessageOut> replies = new ArrayList<>(1);
        MockCallbackState state = new MockCallbackState(3, 0) {
            @Override
            protected void sendReply(MessageOut message, int id, InetAddress to)
            {
                replies.add(message);
            }
        };

        // initially preaccept with no deps
        Instance missed = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        missed.preaccept(state.getCurrentDependencies(missed).left);
        state.saveInstance(missed);
        Assert.assertEquals(Collections.<UUID>emptySet(), missed.getDependencies());

        //
        Instance previous = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        previous.commit(Collections.<UUID>emptySet());
        state.addMissingInstance(previous);
        Assert.assertEquals(Collections.<UUID>emptySet(), missed.getDependencies());

        Instance after1 = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        after1.commit(state.getCurrentDependencies(after1).left);
        Assert.assertEquals(Sets.newHashSet(missed.getId(), previous.getId()), after1.getDependencies());
        state.saveInstance(after1);

        Instance after2 = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        after2.commit(state.getCurrentDependencies(after2).left);
        Assert.assertEquals(Sets.newHashSet(missed.getId(), previous.getId(), after1.getId()), after2.getDependencies());
        state.saveInstance(after2);

        Set<UUID> proposedDeps = Sets.newHashSet(previous.getId());
        Assert.assertEquals(0, replies.size());
        TryPreacceptVerbHandler handler = new TryPreacceptVerbHandler(state);
        handler.doEpochVerb(createMessage(incrementedCopy(missed), proposedDeps), 0);

        Assert.assertEquals(1, replies.size());

        TryPreacceptResponse response = (TryPreacceptResponse) replies.get(0).payload;
        Assert.assertEquals(TryPreacceptDecision.ACCEPTED, response.decision);
        missed = state.loadInstance(missed.getId());
        Assert.assertEquals(proposedDeps, missed.getDependencies());
        Assert.assertEquals(Instance.State.PREACCEPTED, missed.getState());
    }

    @Test
    public void rejectedCase() throws Exception
    {
        final List<MessageOut> replies = new ArrayList<>(1);
        MockCallbackState state = new MockCallbackState(3, 0) {
            @Override
            protected void sendReply(MessageOut message, int id, InetAddress to)
            {
                replies.add(message);
            }
        };

        // initially preaccept with no deps
        Instance missed = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        missed.preaccept(state.getCurrentDependencies(missed).left);
        state.saveInstance(missed);
        Assert.assertEquals(Collections.<UUID>emptySet(), missed.getDependencies());

        //
        Instance previous = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        previous.commit(Collections.<UUID>emptySet());
        state.addMissingInstance(previous);
        Assert.assertEquals(Collections.<UUID>emptySet(), missed.getDependencies());

        Instance after = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> afterDeps = state.getCurrentDependencies(after).left;
        afterDeps.remove(missed.getId());
        after.commit(afterDeps);
        Assert.assertEquals(Sets.newHashSet(previous.getId()), after.getDependencies());
        state.saveInstance(after);

        Set<UUID> proposedDeps = Sets.newHashSet(previous.getId());
        Assert.assertEquals(0, replies.size());
        TryPreacceptVerbHandler handler = new TryPreacceptVerbHandler(state);
        handler.doEpochVerb(createMessage(incrementedCopy(missed), proposedDeps), 0);

        Assert.assertEquals(1, replies.size());

        TryPreacceptResponse response = (TryPreacceptResponse) replies.get(0).payload;
        Assert.assertEquals(TryPreacceptDecision.REJECTED, response.decision);
        missed = state.loadInstance(missed.getId());
        Assert.assertEquals(Collections.<UUID>emptySet(), missed.getDependencies());
        Assert.assertEquals(Instance.State.PREACCEPTED, missed.getState());
    }

    @Test
    public void contendedCase() throws Exception
    {
        final List<MessageOut> replies = new ArrayList<>(1);
        MockCallbackState state = new MockCallbackState(3, 0) {
            @Override
            protected void sendReply(MessageOut message, int id, InetAddress to)
            {
                replies.add(message);
            }
        };

        // initially preaccept with no deps
        Instance missed = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        missed.preaccept(state.getCurrentDependencies(missed).left);
        state.saveInstance(missed);
        Assert.assertEquals(Collections.<UUID>emptySet(), missed.getDependencies());

        // commit another in it's place, with no deps
        Instance previous = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        previous.commit(Collections.<UUID>emptySet());
        state.addMissingInstance(previous);
        Assert.assertEquals(Collections.<UUID>emptySet(), missed.getDependencies());

        Instance after = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        after.accept(state.getCurrentDependencies(after).left);
        Assert.assertEquals(Sets.newHashSet(missed.getId(), previous.getId()), after.getDependencies());
        state.saveInstance(after);

        Set<UUID> proposedDeps = Sets.newHashSet(previous.getId());
        Assert.assertEquals(0, replies.size());
        TryPreacceptVerbHandler handler = new TryPreacceptVerbHandler(state);
        handler.doEpochVerb(createMessage(incrementedCopy(missed), proposedDeps), 0);

        Assert.assertEquals(1, replies.size());

        TryPreacceptResponse response = (TryPreacceptResponse) replies.get(0).payload;
        Assert.assertEquals(TryPreacceptDecision.CONTENDED, response.decision);
        missed = state.loadInstance(missed.getId());
        Assert.assertEquals(Collections.<UUID>emptySet(), missed.getDependencies());
        Assert.assertEquals(Instance.State.PREACCEPTED, missed.getState());
    }

    @Test
    public void nonPreacceptedInstanceRejected() throws Exception
    {
        final List<MessageOut> replies = new ArrayList<>(1);
        MockCallbackState state = new MockCallbackState(3, 0) {
            @Override
            protected void sendReply(MessageOut message, int id, InetAddress to)
            {
                replies.add(message);
            }
        };

        // initially preaccept with no deps
        Instance missed = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        missed.accept(state.getCurrentDependencies(missed).left);
        state.saveInstance(missed);
        Assert.assertEquals(Collections.<UUID>emptySet(), missed.getDependencies());

        Set<UUID> proposedDeps = Sets.newHashSet();
        Assert.assertEquals(0, replies.size());
        TryPreacceptVerbHandler handler = new TryPreacceptVerbHandler(state);
        handler.doEpochVerb(createMessage(incrementedCopy(missed), proposedDeps), 0);

        Assert.assertEquals(1, replies.size());

        TryPreacceptResponse response = (TryPreacceptResponse) replies.get(0).payload;
        Assert.assertEquals(TryPreacceptDecision.REJECTED, response.decision);
        missed = state.loadInstance(missed.getId());
        Assert.assertEquals(Collections.<UUID>emptySet(), missed.getDependencies());
        Assert.assertEquals(Instance.State.ACCEPTED, missed.getState());
    }

    @Test
    public void ballotFailure() throws Exception
    {
        final List<MessageOut> replies = new ArrayList<>(1);
        MockCallbackState state = new MockCallbackState(3, 0) {
            @Override
            protected void sendReply(MessageOut message, int id, InetAddress to)
            {
                replies.add(message);
            }
        };

        // initially preaccept with no deps
        Instance missed = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        missed.preaccept(state.getCurrentDependencies(missed).left);
        missed.updateBallot(5);
        state.saveInstance(missed);
        Assert.assertEquals(Collections.<UUID>emptySet(), missed.getDependencies());

        Set<UUID> proposedDeps = Sets.newHashSet();
        Assert.assertEquals(0, replies.size());
        TryPreacceptVerbHandler handler = new TryPreacceptVerbHandler(state);
        // don't increment the ballot here
        handler.doEpochVerb(createMessage(missed, proposedDeps), 0);

        Assert.assertEquals(1, replies.size());

        TryPreacceptResponse response = (TryPreacceptResponse) replies.get(0).payload;
        Assert.assertEquals(TryPreacceptDecision.REJECTED, response.decision);
        Assert.assertEquals(5, response.ballotFailure);

        missed = state.loadInstance(missed.getId());
        Assert.assertEquals(Collections.<UUID>emptySet(), missed.getDependencies());
        Assert.assertEquals(Instance.State.PREACCEPTED, missed.getState());
    }

    @Test
    public void epochVeto() throws Exception
    {
        final List<MessageOut> replies = new ArrayList<>(1);
        MockCallbackState state = new MockCallbackState(3, 0) {
            @Override
            protected void sendReply(MessageOut message, int id, InetAddress to)
            {
                replies.add(message);
            }
        };

        // initially preaccept with no deps
        Instance missed = state.createEpochInstance(TOKEN0, CFID, 2, DEFAULT_SCOPE);
        missed.preaccept(state.getCurrentDependencies(missed).left);
        state.saveInstance(missed);
        Assert.assertEquals(Collections.<UUID>emptySet(), missed.getDependencies());

        Assert.assertEquals(0, replies.size());
        TryPreacceptVerbHandler handler = new TryPreacceptVerbHandler(state);
        handler.doEpochVerb(createMessage(incrementedCopy(missed), Collections.<UUID>emptySet()), 0);

        Assert.assertEquals(1, replies.size());

        TryPreacceptResponse response = (TryPreacceptResponse) replies.get(0).payload;
        Assert.assertEquals(TryPreacceptDecision.ACCEPTED, response.decision);
        Assert.assertEquals(true, response.vetoed);
        missed = state.loadInstance(missed.getId());
        Assert.assertEquals(Instance.State.PREACCEPTED, missed.getState());
    }

    @Test
    public void passiveRecord()
    {
        MockVerbHandlerState state = new MockVerbHandlerState();
        TryPreacceptVerbHandler handler = new TryPreacceptVerbHandler(state);
        Assert.assertFalse(handler.canPassiveRecord());
    }


    /**
     * A trypreaccept request shouldn't be sent to a node that doesn't have a copy of the instance since
     * the prepare leader needs to have seen a conflicting set of instance if it's sending one
     */
    @Test(expected=RuntimeException.class)
    public void missingInstanceFailure() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);
        TryPreacceptVerbHandler handler = new TryPreacceptVerbHandler(state);

        Instance missed = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        missed.preaccept(state.getCurrentDependencies(missed).left);
        handler.doEpochVerb(createMessage(missed, Collections.<UUID>emptySet()), 0);
    }

    /**
     * A trypreaccept request shouldn't be sent to a node that has a placeholder copy of the instance since
     * the prepare leader needs to have seen a conflicting set of instance if it's sending one
     */
    @Test(expected=RuntimeException.class)
    public void placeholderInstanceFailure() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);
        TryPreacceptVerbHandler handler = new TryPreacceptVerbHandler(state);

        Instance missed = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        missed.preaccept(state.getCurrentDependencies(missed).left);
        missed.makePlacehoder();
        state.saveInstance(missed);
        handler.doEpochVerb(createMessage(missed, Collections.<UUID>emptySet()), 0);
    }
}

package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.UUID;

public class EpaxosCommitHandlerTest extends AbstractEpaxosTest
{
    static final InetAddress LOCAL;
    static final InetAddress LEADER;
    static final String LEADER_DC = "DC1";

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

    MessageIn<MessageEnvelope<Instance>> createMessage(Instance instance)
    {
        return MessageIn.create(LEADER,
                                new MessageEnvelope<>(instance.getToken(), instance.getCfId(), 0, instance),
                                Collections.<String, byte[]>emptyMap(),
                                MessagingService.Verb.EPAXOS_COMMIT,
                                0);
    }

    @Test
    public void existingSuccessCase() throws Exception
    {
        MockVerbHandlerState state = new MockVerbHandlerState();
        CommitVerbHandler handler = new CommitVerbHandler(state);

        UUID dep1 = UUIDGen.getTimeUUID();
        UUID dep2 = UUIDGen.getTimeUUID();

        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER, LEADER_DC);
        instance.accept(Sets.newHashSet(dep1));

        state.instanceMap.put(instance.getId(), instance.copy());

        instance.commit(Sets.newHashSet(dep1, dep2));
        Assert.assertNotSame(state.instanceMap.get(instance.getId()).getDependencies(), instance.getDependencies());

        Assert.assertEquals(0, state.missingRecoreded.size());
        Assert.assertEquals(0, state.savedInstances.size());
        Assert.assertEquals(0, state.acknowledgedRecoreded.size());
        Assert.assertEquals(0, state.commitNotified.size());
        Assert.assertEquals(0, state.executed.size());
        Assert.assertEquals(0, state.getCurrentDeps.size());

        handler.doVerb(createMessage(instance.copy()), 0);

        Assert.assertEquals(0, state.missingRecoreded.size());
        Assert.assertEquals(1, state.savedInstances.size());
        Assert.assertEquals(1, state.acknowledgedRecoreded.size());
        Assert.assertEquals(1, state.commitNotified.size());
        Assert.assertEquals(1, state.executed.size());
        Assert.assertEquals(0, state.getCurrentDeps.size());

        Instance localInstance = state.savedInstances.get(instance.getId());
        Assert.assertEquals(Instance.State.COMMITTED, localInstance.getState());
        Assert.assertEquals(instance.getDependencies(), localInstance.getDependencies());
    }

    @Test
    public void newInstanceSuccessCase() throws Exception
    {
        MockVerbHandlerState state = new MockVerbHandlerState();
        CommitVerbHandler handler = new CommitVerbHandler(state);
        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER, LEADER_DC);
        instance.accept(Sets.newHashSet(UUIDGen.getTimeUUID()));

        Assert.assertEquals(0, state.missingRecoreded.size());
        Assert.assertEquals(0, state.savedInstances.size());
        Assert.assertEquals(0, state.acknowledgedRecoreded.size());
        Assert.assertEquals(0, state.commitNotified.size());
        Assert.assertEquals(0, state.executed.size());
        Assert.assertEquals(0, state.getCurrentDeps.size());

        handler.doVerb(createMessage(instance.copy()), 0);

        Assert.assertEquals(1, state.missingRecoreded.size());
        Assert.assertEquals(1, state.savedInstances.size());
        Assert.assertEquals(1, state.acknowledgedRecoreded.size());
        Assert.assertEquals(1, state.commitNotified.size());
        Assert.assertEquals(1, state.executed.size());
        Assert.assertEquals(1, state.getCurrentDeps.size());

        Instance localInstance = state.savedInstances.get(instance.getId());
        Assert.assertEquals(Instance.State.COMMITTED, localInstance.getState());
    }

    @Test
    public void depsAcknowledged() throws Exception
    {
        MockVerbHandlerState state = new MockVerbHandlerState();
        CommitVerbHandler handler = new CommitVerbHandler(state);

        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER, LEADER_DC);
        instance.commit(Sets.newHashSet(UUIDGen.getTimeUUID()));

        Assert.assertFalse(state.acknowledgedRecoreded.contains(instance.getId()));

        handler.doVerb(createMessage(instance.copy()), 0);
        Assert.assertTrue(state.acknowledgedRecoreded.contains(instance.getId()));
    }

    @Test
    public void passiveRecord()
    {
        MockVerbHandlerState state = new MockVerbHandlerState();
        CommitVerbHandler handler = new CommitVerbHandler(state);
        Assert.assertTrue(handler.canPassiveRecord());
    }

    /**
     * Placeholder instances should be added to the key manager on commit
     */
    @Test
    public void placeholderInstances() throws Exception
    {
        MockVerbHandlerState state = new MockVerbHandlerState();
        CommitVerbHandler handler = new CommitVerbHandler(state);

        UUID dep1 = UUIDGen.getTimeUUID();
        UUID dep2 = UUIDGen.getTimeUUID();

        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER, LEADER_DC);
        instance.preaccept(Sets.<UUID>newHashSet());
        instance.makePlacehoder();

        state.instanceMap.put(instance.getId(), instance.copy());

        instance.commit(Sets.newHashSet(dep1, dep2));
        Assert.assertNotSame(state.instanceMap.get(instance.getId()).getDependencies(), instance.getDependencies());

        Assert.assertEquals(0, state.missingRecoreded.size());
        Assert.assertEquals(0, state.savedInstances.size());
        Assert.assertEquals(0, state.acknowledgedRecoreded.size());
        Assert.assertEquals(0, state.commitNotified.size());
        Assert.assertEquals(0, state.executed.size());
        Assert.assertEquals(0, state.getCurrentDeps.size());

        handler.doVerb(createMessage(instance.copy()), 0);

        Assert.assertEquals(0, state.missingRecoreded.size());
        Assert.assertEquals(1, state.savedInstances.size());
        Assert.assertEquals(1, state.acknowledgedRecoreded.size());
        Assert.assertEquals(1, state.commitNotified.size());
        Assert.assertEquals(1, state.executed.size());
        Assert.assertEquals(1, state.getCurrentDeps.size());

        Instance localInstance = state.savedInstances.get(instance.getId());
        Assert.assertEquals(Instance.State.COMMITTED, localInstance.getState());
        Assert.assertEquals(instance.getDependencies(), localInstance.getDependencies());
    }
}

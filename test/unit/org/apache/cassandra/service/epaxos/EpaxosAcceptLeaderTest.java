package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.service.epaxos.integration.AbstractEpaxosIntegrationTest;
import org.apache.cassandra.service.epaxos.integration.Messenger;
import org.apache.cassandra.service.epaxos.integration.Node;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.util.*;

/**
 * Tests command leader's handling of the accept phase
 */
public class EpaxosAcceptLeaderTest extends AbstractEpaxosIntegrationTest.SingleThread
{

    private static class LastCommit
    {
        public final UUID id;
        public final Set<UUID> deps;

        private LastCommit(UUID id, Set<UUID> deps)
        {
            this.id = id;
            this.deps = deps;
        }
    }

    private volatile LastCommit lastCommit = null;

    @Override
    public Node createNode(int number, String ksName, Messenger messenger)
    {
        return new Node.SingleThreaded(number, ksName, messenger)
        {
            @Override
            protected PreacceptCallback getPreacceptCallback(Instance instance, ParticipantInfo participantInfo, Runnable failureCallback, boolean forceAccept)
            {
                return new PreacceptCallback(this, instance, participantInfo, failureCallback, forceAccept)
                {
                    @Override
                    protected void processDecision(AcceptDecision decision)
                    {
                        // do nothing
                    }
                };
            }

            @Override
            public void commit(UUID iid, Set<UUID> deps)
            {
                lastCommit = new LastCommit(iid, deps);
            }
        };
    }

    @Override
    public void setUp()
    {
        super.setUp();
        lastCommit = null;
    }

    @Test
    public void successCase() throws Exception
    {
        Node node = nodes.get(0);

        Instance oldInstance = new Instance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        oldInstance.setSuccessors(Lists.newArrayList(nodes.get(1).getEndpoint()));
        oldInstance.commit(Sets.<UUID>newHashSet());
        for (Node n: nodes)
            n.addMissingInstance(oldInstance);

        Instance instance = new Instance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        instance.setSuccessors(Lists.newArrayList(nodes.get(1).getEndpoint()));
        node.preaccept(instance);

        Set<UUID> newDeps = Sets.newHashSet(instance.getDependencies());
        newDeps.add(UUIDGen.getTimeUUID());
        node.accept(instance.getId(), new AcceptDecision(true, newDeps, Collections.EMPTY_MAP), null);

        instance = node.getInstance(instance.getId());
        Assert.assertEquals(newDeps, instance.getDependencies());
        Assert.assertEquals(Instance.State.ACCEPTED, instance.getState());

        // check that a commit would have been performed
        Assert.assertNotNull(lastCommit);
    }

    @Test
    public void missingInstancesAreSent() throws Exception
    {
        Node node = nodes.get(0);

        // node 2 is unaware of this instance
        Instance oldInstance = new Instance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        oldInstance.setSuccessors(Lists.newArrayList(nodes.get(1).getEndpoint()));
        oldInstance.commit(Sets.<UUID>newHashSet());

        node.addMissingInstance(oldInstance);
        for (Node n: nodes.subList(2, nodes.size()))
            n.addMissingInstance(oldInstance);

        Assert.assertNull(nodes.get(1).getInstance(oldInstance.getId()));

        Instance instance = new Instance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        instance.setSuccessors(Lists.newArrayList(nodes.get(1).getEndpoint()));
        node.preaccept(instance);

        Map<InetAddress, Set<UUID>> missingInstances = new HashMap<>();
        missingInstances.put(nodes.get(1).getEndpoint(), Sets.newHashSet(oldInstance.getId()));

        node.accept(instance.getId(), new AcceptDecision(true, instance.getDependencies(), missingInstances), null);

        Assert.assertNotNull(nodes.get(1).getInstance(oldInstance.getId()));
    }

    @Test
    public void remoteDatacentersAreSentMessagesInLocalSerial() throws Exception
    {

    }

    @Test
    public void quorumFailure() throws Exception
    {
        Node node = nodes.get(0);

        Instance instance = new Instance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        instance.setSuccessors(Lists.newArrayList(nodes.get(1).getEndpoint()));
        node.preaccept(instance);

        setState(nodes.subList(1, nodes.size()), Node.State.DOWN);

        node.accept(instance.getId(), new AcceptDecision(true, instance.getDependencies(), Collections.EMPTY_MAP), null);

        // TODO: check not committed
        Assert.assertNull(lastCommit);
    }

    @Test
    public void ballotFailure() throws Exception
    {
        Node node = nodes.get(0);

        Instance instance = new Instance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        instance.setSuccessors(Lists.newArrayList(nodes.get(1).getEndpoint()));
        node.preaccept(instance);

        for (Node n: nodes.subList(1, nodes.size()))
            n.getInstance(instance.getId()).incrementBallot();

        node.accept(instance.getId(), new AcceptDecision(true, instance.getDependencies(), Collections.EMPTY_MAP), null);
        // TODO: check not committed
        Assert.assertNull(lastCommit);
    }
}

package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.epaxos.exceptions.BallotException;
import org.apache.cassandra.service.epaxos.integration.AbstractEpaxosIntegrationTest;
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
        node.accept(instance.getId(), new AcceptDecision(true, newDeps, Collections.EMPTY_MAP));

        Assert.assertEquals(newDeps, instance.getDependencies());
        Assert.assertEquals(Instance.State.ACCEPTED, instance.getState());
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

        node.accept(instance.getId(), new AcceptDecision(true, instance.getDependencies(), missingInstances));

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

        try
        {
            node.accept(instance.getId(), new AcceptDecision(true, instance.getDependencies(), Collections.EMPTY_MAP));
            Assert.fail("Expecting WriteTimeoutException");
        }
        catch (WriteTimeoutException e)
        {
            // expected
        }
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

        try
        {
            node.accept(instance.getId(), new AcceptDecision(true, instance.getDependencies(), Collections.EMPTY_MAP));
            Assert.fail("Expected BallotException");
        }
        catch (BallotException e)
        {
            // expected
        }
    }
}

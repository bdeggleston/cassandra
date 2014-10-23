package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.epaxos.integration.AbstractEpaxosIntegrationTest;
import org.apache.cassandra.service.epaxos.integration.Node;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

public class EpaxosPreacceptLeaderTest extends AbstractEpaxosIntegrationTest.SingleThread
{

    @Test
    public void replicasAgree() throws Exception
    {
        Node node = nodes.get(0);

        Instance oldInstance = new Instance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        oldInstance.setSuccessors(Lists.newArrayList(nodes.get(1).getEndpoint()));
        oldInstance.commit(Sets.<UUID>newHashSet());
        for (Node n: nodes)
            n.addMissingInstance(oldInstance);

        Instance instance = new Instance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        instance.setSuccessors(Lists.newArrayList(nodes.get(1).getEndpoint()));
        AcceptDecision decision = node.preaccept(instance);

        Assert.assertEquals(Sets.newHashSet(oldInstance.getId()), instance.getDependencies());
        Assert.assertFalse(instance.isFastPathImpossible());
        Assert.assertFalse(decision.acceptNeeded);
        Assert.assertEquals(instance.getDependencies(), decision.acceptDeps);
        Assert.assertEquals(Collections.EMPTY_MAP, decision.missingInstances);
    }

    @Test
    public void quorumFailure() throws Exception
    {
        setState(nodes.subList(1, nodes.size()), Node.State.DOWN);
        Node node = nodes.get(0);
        Instance instance = new Instance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        instance.setSuccessors(Lists.newArrayList(nodes.get(1).getEndpoint()));

        try
        {
            node.preaccept(instance);
            Assert.fail("Expecting WriteTimeoutException");
        }
        catch (WriteTimeoutException e)
        {
            // expected
        }

        Assert.assertEquals(Sets.<UUID>newHashSet(), instance.getDependencies());
        Assert.assertTrue(instance.isFastPathImpossible());
    }

    @Test
    public void disagreeingReplicas() throws Exception
    {
        Node node = nodes.get(0);

        // add an instance the leader doesn't know about
        Instance oldInstance = new Instance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        oldInstance.setSuccessors(Lists.newArrayList(nodes.get(1).getEndpoint()));
        oldInstance.commit(Sets.<UUID>newHashSet());
        for (Node n: nodes.subList(1, nodes.size()))
            n.addMissingInstance(oldInstance);

        Assert.assertNull(node.getInstance(oldInstance.getId()));
        Instance instance = new Instance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        instance.setSuccessors(Lists.newArrayList(nodes.get(1).getEndpoint()));
        AcceptDecision decision = node.preaccept(instance);

        Assert.assertEquals(Sets.<UUID>newHashSet(), instance.getDependencies());
        Assert.assertTrue(instance.isFastPathImpossible());
        Assert.assertTrue(decision.acceptNeeded);
        Assert.assertEquals(Sets.newHashSet(oldInstance.getId()), decision.acceptDeps);

        // check that we got our missing instance
        Assert.assertNotNull(node.getInstance(oldInstance.getId()));
    }
}

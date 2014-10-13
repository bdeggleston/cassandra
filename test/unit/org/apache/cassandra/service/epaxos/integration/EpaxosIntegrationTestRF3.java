package org.apache.cassandra.service.epaxos.integration;

import org.apache.cassandra.service.epaxos.Instance;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class EpaxosIntegrationTestRF3 extends AbstractEpaxosIntegrationTest
{
    @Override
    public int getReplicationFactor()
    {
        return 3;
    }

    @Override
    public Node createNode(InetAddress endpoint, Messenger messenger)
    {
        return new Node.SingleThreaded(endpoint, messenger);
    }

    public static void assertInstanceDeps(UUID iid, List<Node> nodes, Set<UUID> expectedDeps)
    {
        for (Node node: nodes)
        {
            Instance instance = node.getInstance(iid);
            Assert.assertEquals(expectedDeps, instance.getDependencies());
        }
    }

    public static void assertInstanceState(UUID iid, List<Node> nodes, Instance.State expectedState)
    {
        for (Node node: nodes)
        {
            Instance instance = node.getInstance(iid);
            Assert.assertEquals(expectedState, instance.getState());
        }
    }

    @Test
    public void successCase() throws Exception
    {
        Node leader = nodes.get(0);
        leader.query(getSerializedRequest());
        Instance instance1 = leader.getLastCreatedInstance();
        Set<UUID> expectedDeps = new HashSet<>();

        assertInstanceDeps(instance1.getId(), nodes, expectedDeps);
        assertInstanceState(instance1.getId(), nodes, Instance.State.EXECUTED);
    }
}

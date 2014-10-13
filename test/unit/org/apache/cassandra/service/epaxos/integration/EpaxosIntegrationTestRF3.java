package org.apache.cassandra.service.epaxos.integration;

import com.google.common.collect.Lists;
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

    /**
     * All nodes are replying to messages
     */
    @Test
    public void successCase() throws Exception
    {
        Node leader1 = nodes.get(0);
        leader1.query(getSerializedRequest());
        Instance instance1 = leader1.getLastCreatedInstance();
        Set<UUID> expectedDeps = new HashSet<>();

        assertInstanceDeps(instance1.getId(), nodes, expectedDeps);
        assertInstanceState(instance1.getId(), nodes, Instance.State.EXECUTED);
        Assert.assertFalse(leader1.accepted.contains(instance1.getId()));

        Node leader2 = nodes.get(1);
        leader2.query(getSerializedRequest());
        Instance instance2 = leader2.getLastCreatedInstance();
        expectedDeps.add(instance1.getId());

        assertInstanceDeps(instance2.getId(), nodes, expectedDeps);
        assertInstanceState(instance2.getId(), nodes, Instance.State.EXECUTED);
        Assert.assertFalse(leader2.accepted.contains(instance2.getId()));

        List<UUID> expectedOrder = Lists.newArrayList(instance1.getId(), instance2.getId());
        assertExecutionOrder(nodes, expectedOrder);
    }

    /**
     * only a quorum of nodes respond
     */
    @Test
    public void quorumSuccessCase() throws Exception
    {
        List<Node> up = nodes.subList(0, quorumSize() - 1);
        List<Node> down = nodes.subList(quorumSize(), nodes.size() - 1);
        setState(up, Node.State.UP);
        setState(down, Node.State.DOWN);

        boolean acceptExpected = fastPathQuorumSize() > quorumSize();

        Node leader1 = nodes.get(0);
        leader1.query(getSerializedRequest());
        Instance instance1 = leader1.getLastCreatedInstance();
        Set<UUID> expectedDeps = new HashSet<>();

        assertInstanceUnknown(instance1.getId(), down);
        assertInstanceDeps(instance1.getId(), up, expectedDeps);
        assertInstanceState(instance1.getId(), up, Instance.State.EXECUTED);
        Assert.assertEquals(acceptExpected, leader1.accepted.contains(instance1.getId()));

        Node leader2 = nodes.get(1);
        leader2.query(getSerializedRequest());
        Instance instance2 = leader2.getLastCreatedInstance();
        expectedDeps.add(instance1.getId());

        assertInstanceUnknown(instance2.getId(), down);
        assertInstanceDeps(instance2.getId(), up, expectedDeps);
        assertInstanceState(instance2.getId(), up, Instance.State.EXECUTED);
        Assert.assertEquals(acceptExpected, leader2.accepted.contains(instance2.getId()));

        List<UUID> expectedOrder = Lists.newArrayList(instance1.getId(), instance2.getId());
        assertExecutionOrder(up, expectedOrder);
    }

    /**
     * only a fast quorum of nodes respond
     */
    @Test
    public void fastQuorumSuccessCase() throws Exception
    {
        List<Node> up = nodes.subList(0, fastPathQuorumSize() - 1);
        List<Node> down = nodes.subList(fastPathQuorumSize(), nodes.size() - 1);
        setState(up, Node.State.UP);
        setState(down, Node.State.DOWN);

        Node leader1 = nodes.get(0);
        leader1.query(getSerializedRequest());
        Instance instance1 = leader1.getLastCreatedInstance();
        Set<UUID> expectedDeps = new HashSet<>();

        assertInstanceUnknown(instance1.getId(), down);
        assertInstanceDeps(instance1.getId(), up, expectedDeps);
        assertInstanceState(instance1.getId(), up, Instance.State.EXECUTED);
        Assert.assertFalse(leader1.accepted.contains(instance1.getId()));

        Node leader2 = nodes.get(1);
        leader2.query(getSerializedRequest());
        Instance instance2 = leader2.getLastCreatedInstance();
        expectedDeps.add(instance1.getId());

        assertInstanceUnknown(instance2.getId(), down);
        assertInstanceDeps(instance2.getId(), up, expectedDeps);
        assertInstanceState(instance2.getId(), up, Instance.State.EXECUTED);
        Assert.assertFalse(leader2.accepted.contains(instance2.getId()));

        List<UUID> expectedOrder = Lists.newArrayList(instance1.getId(), instance2.getId());
        assertExecutionOrder(up, expectedOrder);
    }
}

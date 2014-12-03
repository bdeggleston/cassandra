package org.apache.cassandra.service.epaxos.integration;

import com.google.common.collect.Lists;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.service.epaxos.AbstractEpaxosTest;
import org.apache.cassandra.service.epaxos.Instance;
import org.junit.Assert;
import org.junit.Before;

import java.util.*;

public abstract class AbstractEpaxosIntegrationTest extends AbstractEpaxosTest
{
    public List<Node> nodes;
    public Messenger messenger;

    protected String createTestKeyspace()
    {
        String ksName = String.format("epaxos_%s", System.currentTimeMillis());
        List<CFMetaData> cfDefs = Lists.newArrayListWithCapacity(getReplicationFactor() * 2);
        for (int i=0; i<getReplicationFactor(); i++)
        {
            CFMetaData instanceTable = new CFMetaData(ksName,
                                                      String.format("%s_%s", SystemKeyspace.EPAXOS_INSTANCE, i + 1),
                                                      CFMetaData.EpaxosInstanceCf.cfType,
                                                      CFMetaData.EpaxosInstanceCf.comparator);
            instanceTable = CFMetaData.copyOpts(instanceTable, CFMetaData.EpaxosInstanceCf);
            cfDefs.add(instanceTable);

            CFMetaData dependencyTable = new CFMetaData(ksName,
                                                        String.format("%s_%s", SystemKeyspace.EPAXOS_KEY_STATE, i + 1),
                                                        CFMetaData.EpaxosKeyStateCF.cfType,
                                                        CFMetaData.EpaxosKeyStateCF.comparator);
            dependencyTable = CFMetaData.copyOpts(dependencyTable, CFMetaData.EpaxosKeyStateCF);

            cfDefs.add(dependencyTable);
        }

        KSMetaData ks = KSMetaData.newKeyspace(ksName, LocalStrategy.class, Collections.EMPTY_MAP, true, cfDefs);
        Schema.instance.load(ks);
        return ksName;
    }

    public int getReplicationFactor()
    {
        return 3;
    }

    public abstract Node createNode(int number, String ksName, Messenger messenger);

    public int quorumSize()
    {
        int f = getReplicationFactor() / 2;
        return f + 1;
    }

    public int fastPathQuorumSize()
    {
        int f = getReplicationFactor() / 2;
        return f + ((f + 1) / 2);
    }

    public static void setState(List<Node> nodes, Node.State state)
    {
        for (Node node: nodes)
            node.setState(state);
    }

    public static void assertInstanceUnknown(UUID iid, List<Node> nodes)
    {
        for (Node node: nodes)
        {
            String msg = String.format("Node found unexpectedly on %s", node.getEndpoint());
            Assert.assertNull(msg, node.getInstance(iid));
        }
    }

    public static void assertInstanceDeps(UUID iid, List<Node> nodes, Set<UUID> expectedDeps)
    {
        for (Node node: nodes)
        {
            Instance instance = node.getInstance(iid);
            String msg = String.format("Deps mismatch on %s", node.getEndpoint());
            Assert.assertEquals(msg, expectedDeps, instance.getDependencies());
        }
    }

    public static void assertInstanceState(UUID iid, List<Node> nodes, Instance.State expectedState)
    {
        for (Node node: nodes)
        {
            Instance instance = node.getInstance(iid);
            String msg = String.format("State mismatch on %s", node.getEndpoint());
            Assert.assertEquals(msg, expectedState, instance.getState());
        }
    }

    public static void assertExecutionOrder(List<Node> nodes, List<UUID> expectedOrder)
    {
        for (Node node: nodes)
        {
            String msg = String.format("Order mismatch on %s", node.getEndpoint());
            Assert.assertEquals(msg, expectedOrder, node.executionOrder);
        }
    }

    public static void assertInstanceLeaderDepsMatch(UUID iid, List<Node> nodes, boolean expected)
    {
        for (Node node: nodes)
        {
            Instance instance = node.getInstance(iid);
            String msg = String.format("Unexpected leader deps match value on %s", node.getEndpoint());
            Assert.assertEquals(msg, expected, instance.getLeaderDepsMatch());
        }
    }

    protected Messenger createMessenger()
    {
        return new Messenger();
    }

    @Before
    public void setUp()
    {
        String ksName = createTestKeyspace();
        messenger = createMessenger();
        nodes = Lists.newArrayListWithCapacity(getReplicationFactor());
        for (int i=0; i<getReplicationFactor(); i++)
        {
            Node node = createNode(i + 1, ksName, messenger);
            messenger.registerNode(node);
            nodes.add(node);
        }
    }

    public abstract static class SingleThread extends AbstractEpaxosIntegrationTest
    {
        @Override
        public Node createNode(final int nodeNumber, final String ksName, Messenger messenger)
        {
            return new Node.SingleThreaded(nodeNumber, messenger)
            {
                @Override
                protected String keyspace()
                {
                    return ksName;
                }

                @Override
                protected String instanceTable()
                {
                    return String.format("%s_%s", SystemKeyspace.EPAXOS_INSTANCE, nodeNumber);
                }

                @Override
                protected String keyStateTable()
                {
                    return String.format("%s_%s", SystemKeyspace.EPAXOS_KEY_STATE, nodeNumber);
                }

                @Override
                protected String tokenStateTable()
                {
                    return String.format("%s_%s", SystemKeyspace.EPAXOS_TOKEN_STATE, nodeNumber);
                }
            };
        }
    }
}

package org.apache.cassandra.service.epaxos.integration;

import com.google.common.collect.Lists;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ThriftCASRequest;
import org.apache.cassandra.service.epaxos.Instance;
import org.apache.cassandra.service.epaxos.SerializedRequest;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public abstract class AbstractEpaxosIntegrationTest
{

    private static KSMetaData ksm;
    private static CFMetaData cfm;

    static
    {
        DatabaseDescriptor.getConcurrentWriters();
        MessagingService.instance();
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        Map<String, String> ksOpts = new HashMap<>();
        ksOpts.put("replication_factor", "1");
        cfm = CFMetaData.denseCFMetaData("ks", "tbl", Int32Type.instance);
        ksm = KSMetaData.newKeyspace("ks", SimpleStrategy.class, ksOpts, true, Arrays.asList(cfm));
        Schema.instance.load(ksm);
    }

    protected ThriftCASRequest getCasRequest()
    {
        ColumnFamily expected = ArrayBackedSortedColumns.factory.create("ks", "tbl");
        expected.addColumn(CellNames.simpleDense(ByteBufferUtil.bytes("v")), ByteBufferUtil.bytes(2), 3L);

        ColumnFamily updates = ArrayBackedSortedColumns.factory.create("ks", "tbl");
        updates.addColumn(CellNames.simpleDense(ByteBufferUtil.bytes("v")), ByteBufferUtil.bytes(5), 6L);

        return new ThriftCASRequest(expected, updates);
    }

    protected SerializedRequest getSerializedRequest()
    {
        ThriftCASRequest thriftRequest = getCasRequest();
        SerializedRequest.Builder builder = SerializedRequest.builder();
        builder.casRequest(thriftRequest);
        builder.cfName(cfm.cfName);
        builder.keyspaceName(cfm.ksName);
        builder.key(ByteBufferUtil.bytes(7));
        builder.consistencyLevel(ConsistencyLevel.SERIAL);
        return builder.build();
    }

    public abstract int getReplicationFactor();
    public abstract Node createNode(InetAddress endpoint, Messenger messenger);

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

    public void setState(List<Node> nodes, Node.State state)
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


    public List<Node> nodes;
    public Messenger messenger;

    @Before
    public void setUp()
    {
        messenger = new Messenger();
        nodes = Lists.newArrayListWithCapacity(getReplicationFactor());
        for (int i=0; i<getReplicationFactor(); i++)
        {
            Node node;
            try
            {
                node = createNode(InetAddress.getByAddress(new byte[]{127, 0, 0, (byte) (i + 1)}), messenger);
            }
            catch (UnknownHostException e)
            {
                throw new AssertionError(e);
            }
            messenger.registerNode(node);
            nodes.add(node);
        }
    }
}

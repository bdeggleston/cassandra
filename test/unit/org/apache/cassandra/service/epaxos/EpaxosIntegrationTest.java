package org.apache.cassandra.service.epaxos;

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
import org.apache.cassandra.service.epaxos.integration.Messenger;
import org.apache.cassandra.service.epaxos.integration.Node;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Before;
import org.junit.BeforeClass;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class EpaxosIntegrationTest
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

    private ThriftCASRequest getCasRequest()
    {
        ColumnFamily expected = ArrayBackedSortedColumns.factory.create("ks", "tbl");
        expected.addColumn(CellNames.simpleDense(ByteBufferUtil.bytes("v")), ByteBufferUtil.bytes(2), 3L);

        ColumnFamily updates = ArrayBackedSortedColumns.factory.create("ks", "tbl");
        updates.addColumn(CellNames.simpleDense(ByteBufferUtil.bytes("v")), ByteBufferUtil.bytes(5), 6L);

        return new ThriftCASRequest(expected, updates);
    }

    private SerializedRequest getSerializedRequest()
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
    private List<Node> nodes;
    private Messenger messenger;

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
                node = new Node(InetAddress.getByAddress(new byte[]{127, 0, 0, (byte) i}), messenger);
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

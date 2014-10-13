package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ThriftCASRequest;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

public class EpaxosSerializationTest
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

    @Test
    public void checkThriftCasRequest() throws Exception
    {
        ThriftCASRequest request = getCasRequest();

        DataOutputBuffer out = new DataOutputBuffer();
        ThriftCASRequest.serializer.serialize(request, out, 0);
        int expectedSize = out.getLength();
        Assert.assertEquals(expectedSize, ThriftCASRequest.serializer.serializedSize(request, 0));

        ThriftCASRequest deserialized = ThriftCASRequest.serializer.deserialize(ByteStreams.newDataInput(out.getData()), 0);

        Assert.assertEquals(request, deserialized);
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

    @Test
    public void checkSerializedRequest() throws Exception
    {
        SerializedRequest request = getSerializedRequest();

        DataOutputBuffer out = new DataOutputBuffer();
        SerializedRequest.serializer.serialize(request, out, 0);
        int expectedSize = out.getLength();
        Assert.assertEquals(expectedSize, SerializedRequest.serializer.serializedSize(request, 0));

        SerializedRequest deserialized = SerializedRequest.serializer.deserialize(ByteStreams.newDataInput(out.getData()), 0);

        Assert.assertEquals(request, deserialized);
    }

    @Test
    public void checkInstance() throws Exception
    {
        Instance instance = new Instance(getSerializedRequest());
        Set<UUID> deps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.preaccept(deps, deps);
        instance.updateBallot(5);

        // shouldn't be serialized
        instance.setStronglyConnected(Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID()));

        DataOutputBuffer out = new DataOutputBuffer();
        Instance.serializer.serialize(instance, out, 0);
        int expectedSize = out.getLength();
        Assert.assertEquals(expectedSize, Instance.serializer.serializedSize(instance, 0));

        Instance deserialized = Instance.serializer.deserialize(ByteStreams.newDataInput(out.getData()), 0);

        Assert.assertEquals(instance.getId(), deserialized.getId());
        Assert.assertEquals(instance.getState(), deserialized.getState());
        Assert.assertEquals(instance.getDependencies(), deserialized.getDependencies());
        Assert.assertEquals(instance.getLeaderDepsMatch(), deserialized.getLeaderDepsMatch());
        Assert.assertEquals(instance.getBallot(), deserialized.getBallot());

        // strongly connected components shouldn't be serialized
        Assert.assertNotNull(instance.getStronglyConnected());
        Assert.assertNull(deserialized.getStronglyConnected());
    }
}

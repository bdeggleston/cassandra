package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.CQL3CasRequest;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.CASRequest;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.ThriftCASRequest;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Ignore
public abstract class AbstractEpaxosTest
{
    protected static KSMetaData ksm;
    protected static CFMetaData cfm;
    protected static CFMetaData thriftcf;
    protected static final InetAddress LOCALHOST;

    static
    {
        DatabaseDescriptor.setPartitioner(new ByteOrderedPartitioner());
        DatabaseDescriptor.getConcurrentWriters();
        MessagingService.instance();
        SchemaLoader.prepareServer();
        try
        {
            LOCALHOST = InetAddress.getByName("127.0.0.1");
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    protected static final Token TOKEN = DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(0));
    protected static final UUID CFID = UUIDGen.getTimeUUID();

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        cfm = CFMetaData.compile("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT);", "ks");
        thriftcf = CFMetaData.denseCFMetaData("ks", "thrifttbl", Int32Type.instance);
        Map<String, String> ksOpts = new HashMap<>();
        ksOpts.put("replication_factor", "1");
        ksm = KSMetaData.newKeyspace("ks", SimpleStrategy.class, ksOpts, true, Arrays.asList(cfm, thriftcf));
        Schema.instance.load(ksm);
    }

    protected void clearKeyStates()
    {
        String select = String.format("SELECT row_key FROM %s.%s", Keyspace.SYSTEM_KS, SystemKeyspace.EPAXOS_KEY_STATE);
        String delete = String.format("DELETE FROM %s.%s WHERE row_key=?", Keyspace.SYSTEM_KS, SystemKeyspace.EPAXOS_KEY_STATE);
        UntypedResultSet result = QueryProcessor.executeInternal(select);

        while (!result.isEmpty())
        {
            for (UntypedResultSet.Row row: result)
            {
                QueryProcessor.executeInternal(delete, row.getBlob("row_key"));
            }
            result = QueryProcessor.executeInternal(select);
        }

        Assert.assertEquals(0, QueryProcessor.executeInternal(select).size());
    }

    protected void clearTokenStates()
    {
        String select = String.format("SELECT cf_id FROM %s.%s", Keyspace.SYSTEM_KS, SystemKeyspace.EPAXOS_TOKEN_STATE);
        String delete = String.format("DELETE FROM %s.%s WHERE cf_id=?", Keyspace.SYSTEM_KS, SystemKeyspace.EPAXOS_TOKEN_STATE);
        UntypedResultSet result = QueryProcessor.executeInternal(select);

        while (!result.isEmpty())
        {
            for (UntypedResultSet.Row row: result)
            {
                QueryProcessor.executeInternal(delete, row.getBlob("cf_id"));
            }
            result = QueryProcessor.executeInternal(select);
        }

        Assert.assertEquals(0, QueryProcessor.executeInternal(select).size());
    }

    protected MessageEnvelope<Instance> wrapInstance(Instance instance)
    {
        return wrapInstance(instance, 0);
    }

    protected MessageEnvelope<Instance> wrapInstance(Instance instance, long epoch)
    {
        return new MessageEnvelope<>(instance.getToken(), instance.getCfId(), epoch, instance);
    }

    protected ThriftCASRequest getThriftCasRequest()
    {
        ColumnFamily expected = ArrayBackedSortedColumns.factory.create("ks", thriftcf.cfName);
        expected.addColumn(CellNames.simpleDense(ByteBufferUtil.bytes("v")), ByteBufferUtil.bytes(2), 3L);

        ColumnFamily updates = ArrayBackedSortedColumns.factory.create("ks", thriftcf.cfName);
        updates.addColumn(CellNames.simpleDense(ByteBufferUtil.bytes("v")), ByteBufferUtil.bytes(5), 6L);

        return new ThriftCASRequest(expected, updates);
    }

    protected CQL3CasRequest getCqlCasRequest(int k, int v, ConsistencyLevel consistencyLevel)
    {
        try
        {
            String query = "INSERT INTO ks.tbl (k, v) VALUES (?, ?) IF NOT EXISTS";
            ModificationStatement.Parsed parsed = (ModificationStatement.Parsed) QueryProcessor.parseStatement(query);
            parsed.prepareKeyspace("ks");
            parsed.setQueryString(query);
            ParsedStatement.Prepared prepared = parsed.prepare();

            QueryOptions options = QueryOptions.create(consistencyLevel,
                                                       Lists.newArrayList(ByteBufferUtil.bytes(k), ByteBufferUtil.bytes(v)),
                                                       false, 1, null, ConsistencyLevel.QUORUM);
            options.prepare(prepared.boundNames);
            QueryState state = QueryState.forInternalCalls();

            ModificationStatement statement = (ModificationStatement) prepared.statement;

            return statement.createCasRequest(state, options);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }
    protected SerializedRequest newSerializedRequest(CASRequest request)
    {
        return newSerializedRequest(request, ConsistencyLevel.SERIAL);
    }

    protected SerializedRequest newSerializedRequest(CASRequest request, ConsistencyLevel consistencyLevel)
    {
        return newSerializedRequest(request, ByteBufferUtil.bytes(7), consistencyLevel);
    }

    protected SerializedRequest newSerializedRequest(CASRequest request, ByteBuffer key, ConsistencyLevel consistencyLevel)
    {
        SerializedRequest.Builder builder = SerializedRequest.builder();
        builder.casRequest(request);
        builder.cfName(cfm.cfName);
        builder.keyspaceName(cfm.ksName);
        builder.key(key);
        builder.consistencyLevel(consistencyLevel);
        return builder.build();
    }

    protected SerializedRequest getSerializedThriftRequest()
    {
        ThriftCASRequest casRequest = getThriftCasRequest();
        return newSerializedRequest(casRequest);
    }

    protected SerializedRequest getSerializedCQLRequest(int k, int v)
    {
        return getSerializedCQLRequest(k, v, ConsistencyLevel.SERIAL);
    }

    protected SerializedRequest getSerializedCQLRequest(int k, int v, ConsistencyLevel cl)
    {
        CQL3CasRequest casRequest = getCqlCasRequest(k, v, cl);
        return newSerializedRequest(casRequest, casRequest.getKey(), cl);
    }

}

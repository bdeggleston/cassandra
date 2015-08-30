package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.ByteBufferDataInput;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class SinglePartitionSliceCommandTest
{
    private static final Logger logger = LoggerFactory.getLogger(SinglePartitionSliceCommandTest.class);

    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tbl";

    private static CFMetaData cfm;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        cfm = CFMetaData.Builder.create(KEYSPACE, TABLE)
                                .addPartitionKey("k", UTF8Type.instance)
                                .addStaticColumn("s", UTF8Type.instance)
                                .addClusteringColumn("i", IntegerType.instance)
                                .addRegularColumn("v", UTF8Type.instance)
                                .build();

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), cfm);
        cfm = Schema.instance.getCFMetaData(KEYSPACE, TABLE);
    }

    @Test
    public void staticColumnsAreFiltered() throws IOException
    {
        DecoratedKey key = cfm.decorateKey(ByteBufferUtil.bytes("k"));
        ColumnDefinition v = cfm.getColumnDefinition(new ColumnIdentifier("v", true));

        UntypedResultSet rows;

        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, s, i, v) VALUES ('k', 's', 0, 'v')");
        QueryProcessor.executeInternal("DELETE v FROM ks.tbl WHERE k='k' AND i=0");
        QueryProcessor.executeInternal("DELETE FROM ks.tbl WHERE k='k' AND i=0");
        rows = QueryProcessor.executeInternal("SELECT * FROM ks.tbl WHERE k='k' AND i=0");

        for (UntypedResultSet.Row row: rows)
        {
            logger.debug("Current: k={}, s={}, v={}", (row.has("k") ? row.getString("k") : null), (row.has("s") ? row.getString("s") : null), (row.has("v") ? row.getString("v") : null));
        }

        assert rows.isEmpty();

        ColumnFilter columnFilter = ColumnFilter.selection(PartitionColumns.of(v));
        ByteBuffer zero = ByteBufferUtil.bytes(0);
        Slices slices = Slices.with(cfm.comparator, Slice.make(Slice.Bound.inclusiveStartOf(zero), Slice.Bound.inclusiveEndOf(zero)));
        ClusteringIndexSliceFilter sliceFilter = new ClusteringIndexSliceFilter(slices, false);
        ReadCommand cmd = new SinglePartitionSliceCommand(false, true, cfm,
                                                          FBUtilities.nowInSeconds(),
                                                          columnFilter,
                                                          RowFilter.NONE,
                                                          DataLimits.NONE,
                                                          key,
                                                          sliceFilter);

        DataOutputBuffer out = new DataOutputBuffer((int) ReadCommand.legacyReadCommandSerializer.serializedSize(cmd, MessagingService.VERSION_21));
        ReadCommand.legacyReadCommandSerializer.serialize(cmd, out, MessagingService.VERSION_21);
        DataInputPlus in = new ByteBufferDataInput(out.buffer(), null, 0, 0);
        cmd = ReadCommand.legacyReadCommandSerializer.deserialize(in, MessagingService.VERSION_21);

        logger.debug("ReadCommand: {}", cmd);
        UnfilteredPartitionIterator partitionIterator = cmd.executeLocally(ReadOrderGroup.emptyGroup());
        ReadResponse response = ReadResponse.createDataResponse(partitionIterator, cmd.columnFilter());

        logger.debug("creating response: {}", response);
        partitionIterator = response.makeIterator(cfm, null);  // <- cmd is null
        assert partitionIterator.hasNext();
        UnfilteredRowIterator partition = partitionIterator.next();

        LegacyLayout.LegacyUnfilteredPartition rowIter = LegacyLayout.fromUnfilteredRowIterator(partition);
        Assert.assertEquals(Collections.emptyList(), rowIter.cells);
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;

public class MutationIdRangeTest
{
    private static final String KS = "ks";
    private static final String TBL = "tbl";
    private static TableId cfid;

    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
        TableMetadata tableMetadata = TableMetadata.builder("ks", "tbl")
                                                   .addPartitionKeyColumn("k", Int32Type.instance)
                                                   .addRegularColumn("v", Int32Type.instance)
                                                   .build();
        cfid = tableMetadata.id;
        SchemaLoader.createKeyspace(KS, KeyspaceParams.simple(1, ReplicationType.logged), tableMetadata);
    }

    private static Mutation createMutation(TableMetadata tableMetadata, int k, int v)
    {
        DecoratedKey key = tableMetadata.partitioner.decorateKey(ByteBufferUtil.bytes(1));
        SimpleBuilders.MutationBuilder builder = new SimpleBuilders.MutationBuilder(KS, key);
        PartitionUpdate.SimpleBuilder partition = builder.update(tableMetadata);
        partition.row().add("v", 1);
        Mutation mutation = builder.build();
        Assert.assertFalse(mutation.id().isNone());
        return mutation;
    }

    private static MutationId applyMutation(TableMetadata tableMetadata, int k, int v)
    {
        Mutation mutation = createMutation(tableMetadata, 1, 1);
        mutation.apply();
        return mutation.id();
    }

    private static void assertViewContents(View view, int numMemtables, int numSSTables)
    {
        Assert.assertEquals(numMemtables, view.liveMemtables.size());
        Assert.assertEquals(numSSTables, view.liveSSTables().size());
    }

    private static void assertEmptyMemtable(View view)
    {
        Assert.assertEquals(0, view.getCurrentMemtable().operationCount());
    }

    private static void assertNonEmptyMemtable(View view)
    {
        Assert.assertTrue(view.getCurrentMemtable().operationCount() > 0);
    }

    private static void assertNumSSTables(View view, int numSSTables)
    {
        Assert.assertEquals(numSSTables, view.liveSSTables().size());
    }

    /**
     * Test that mutation ids go from the memtable to sstables, and are combined during compaction
     */
    @Test
    public void mutationIdLifecycleTest()
    {
        // TODO: check against all memtable types (David Capwell style)
        Keyspace keyspace = Keyspace.open(KS);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(TBL);
        cfs.disableAutoCompaction();

        // pre-apply
        {
            View view = cfs.getTracker().getView();
            assertEmptyMemtable(view);
            assertNumSSTables(view, 0);
        }

        MutationId id1;
        MutationId id2;
        // apply 1
        {
            id1 = applyMutation(cfs.metadata(), 1, 1);
            id2 = applyMutation(cfs.metadata(), 2, 2);

            View view = cfs.getTracker().getView();
            assertNonEmptyMemtable(view);
            assertNumSSTables(view, 0);

            Memtable memtable = view.getCurrentMemtable();
            Assert.assertEquals(new MutationIdRanges(id1, id2), memtable.getMutationIdRanges());
        }

        // flush 1
        {
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

            View view = cfs.getTracker().getView();
            assertEmptyMemtable(view);
            assertNumSSTables(view, 1);

            SSTableReader sstable = Iterables.getOnlyElement(view.liveSSTables());
            Assert.assertEquals(new MutationIdRanges(id1, id2), sstable.getMutationIdRanges());
        }

        MutationId id3;
        MutationId id4;
        // apply 2
        {
            id3 = applyMutation(cfs.metadata(), 3, 3);
            id4 = applyMutation(cfs.metadata(), 4, 4);

            View view = cfs.getTracker().getView();
            assertNonEmptyMemtable(view);
            assertNumSSTables(view, 1);

            Memtable memtable = view.getCurrentMemtable();
            Assert.assertEquals(new MutationIdRanges(id3, id4), memtable.getMutationIdRanges());
        }

        // flush 2
        {
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

            View view = cfs.getTracker().getView();
            assertEmptyMemtable(view);
            assertNumSSTables(view, 2);

            List<SSTableReader> sstables = Lists.newArrayList(view.liveSSTables());
            sstables.sort(Comparator.comparing(sst -> sst.descriptor.id.asBytes()));
            Assert.assertEquals(new MutationIdRanges(id1, id2), sstables.get(0).getMutationIdRanges());
            Assert.assertEquals(new MutationIdRanges(id3, id4), sstables.get(1).getMutationIdRanges());
        }

        // compaction
        {
            cfs.forceMajorCompaction();

            View view = cfs.getTracker().getView();
            assertEmptyMemtable(view);
            assertNumSSTables(view, 1);

            SSTableReader sstable = Iterables.getOnlyElement(view.liveSSTables());
            Assert.assertEquals(new MutationIdRanges(id1, id4), sstable.getMutationIdRanges());
        }
    }
}

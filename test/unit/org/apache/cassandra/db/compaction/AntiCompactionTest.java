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
package org.apache.cassandra.db.compaction;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.ByteOrderedPartitioner.BytesToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.utils.concurrent.Transactional;

import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;
import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class AntiCompactionTest
{
    private static final String KEYSPACE1 = "AntiCompactionTest";
    private static final String CF = "AntiCompactionTest";
    private static TableMetadata metadata;
    private static ColumnFamilyStore cfs;

    private static Collection<Range<Token>> NO_RANGES = Collections.emptyList();

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        metadata = SchemaLoader.standardCFMD(KEYSPACE1, CF).build();
        SchemaLoader.createKeyspace(KEYSPACE1, KeyspaceParams.simple(1), metadata);
        cfs = Schema.instance.getColumnFamilyStoreInstance(metadata.id);
    }

    @After
    public void truncateCF()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.truncateBlocking();
    }

    private void registerParentRepairSession(UUID sessionID, Collection<Range<Token>> ranges, long repairedAt, UUID pendingRepair) throws IOException
    {
        ActiveRepairService.instance.registerParentRepairSession(sessionID,
                                                                 InetAddressAndPort.getByName("10.0.0.1"),
                                                                 Lists.newArrayList(cfs), ranges,
                                                                 pendingRepair != null || repairedAt != UNREPAIRED_SSTABLE,
                                                                 repairedAt, true, PreviewKind.NONE);
    }

    private static Collection<Range<Token>> range(int l, int r)
    {
        return Collections.singleton(new Range<>(new BytesToken(Integer.toString(l).getBytes()), new BytesToken(Integer.toString(r).getBytes())));
    }

    private static class SSTableStats
    {
        int numLiveSSTables = 0;
        int pendingKeys = 0;
        int transKeys = 0;
        int unrepairedKeys = 0;
    }

    private SSTableStats antiCompactRanges(ColumnFamilyStore store, Collection<Range<Token>> fullRanges, Collection<Range<Token>> transRanges) throws IOException
    {
        UUID sessionID = UUID.randomUUID();
        Collection<SSTableReader> sstables = getUnrepairedSSTables(store);
        try (LifecycleTransaction txn = store.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            if (txn == null)
                throw new IllegalStateException();
            registerParentRepairSession(sessionID, Sets.newHashSet(Iterables.concat(fullRanges, transRanges)), FBUtilities.nowInSeconds(), sessionID);
            CompactionManager.instance.performAnticompaction(store, fullRanges, transRanges, refs, txn, sessionID);
        }

        SSTableStats stats = new SSTableStats();
        stats.numLiveSSTables = store.getLiveSSTables().size();

        Predicate<Token> fullContains = t -> Iterables.any(fullRanges, r -> r.contains(t));
        Predicate<Token> transContains = t -> Iterables.any(transRanges, r -> r.contains(t));
        for (SSTableReader sstable : store.getLiveSSTables())
        {
            assertFalse(sstable.isRepaired());
            assertEquals(sstable.isPendingRepair() ? sessionID : NO_PENDING_REPAIR, sstable.getPendingRepair());
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                while (scanner.hasNext())
                {
                    UnfilteredRowIterator row = scanner.next();
                    Token token = row.partitionKey().getToken();
                    if (sstable.isPendingRepair() && !sstable.isTransient())
                    {
                        assertTrue(fullContains.test(token));
                        assertFalse(transContains.test(token));
                        stats.pendingKeys++;
                    }
                    else if (sstable.isPendingRepair() && sstable.isTransient())
                    {

                        assertTrue(transContains.test(token));
                        assertFalse(fullContains.test(token));
                        stats.transKeys++;
                    }
                    else
                    {
                        assertFalse(fullContains.test(token));
                        assertFalse(transContains.test(token));
                        stats.unrepairedKeys++;
                    }
                }
            }
        }
        for (SSTableReader sstable : store.getLiveSSTables())
        {
            assertFalse(sstable.isMarkedCompacted());
            assertEquals(1, sstable.selfRef().globalCount());
        }
        assertEquals(0, store.getTracker().getCompacting().size());
        return stats;
    }

    @Test
    public void antiCompactOneFull() throws Exception
    {
        ColumnFamilyStore store = prepareColumnFamilyStore();
        SSTableStats stats = antiCompactRanges(store, range(0, 4), NO_RANGES);
        assertEquals(2, stats.numLiveSSTables);
        assertEquals(stats.pendingKeys, 4);
        assertEquals(stats.transKeys, 0);
        assertEquals(stats.unrepairedKeys, 6);
    }

    @Test
    public void antiCompactOneMixed() throws Exception
    {
        ColumnFamilyStore store = prepareColumnFamilyStore();
        SSTableStats stats = antiCompactRanges(store, range(0, 4), range(4, 8));
        assertEquals(3, stats.numLiveSSTables);
        assertEquals(stats.pendingKeys, 4);
        assertEquals(stats.transKeys, 4);
        assertEquals(stats.unrepairedKeys, 2);
    }

    @Test
    public void antiCompactOneTransOnly() throws Exception
    {
        ColumnFamilyStore store = prepareColumnFamilyStore();
        SSTableStats stats = antiCompactRanges(store, NO_RANGES, range(0, 4));
        assertEquals(2, stats.numLiveSSTables);
        assertEquals(stats.pendingKeys, 0);
        assertEquals(stats.transKeys, 4);
        assertEquals(stats.unrepairedKeys, 6);
    }

    @Test
    public void antiCompactionSizeTest() throws InterruptedException, IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.disableAutoCompaction();
        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        Range<Token> range = new Range<Token>(new BytesToken(ByteBufferUtil.bytes(0)), new BytesToken(ByteBufferUtil.bytes(500)));
        List<Range<Token>> ranges = Arrays.asList(range);
        Collection<SSTableReader> sstables = cfs.getLiveSSTables();
        UUID parentRepairSession = UUID.randomUUID();
        registerParentRepairSession(parentRepairSession, ranges, UNREPAIRED_SSTABLE, UUIDGen.getTimeUUID());
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            CompactionManager.instance.performAnticompaction(cfs, ranges, NO_RANGES, refs, txn, parentRepairSession);
        }
        long sum = 0;
        long rows = 0;
        for (SSTableReader x : cfs.getLiveSSTables())
        {
            sum += x.bytesOnDisk();
            rows += x.getTotalRows();
        }
        assertEquals(sum, cfs.metric.liveDiskSpaceUsed.getCount());
        assertEquals(rows, 1000 * (1000 * 5));//See writeFile for how this number is derived
    }

    private SSTableReader writeFile(ColumnFamilyStore cfs, int count)
    {
        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        Descriptor desc = cfs.newSSTableDescriptor(dir);

        try (SSTableTxnWriter writer = SSTableTxnWriter.create(cfs, desc, 0, 0, NO_PENDING_REPAIR, false, new SerializationHeader(true, cfs.metadata(), cfs.metadata().regularAndStaticColumns(), EncodingStats.NO_STATS)))
        {
            for (int i = 0; i < count; i++)
            {
                UpdateBuilder builder = UpdateBuilder.create(metadata, ByteBufferUtil.bytes(i));
                for (int j = 0; j < count * 5; j++)
                    builder.newRow("c" + j).add("val", "value1");
                writer.append(builder.build().unfilteredIterator());

            }
            Collection<SSTableReader> sstables = writer.finish(true);
            assertNotNull(sstables);
            assertEquals(1, sstables.size());
            return sstables.iterator().next();
        }
    }

    public void generateSStable(ColumnFamilyStore store, String Suffix)
    {
        for (int i = 0; i < 10; i++)
        {
            String localSuffix = Integer.toString(i);
            new RowUpdateBuilder(metadata, System.currentTimeMillis(), localSuffix + "-" + Suffix)
                    .clustering("c")
                    .add("val", "val" + localSuffix)
                    .build()
                    .applyUnsafe();
        }
        store.forceBlockingFlush();
    }

    @Test
    public void antiCompactTenFull() throws InterruptedException, IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.disableAutoCompaction();

        for (int table = 0; table < 10; table++)
        {
            generateSStable(store,Integer.toString(table));
        }
        SSTableStats stats = antiCompactRanges(store, range(0, 4), NO_RANGES);
        /*
        Anticompaction will be anti-compacting 10 SSTables but will be doing this two at a time
        so there will be no net change in the number of sstables
         */
        assertEquals(10, stats.numLiveSSTables);
        assertEquals(stats.pendingKeys, 40);
        assertEquals(stats.transKeys, 0);
        assertEquals(stats.unrepairedKeys, 60);
    }

    @Test
    public void antiCompactTenTrans() throws InterruptedException, IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.disableAutoCompaction();

        for (int table = 0; table < 10; table++)
        {
            generateSStable(store,Integer.toString(table));
        }
        SSTableStats stats = antiCompactRanges(store, NO_RANGES, range(0, 4));
        /*
        Anticompaction will be anti-compacting 10 SSTables but will be doing this two at a time
        so there will be no net change in the number of sstables
         */
        assertEquals(10, stats.numLiveSSTables);
        assertEquals(stats.pendingKeys, 0);
        assertEquals(stats.transKeys, 40);
        assertEquals(stats.unrepairedKeys, 60);
    }

    @Test
    public void antiCompactTenMixed() throws InterruptedException, IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.disableAutoCompaction();

        for (int table = 0; table < 10; table++)
        {
            generateSStable(store,Integer.toString(table));
        }
        SSTableStats stats = antiCompactRanges(store, range(0, 4), range(4, 8));
        assertEquals(15, stats.numLiveSSTables);
        assertEquals(stats.pendingKeys, 40);
        assertEquals(stats.transKeys, 40);
        assertEquals(stats.unrepairedKeys, 20);
    }

    @Test
    public void shouldMutatePendingRepair() throws InterruptedException, IOException
    {
        ColumnFamilyStore store = prepareColumnFamilyStore();
        Collection<SSTableReader> sstables = getUnrepairedSSTables(store);
        assertEquals(store.getLiveSSTables().size(), sstables.size());
        // the sstables start at "0".getBytes() = 48, we need to include that first token, with "/".getBytes() = 47
        Range<Token> range = new Range<Token>(new BytesToken("/".getBytes()), new BytesToken("9999".getBytes()));
        List<Range<Token>> ranges = Arrays.asList(range);
        UUID pendingRepair = UUID.randomUUID();
        registerParentRepairSession(pendingRepair, ranges, UNREPAIRED_SSTABLE, pendingRepair);

        try (LifecycleTransaction txn = store.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            CompactionManager.instance.performAnticompaction(store, ranges, Collections.emptyList(), refs, txn, pendingRepair);
        }

        assertThat(store.getLiveSSTables().size(), is(1));
        assertThat(Iterables.get(store.getLiveSSTables(), 0).isRepaired(), is(false));
        assertThat(Iterables.get(store.getLiveSSTables(), 0).isPendingRepair(), is(true));
        assertThat(Iterables.get(store.getLiveSSTables(), 0).selfRef().globalCount(), is(1));
        assertThat(store.getTracker().getCompacting().size(), is(0));
    }

    @Test
    public void shouldSkipAntiCompactionForNonIntersectingRange() throws InterruptedException, IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.disableAutoCompaction();

        for (int table = 0; table < 10; table++)
        {
            generateSStable(store,Integer.toString(table));
        }
        int refCountBefore = Iterables.get(store.getLiveSSTables(), 0).selfRef().globalCount();
        Collection<SSTableReader> sstables = getUnrepairedSSTables(store);
        assertEquals(store.getLiveSSTables().size(), sstables.size());

        Range<Token> range = new Range<Token>(new BytesToken("-1".getBytes()), new BytesToken("-10".getBytes()));
        List<Range<Token>> ranges = Arrays.asList(range);
        UUID parentRepairSession = UUID.randomUUID();
        registerParentRepairSession(parentRepairSession, ranges, UNREPAIRED_SSTABLE, null);
        boolean gotException = false;
        try (LifecycleTransaction txn = store.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            CompactionManager.instance.performAnticompaction(store, ranges, Collections.emptyList(), refs, txn, parentRepairSession);
        }
        catch (IllegalStateException e)
        {
            gotException = true;
        }

        assertTrue(gotException);
        assertThat(store.getLiveSSTables().size(), is(10));
        assertThat(Iterables.get(store.getLiveSSTables(), 0).isRepaired(), is(false));
        assertEquals(refCountBefore, Iterables.get(store.getLiveSSTables(), 0).selfRef().globalCount());
    }

    private ColumnFamilyStore prepareColumnFamilyStore()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.disableAutoCompaction();
        for (int i = 0; i < 10; i++)
        {
            new RowUpdateBuilder(metadata, System.currentTimeMillis(), Integer.toString(i))
                .clustering("c")
                .add("val", "val")
                .build()
                .applyUnsafe();
        }
        store.forceBlockingFlush();
        return store;
    }

    @After
    public void truncateCfs()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.truncateBlocking();
    }

    private static Set<SSTableReader> getUnrepairedSSTables(ColumnFamilyStore cfs)
    {
        return ImmutableSet.copyOf(cfs.getTracker().getView().sstables(SSTableSet.LIVE, (s) -> !s.isRepaired()));
    }

    /**
     * If the parent repair session is missing, we should still clean up
     */
    @Test
    public void missingParentRepairSession() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.disableAutoCompaction();

        for (int table = 0; table < 10; table++)
        {
            generateSStable(store,Integer.toString(table));
        }
        Collection<SSTableReader> sstables = getUnrepairedSSTables(store);
        assertEquals(10, sstables.size());

        Range<Token> range = new Range<Token>(new BytesToken("-1".getBytes()), new BytesToken("-10".getBytes()));
        List<Range<Token>> ranges = Arrays.asList(range);

        UUID missingRepairSession = UUIDGen.getTimeUUID();
        try (LifecycleTransaction txn = store.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            Assert.assertFalse(refs.isEmpty());
            try
            {
                CompactionManager.instance.performAnticompaction(store, ranges, Collections.emptyList(), refs, txn, missingRepairSession);
                Assert.fail("expected RuntimeException");
            }
            catch (RuntimeException e)
            {
                // expected
            }
            Assert.assertEquals(Transactional.AbstractTransactional.State.ABORTED, txn.state());
            Assert.assertTrue(refs.isEmpty());
        }
    }

    @Test
    public void testSSTablesToInclude()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS("anticomp");
        List<SSTableReader> sstables = new ArrayList<>();
        sstables.add(MockSchema.sstable(1, 10, 100, cfs));
        sstables.add(MockSchema.sstable(2, 100, 200, cfs));

        Range<Token> r = new Range<>(t(10), t(100)); // should include sstable 1 and 2 above, but none is fully contained (Range is (x, y])

        Iterator<SSTableReader> sstableIterator = sstables.iterator();
        Set<SSTableReader> fullyContainedSSTables = CompactionManager.findSSTablesToAnticompact(sstableIterator, Collections.singletonList(r), UUID.randomUUID());
        assertTrue(fullyContainedSSTables.isEmpty());
        assertEquals(2, sstables.size());
    }

    @Test
    public void testSSTablesToInclude2()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS("anticomp");
        List<SSTableReader> sstables = new ArrayList<>();
        SSTableReader sstable1 = MockSchema.sstable(1, 10, 100, cfs);
        SSTableReader sstable2 = MockSchema.sstable(2, 100, 200, cfs);
        sstables.add(sstable1);
        sstables.add(sstable2);

        Range<Token> r = new Range<>(t(9), t(100)); // sstable 1 is fully contained

        Iterator<SSTableReader> sstableIterator = sstables.iterator();
        Set<SSTableReader> fullyContainedSSTables = CompactionManager.findSSTablesToAnticompact(sstableIterator, Collections.singletonList(r), UUID.randomUUID());
        assertEquals(Collections.singleton(sstable1), fullyContainedSSTables);
        assertEquals(Collections.singletonList(sstable2), sstables);
    }

    @Test(expected = IllegalStateException.class)
    public void testSSTablesToNotInclude()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS("anticomp");
        List<SSTableReader> sstables = new ArrayList<>();
        SSTableReader sstable1 = MockSchema.sstable(1, 0, 5, cfs);
        sstables.add(sstable1);

        Range<Token> r = new Range<>(t(9), t(100)); // sstable is not intersecting and should not be included

        Iterator<SSTableReader> sstableIterator = sstables.iterator();
        CompactionManager.findSSTablesToAnticompact(sstableIterator, Collections.singletonList(r), UUID.randomUUID());
    }

    @Test(expected = IllegalStateException.class)
    public void testSSTablesToNotInclude2()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS("anticomp");
        List<SSTableReader> sstables = new ArrayList<>();
        SSTableReader sstable1 = MockSchema.sstable(1, 10, 10, cfs);
        SSTableReader sstable2 = MockSchema.sstable(2, 100, 200, cfs);
        sstables.add(sstable1);
        sstables.add(sstable2);

        Range<Token> r = new Range<>(t(10), t(11)); // no sstable included, throw

        Iterator<SSTableReader> sstableIterator = sstables.iterator();
        CompactionManager.findSSTablesToAnticompact(sstableIterator, Collections.singletonList(r), UUID.randomUUID());
    }

    @Test
    public void testSSTablesToInclude4()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS("anticomp");
        List<SSTableReader> sstables = new ArrayList<>();
        SSTableReader sstable1 = MockSchema.sstable(1, 10, 100, cfs);
        SSTableReader sstable2 = MockSchema.sstable(2, 100, 200, cfs);
        sstables.add(sstable1);
        sstables.add(sstable2);

        Range<Token> r = new Range<>(t(9), t(200)); // sstable 2 is fully contained - last token is equal

        Iterator<SSTableReader> sstableIterator = sstables.iterator();
        Set<SSTableReader> fullyContainedSSTables = CompactionManager.findSSTablesToAnticompact(sstableIterator, Collections.singletonList(r), UUID.randomUUID());
        assertEquals(Sets.newHashSet(sstable1, sstable2), fullyContainedSSTables);
        assertTrue(sstables.isEmpty());
    }

    private Token t(long t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }
}

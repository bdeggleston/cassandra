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

package org.apache.cassandra.service.accord;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.ConfigurationService.EpochReady;
import accord.impl.AbstractConfigurationServiceTest;
import accord.local.Node.Id;
import accord.topology.Shard;
import accord.topology.Topology;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordKeyspace.EpochDiskState;

import static accord.impl.AbstractConfigurationServiceTest.TestListener;
import static com.google.common.collect.ImmutableSet.of;
import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.schema.SchemaConstants.ACCORD_KEYSPACE_NAME;
import static org.apache.cassandra.service.accord.AccordKeyspace.EPOCH_METADATA;
import static org.apache.cassandra.service.accord.AccordKeyspace.TOPOLOGIES;
import static org.apache.cassandra.service.accord.AccordKeyspace.loadEpoch;

public class AccordConfigurationServiceTest
{
    private static final Id ID1 = new Id(1);
    private static final Id ID2 = new Id(2);
    private static final Id ID3 = new Id(3);
    private static final List<Id> ID_LIST = ImmutableList.of(ID1, ID2, ID3);
    private static final Set<Id> ID_SET = ImmutableSet.copyOf(ID_LIST);
    private static final TableId TBL1 = TableId.fromUUID(new UUID(0, 1));
    private static final TableId TBL2 = TableId.fromUUID(new UUID(0, 2));

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.daemonInitialization();
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c))", "ks"));
    }

    @Before
    public void setup()
    {
        Keyspace.open(ACCORD_KEYSPACE_NAME).getColumnFamilyStore(TOPOLOGIES).truncateBlocking();
        Keyspace.open(ACCORD_KEYSPACE_NAME).getColumnFamilyStore(EPOCH_METADATA).truncateBlocking();
    }

    @Test
    public void initialEpochTest() throws Throwable
    {
        AccordConfigurationService service = new AccordConfigurationService(ID1);
        Assert.assertEquals(null, AccordKeyspace.loadEpochDiskState());
        service.start();
        Assert.assertEquals(null, AccordKeyspace.loadEpochDiskState());
        Assert.assertTrue(executeInternal(format("SELECT * FROM %s.%s WHERE epoch=1", ACCORD_KEYSPACE_NAME, TOPOLOGIES)).isEmpty());

        Topology topology1 = new Topology(1, new Shard(AccordTopologyUtils.fullRange("ks"), ID_LIST, ID_SET));
        service.reportTopology(topology1);
        loadEpoch(1, (epoch, topology, synced) -> {
            Assert.assertEquals(topology1, topology);
            Assert.assertTrue(synced.isEmpty());
        });
        Assert.assertEquals(new EpochDiskState(1, 1), service.diskState());

        service.epochSyncComplete(ID1, 1);
        service.epochSyncComplete(ID2, 1);
        loadEpoch(1, (epoch, topology, synced) -> {
            Assert.assertEquals(topology1, topology);
            Assert.assertEquals(Sets.newHashSet(ID1, ID2), synced);
        });
    }

    @Test
    public void loadTest() throws Throwable
    {
        AccordConfigurationService service = new AccordConfigurationService(ID1);
        service.start();

        Topology topology1 = new Topology(1, new Shard(AccordTopologyUtils.fullRange("ks"), ID_LIST, ID_SET));
        service.reportTopology(topology1);
        service.acknowledgeEpoch(EpochReady.done(1));
        service.epochSyncComplete(ID1, 1);
        service.epochSyncComplete(ID2, 1);
        service.epochSyncComplete(ID3, 1);

        Topology topology2 = new Topology(2, new Shard(AccordTopologyUtils.fullRange("ks"), ID_LIST, of(ID1, ID2)));
        service.reportTopology(topology2);
        service.acknowledgeEpoch(EpochReady.done(2));
        service.epochSyncComplete(ID1, 2);

        Topology topology3 = new Topology(3, new Shard(AccordTopologyUtils.fullRange("ks"), ID_LIST, of(ID1, ID2)));
        service.reportTopology(topology3);
        service.acknowledgeEpoch(EpochReady.done(3));

        AccordConfigurationService loaded = new AccordConfigurationService(ID1);
        AbstractConfigurationServiceTest.TestListener listener = new AbstractConfigurationServiceTest.TestListener(loaded, true);
        loaded.registerListener(listener);
        loaded.start();

        listener.assertNoTruncates();
        listener.assertTopologiesFor(1L, 2L, 3L);
        listener.assertTopologyForEpoch(1, topology1);
        listener.assertTopologyForEpoch(2, topology2);
        listener.assertTopologyForEpoch(3, topology3);
//        listener.assertSyncsFor(1L, 2L);
        listener.assertSyncsFor(1L, 2L, 3L); // replace w/ line above once epoch sync is working
        listener.assertSyncsForEpoch(1, ID1, ID2, ID3);
//        listener.assertSyncsForEpoch(2, ID1);
        listener.assertSyncsForEpoch(2, ID1, ID2, ID3); // replace w/ line above once epoch sync is working
    }

    @Test
    public void truncateTest()
    {
        AccordConfigurationService service = new AccordConfigurationService(ID1);
        TestListener serviceListener = new TestListener(service, true);
        service.registerListener(serviceListener);
        service.start();

        Topology topology1 = new Topology(1, new Shard(AccordTopologyUtils.fullRange("ks"), ID_LIST, ID_SET));
        service.reportTopology(topology1);

        Topology topology2 = new Topology(2, new Shard(AccordTopologyUtils.fullRange("ks"), ID_LIST, of(ID1, ID2)));
        service.reportTopology(topology2);

        Topology topology3 = new Topology(3, new Shard(AccordTopologyUtils.fullRange("ks"), ID_LIST, of(ID1, ID2)));
        service.reportTopology(topology3);
        service.truncateTopologiesUntil(3);
        Assert.assertEquals(new EpochDiskState(3, 3), service.diskState());
        serviceListener.assertTruncates(3L);

        AccordConfigurationService loaded = new AccordConfigurationService(ID1);
        TestListener loadListener = new TestListener(loaded, true);
        loaded.registerListener(loadListener);
        loaded.start();
        loadListener.assertTopologiesFor(3L);
    }
}

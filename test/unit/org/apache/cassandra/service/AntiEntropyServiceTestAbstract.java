/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.KeyspaceManager;
import org.apache.cassandra.locator.LocatorConfig;
import org.apache.cassandra.repair.RepairJobDesc;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.net.MessagingService;

import static org.junit.Assert.assertEquals;

public abstract class AntiEntropyServiceTestAbstract
{
    // keyspace and column family to test against
    public ActiveRepairService aes;

    public String keyspaceName;
    public String cfname;
    public RepairJobDesc desc;
    public ColumnFamilyStore store;
    public InetAddress LOCAL, REMOTE;

    public Range<Token> local_range;

    private boolean initialized;

    public abstract void init();

    public abstract List<IMutation> getWriteData();

    public static final String KEYSPACE5 = "Keyspace5";
    public static final String CF_STANDRAD1 = "Standard1";
    public static final String CF_COUNTER = "Counter1";

    public static final DatabaseDescriptor databaseDescriptor = SchemaLoader.databaseDescriptor;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE5,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(2),
                                    SchemaLoader.standardCFMD(KEYSPACE5, CF_COUNTER),
                                    SchemaLoader.standardCFMD(KEYSPACE5, CF_STANDRAD1));
    }

    @Before
    public void prepare() throws Exception
    {
        if (!initialized)
        {
            SchemaLoader.startGossiper();
            initialized = true;

            init();

            LOCAL = DatabaseDescriptor.createMain(false, false).getBroadcastAddress();
            // generate a fake endpoint for which we can spoof receiving/sending trees
            REMOTE = InetAddress.getByName("127.0.0.2");
            store = null;
            for (ColumnFamilyStore cfs : databaseDescriptor.getKeyspaceManager().open(keyspaceName).getColumnFamilyStores())
            {
                if (cfs.name.equals(cfname))
                {
                    store = cfs;
                    break;
                }
            }
            assert store != null : "CF not found: " + cfname;
        }

        aes = databaseDescriptor.getActiveRepairService();
        TokenMetadata tmd = databaseDescriptor.getLocatorConfig().getTokenMetadata();
        tmd.clearUnsafe();
        databaseDescriptor.getStorageService().setTokens(Collections.singleton(databaseDescriptor.getLocatorConfig().getPartitioner().getRandomToken()));
        tmd.updateNormalToken(databaseDescriptor.getLocatorConfig().getPartitioner().getMinimumToken(), REMOTE);
        assert tmd.isMember(REMOTE);

        databaseDescriptor.getMessagingService().setVersion(REMOTE, MessagingService.current_version);
        databaseDescriptor.getGossiper().initializeNodeUnsafe(REMOTE, UUID.randomUUID(), 1);

        local_range = databaseDescriptor.getLocatorConfig().getPrimaryRangesForEndpoint(keyspaceName, LOCAL).iterator().next();

        desc = new RepairJobDesc(UUID.randomUUID(), UUID.randomUUID(), keyspaceName, cfname, local_range);
        // Set a fake session corresponding to this fake request
        databaseDescriptor.getActiveRepairService().submitArtificialRepairSession(desc);
    }

    @After
    public void teardown() throws Exception
    {
        flushAES();
    }

    @Test
    public void testGetNeighborsPlusOne() throws Throwable
    {
        // generate rf+1 nodes, and ensure that all nodes are returned
        Set<InetAddress> expected = addTokens(1 + databaseDescriptor.getKeyspaceManager().open(keyspaceName).getReplicationStrategy().getReplicationFactor());
        expected.remove(DatabaseDescriptor.createMain(false, false).getBroadcastAddress());
        Collection<Range<Token>> ranges = databaseDescriptor.getLocatorConfig().getLocalRanges(keyspaceName);
        Set<InetAddress> neighbors = new HashSet<InetAddress>();
        for (Range<Token> range : ranges)
        {
            neighbors.addAll(databaseDescriptor.getActiveRepairService().getNeighbors(keyspaceName, range, null, null));
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsTimesTwo() throws Throwable
    {
        TokenMetadata tmd = databaseDescriptor.getLocatorConfig().getTokenMetadata();

        // generate rf*2 nodes, and ensure that only neighbors specified by the ARS are returned
        addTokens(2 * databaseDescriptor.getKeyspaceManager().open(keyspaceName).getReplicationStrategy().getReplicationFactor());
        AbstractReplicationStrategy ars = databaseDescriptor.getKeyspaceManager().open(keyspaceName).getReplicationStrategy();
        Set<InetAddress> expected = new HashSet<InetAddress>();
        for (Range<Token> replicaRange : ars.getAddressRanges().get(DatabaseDescriptor.createMain(false, false).getBroadcastAddress()))
        {
            expected.addAll(ars.getRangeAddresses(tmd.cloneOnlyTokenMap()).get(replicaRange));
        }
        expected.remove(DatabaseDescriptor.createMain(false, false).getBroadcastAddress());
        Collection<Range<Token>> ranges = databaseDescriptor.getLocatorConfig().getLocalRanges(keyspaceName);
        Set<InetAddress> neighbors = new HashSet<InetAddress>();
        for (Range<Token> range : ranges)
        {
            neighbors.addAll(databaseDescriptor.getActiveRepairService().getNeighbors(keyspaceName, range, null, null));
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsPlusOneInLocalDC() throws Throwable
    {
        TokenMetadata tmd = databaseDescriptor.getLocatorConfig().getTokenMetadata();
        
        // generate rf+1 nodes, and ensure that all nodes are returned
        Set<InetAddress> expected = addTokens(1 + databaseDescriptor.getKeyspaceManager().open(keyspaceName).getReplicationStrategy().getReplicationFactor());
        expected.remove(DatabaseDescriptor.createMain(false, false).getBroadcastAddress());
        // remove remote endpoints
        TokenMetadata.Topology topology = tmd.cloneOnlyTokenMap().getTopology();
        HashSet<InetAddress> localEndpoints = Sets.newHashSet(topology.getDatacenterEndpoints().get(DatabaseDescriptor.createMain(false, false).getLocalDataCenter()));
        expected = Sets.intersection(expected, localEndpoints);

        Collection<Range<Token>> ranges = databaseDescriptor.getLocatorConfig().getLocalRanges(keyspaceName);
        Set<InetAddress> neighbors = new HashSet<InetAddress>();
        for (Range<Token> range : ranges)
        {
            neighbors.addAll(databaseDescriptor.getActiveRepairService().getNeighbors(keyspaceName, range, Arrays.asList(DatabaseDescriptor.createMain(false, false).getLocalDataCenter()), null));
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsTimesTwoInLocalDC() throws Throwable
    {
        TokenMetadata tmd = databaseDescriptor.getLocatorConfig().getTokenMetadata();

        // generate rf*2 nodes, and ensure that only neighbors specified by the ARS are returned
        addTokens(2 * databaseDescriptor.getKeyspaceManager().open(keyspaceName).getReplicationStrategy().getReplicationFactor());
        AbstractReplicationStrategy ars = databaseDescriptor.getKeyspaceManager().open(keyspaceName).getReplicationStrategy();
        Set<InetAddress> expected = new HashSet<InetAddress>();
        for (Range<Token> replicaRange : ars.getAddressRanges().get(DatabaseDescriptor.createMain(false, false).getBroadcastAddress()))
        {
            expected.addAll(ars.getRangeAddresses(tmd.cloneOnlyTokenMap()).get(replicaRange));
        }
        expected.remove(DatabaseDescriptor.createMain(false, false).getBroadcastAddress());
        // remove remote endpoints
        TokenMetadata.Topology topology = tmd.cloneOnlyTokenMap().getTopology();
        HashSet<InetAddress> localEndpoints = Sets.newHashSet(topology.getDatacenterEndpoints().get(DatabaseDescriptor.createMain(false, false).getLocalDataCenter()));
        expected = Sets.intersection(expected, localEndpoints);
        
        Collection<Range<Token>> ranges = databaseDescriptor.getLocatorConfig().getLocalRanges(keyspaceName);
        Set<InetAddress> neighbors = new HashSet<InetAddress>();
        for (Range<Token> range : ranges)
        {
            neighbors.addAll(databaseDescriptor.getActiveRepairService().getNeighbors(keyspaceName, range, Arrays.asList(DatabaseDescriptor.createMain(false, false).getLocalDataCenter()), null));
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsTimesTwoInSpecifiedHosts() throws Throwable
    {
        TokenMetadata tmd = databaseDescriptor.getLocatorConfig().getTokenMetadata();

        // generate rf*2 nodes, and ensure that only neighbors specified by the hosts are returned
        addTokens(2 * databaseDescriptor.getKeyspaceManager().open(keyspaceName).getReplicationStrategy().getReplicationFactor());
        AbstractReplicationStrategy ars = databaseDescriptor.getKeyspaceManager().open(keyspaceName).getReplicationStrategy();
        List<InetAddress> expected = new ArrayList<>();
        for (Range<Token> replicaRange : ars.getAddressRanges().get(DatabaseDescriptor.createMain(false, false).getBroadcastAddress()))
        {
            expected.addAll(ars.getRangeAddresses(tmd.cloneOnlyTokenMap()).get(replicaRange));
        }

        expected.remove(DatabaseDescriptor.createMain(false, false).getBroadcastAddress());
        Collection<String> hosts = Arrays.asList(DatabaseDescriptor.createMain(false, false).getBroadcastAddress().getCanonicalHostName(),expected.get(0).getCanonicalHostName());

       assertEquals(expected.get(0), databaseDescriptor.getActiveRepairService().getNeighbors(keyspaceName, databaseDescriptor.getLocatorConfig().getLocalRanges(keyspaceName).iterator().next(), null, hosts).iterator().next());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNeighborsSpecifiedHostsWithNoLocalHost() throws Throwable
    {
        addTokens(2 * databaseDescriptor.getKeyspaceManager().open(keyspaceName).getReplicationStrategy().getReplicationFactor());
        //Dont give local endpoint
        Collection<String> hosts = Arrays.asList("127.0.0.3");
        databaseDescriptor.getActiveRepairService().getNeighbors(keyspaceName, databaseDescriptor.getLocatorConfig().getLocalRanges(keyspaceName).iterator().next(), null, hosts);
    }

    Set<InetAddress> addTokens(int max) throws Throwable
    {
        TokenMetadata tmd = databaseDescriptor.getLocatorConfig().getTokenMetadata();
        Set<InetAddress> endpoints = new HashSet<InetAddress>();
        for (int i = 1; i <= max; i++)
        {
            InetAddress endpoint = InetAddress.getByName("127.0.0." + i);
            tmd.updateNormalToken(databaseDescriptor.getLocatorConfig().getPartitioner().getRandomToken(), endpoint);
            endpoints.add(endpoint);
        }
        return endpoints;
    }

    void flushAES() throws Exception
    {
        final ExecutorService stage = databaseDescriptor.getStageManager().getStage(Stage.ANTI_ENTROPY);
        final Callable noop = new Callable<Object>()
        {
            public Boolean call()
            {
                return true;
            }
        };

        // send two tasks through the stage: one to follow existing tasks and a second to follow tasks created by
        // those existing tasks: tasks won't recursively create more tasks
        stage.submit(noop).get(5000, TimeUnit.MILLISECONDS);
        stage.submit(noop).get(5000, TimeUnit.MILLISECONDS);
    }
}

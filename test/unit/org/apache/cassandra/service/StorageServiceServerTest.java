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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.util.*;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.KeyspaceManager;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.LocatorConfig;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.StringToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.PropertyFileSnitch;
import org.apache.cassandra.locator.TokenMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class StorageServiceServerTest
{

    public static final DatabaseDescriptor databaseDescriptor = DatabaseDescriptor.instance;

    public static class ConfigLoader extends YamlConfigurationLoader
    {
        @Override
        public Config loadConfig(URL url) throws ConfigurationException
        {
            Config conf = super.loadConfig(url);
            conf.endpoint_snitch = PropertyFileSnitch.class.getName();
            return conf;
        }
    }

    @BeforeClass
    public static void setUp() throws ConfigurationException
    {
        System.setProperty("cassandra.config.loader", ConfigLoader.class.getName());
        DatabaseDescriptor.init();
//        IEndpointSnitch snitch = new PropertyFileSnitch();
//        DatabaseDescriptor.instance.setEndpointSnitch(snitch);
        KeyspaceManager.instance.setInitialized();
//        snitch.gossiperStarting();
    }

    @Test
    public void testRegularMode() throws ConfigurationException
    {
        SchemaLoader.mkdirs();
        SchemaLoader.cleanup();
        StorageService.instance.initServer(0);
        for (String path : DatabaseDescriptor.instance.getAllDataFileLocations())
        {
            // verify that storage directories are there.
            assertTrue(new File(path).exists());
        }
        // a proper test would be to call decommission here, but decommission() mixes both shutdown and datatransfer
        // calls.  This test is only interested in the shutdown-related items which a properly handled by just
        // stopping the client.
        //StorageService.instance.decommission();
        StorageService.instance.stopClient();
    }

    @Test
    public void testGetAllRangesEmpty()
    {
        List<Token> toks = Collections.emptyList();
        assertEquals(Collections.emptyList(), StorageService.instance.getAllRanges(toks));
    }

    @Test
    public void testSnapshot() throws IOException
    {
        // no need to insert extra data, even an "empty" database will have a little information in the system keyspace
        StorageService.instance.takeSnapshot("snapshot", new String[0]);
    }

    @Test
    public void testColumnFamilySnapshot() throws IOException
    {
        // no need to insert extra data, even an "empty" database will have a little information in the system keyspace
        StorageService.instance.takeColumnFamilySnapshot(Keyspace.SYSTEM_KS, SystemKeyspace.SCHEMA_KEYSPACES_CF, "cf_snapshot");
    }

    @Test
    public void testPrimaryRangesWithNetworkTopologyStrategy() throws Exception
    {
        TokenMetadata metadata = LocatorConfig.instance.getTokenMetadata();
        metadata.clearUnsafe();
        // DC1
        metadata.updateNormalToken(new StringToken("A", LocatorConfig.instance.getPartitioner()), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("C", LocatorConfig.instance.getPartitioner()), InetAddress.getByName("127.0.0.2"));
        // DC2
        metadata.updateNormalToken(new StringToken("B", LocatorConfig.instance.getPartitioner()), InetAddress.getByName("127.0.0.4"));
        metadata.updateNormalToken(new StringToken("D", LocatorConfig.instance.getPartitioner()), InetAddress.getByName("127.0.0.5"));

        Map<String, String> configOptions = new HashMap<String, String>();
        configOptions.put("DC1", "1");
        configOptions.put("DC2", "1");

        Keyspace.clear("Keyspace1", databaseDescriptor.getSchema());
        KSMetaData meta = KSMetaDataFactory.instance.newKeyspace("Keyspace1", "NetworkTopologyStrategy", configOptions, false);
        databaseDescriptor.getSchema().setKeyspaceDefinition(meta);

        Collection<Range<Token>> primaryRanges = LocatorConfig.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.1"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("D", LocatorConfig.instance.getPartitioner()), new StringToken("A", LocatorConfig.instance.getPartitioner()), LocatorConfig.instance.getPartitioner()));

        primaryRanges = LocatorConfig.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.2"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("B", LocatorConfig.instance.getPartitioner()), new StringToken("C", LocatorConfig.instance.getPartitioner()), LocatorConfig.instance.getPartitioner()));

        primaryRanges = LocatorConfig.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.4"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("A", LocatorConfig.instance.getPartitioner()), new StringToken("B", LocatorConfig.instance.getPartitioner()), LocatorConfig.instance.getPartitioner()));

        primaryRanges = LocatorConfig.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.5"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("C", LocatorConfig.instance.getPartitioner()), new StringToken("D", LocatorConfig.instance.getPartitioner()), LocatorConfig.instance.getPartitioner()));
    }

    @Test
    public void testPrimaryRangesWithNetworkTopologyStrategyOneDCOnly() throws Exception
    {
        TokenMetadata metadata = LocatorConfig.instance.getTokenMetadata();
        metadata.clearUnsafe();
        // DC1
        metadata.updateNormalToken(new StringToken("A", LocatorConfig.instance.getPartitioner()), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("C", LocatorConfig.instance.getPartitioner()), InetAddress.getByName("127.0.0.2"));
        // DC2
        metadata.updateNormalToken(new StringToken("B", LocatorConfig.instance.getPartitioner()), InetAddress.getByName("127.0.0.4"));
        metadata.updateNormalToken(new StringToken("D", LocatorConfig.instance.getPartitioner()), InetAddress.getByName("127.0.0.5"));

        Map<String, String> configOptions = new HashMap<String, String>();
        configOptions.put("DC2", "2");

        Keyspace.clear("Keyspace1", databaseDescriptor.getSchema());
        KSMetaData meta = KSMetaDataFactory.instance.newKeyspace("Keyspace1", "NetworkTopologyStrategy", configOptions, false);
        databaseDescriptor.getSchema().setKeyspaceDefinition(meta);

        // endpoints in DC1 should not have primary range
        Collection<Range<Token>> primaryRanges = LocatorConfig.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.1"));
        assert primaryRanges.isEmpty();

        primaryRanges = LocatorConfig.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.2"));
        assert primaryRanges.isEmpty();

        // endpoints in DC2 should have primary ranges which also cover DC1
        primaryRanges = LocatorConfig.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.4"));
        assert primaryRanges.size() == 2;
        assert primaryRanges.contains(new Range<Token>(new StringToken("D", LocatorConfig.instance.getPartitioner()), new StringToken("A", LocatorConfig.instance.getPartitioner()), LocatorConfig.instance.getPartitioner()));
        assert primaryRanges.contains(new Range<Token>(new StringToken("A", LocatorConfig.instance.getPartitioner()), new StringToken("B", LocatorConfig.instance.getPartitioner()), LocatorConfig.instance.getPartitioner()));

        primaryRanges = LocatorConfig.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.5"));
        assert primaryRanges.size() == 2;
        assert primaryRanges.contains(new Range<Token>(new StringToken("C", LocatorConfig.instance.getPartitioner()), new StringToken("D", LocatorConfig.instance.getPartitioner()), LocatorConfig.instance.getPartitioner()));
        assert primaryRanges.contains(new Range<Token>(new StringToken("B", LocatorConfig.instance.getPartitioner()), new StringToken("C", LocatorConfig.instance.getPartitioner()), LocatorConfig.instance.getPartitioner()));
    }

    @Test
    public void testPrimaryRangesWithVnodes() throws Exception
    {
        TokenMetadata metadata = LocatorConfig.instance.getTokenMetadata();
        metadata.clearUnsafe();
        // DC1
        Multimap<InetAddress, Token> dc1 = HashMultimap.create();
        dc1.put(InetAddress.getByName("127.0.0.1"), new StringToken("A", LocatorConfig.instance.getPartitioner()));
        dc1.put(InetAddress.getByName("127.0.0.1"), new StringToken("E", LocatorConfig.instance.getPartitioner()));
        dc1.put(InetAddress.getByName("127.0.0.1"), new StringToken("H", LocatorConfig.instance.getPartitioner()));
        dc1.put(InetAddress.getByName("127.0.0.2"), new StringToken("C", LocatorConfig.instance.getPartitioner()));
        dc1.put(InetAddress.getByName("127.0.0.2"), new StringToken("I", LocatorConfig.instance.getPartitioner()));
        dc1.put(InetAddress.getByName("127.0.0.2"), new StringToken("J", LocatorConfig.instance.getPartitioner()));
        metadata.updateNormalTokens(dc1);
        // DC2
        Multimap<InetAddress, Token> dc2 = HashMultimap.create();
        dc2.put(InetAddress.getByName("127.0.0.4"), new StringToken("B", LocatorConfig.instance.getPartitioner()));
        dc2.put(InetAddress.getByName("127.0.0.4"), new StringToken("G", LocatorConfig.instance.getPartitioner()));
        dc2.put(InetAddress.getByName("127.0.0.4"), new StringToken("L", LocatorConfig.instance.getPartitioner()));
        dc2.put(InetAddress.getByName("127.0.0.5"), new StringToken("D", LocatorConfig.instance.getPartitioner()));
        dc2.put(InetAddress.getByName("127.0.0.5"), new StringToken("F", LocatorConfig.instance.getPartitioner()));
        dc2.put(InetAddress.getByName("127.0.0.5"), new StringToken("K", LocatorConfig.instance.getPartitioner()));
        metadata.updateNormalTokens(dc2);

        Map<String, String> configOptions = new HashMap<String, String>();
        configOptions.put("DC2", "2");

        Keyspace.clear("Keyspace1", databaseDescriptor.getSchema());
        KSMetaData meta = KSMetaDataFactory.instance.newKeyspace("Keyspace1", "NetworkTopologyStrategy", configOptions, false);
        databaseDescriptor.getSchema().setKeyspaceDefinition(meta);

        // endpoints in DC1 should not have primary range
        Collection<Range<Token>> primaryRanges = LocatorConfig.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.1"));
        assert primaryRanges.isEmpty();

        primaryRanges = LocatorConfig.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.2"));
        assert primaryRanges.isEmpty();

        // endpoints in DC2 should have primary ranges which also cover DC1
        primaryRanges = LocatorConfig.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.4"));
        assert primaryRanges.size() == 4;
        assert primaryRanges.contains(new Range<Token>(new StringToken("A", LocatorConfig.instance.getPartitioner()), new StringToken("B", LocatorConfig.instance.getPartitioner()), LocatorConfig.instance.getPartitioner()));
        assert primaryRanges.contains(new Range<Token>(new StringToken("F", LocatorConfig.instance.getPartitioner()), new StringToken("G", LocatorConfig.instance.getPartitioner()), LocatorConfig.instance.getPartitioner()));
        assert primaryRanges.contains(new Range<Token>(new StringToken("K", LocatorConfig.instance.getPartitioner()), new StringToken("L", LocatorConfig.instance.getPartitioner()), LocatorConfig.instance.getPartitioner()));
        // because /127.0.0.4 holds token "B" which is the next to token "A" from /127.0.0.1,
        // the node covers range (L, A]
        assert primaryRanges.contains(new Range<Token>(new StringToken("L", LocatorConfig.instance.getPartitioner()), new StringToken("A", LocatorConfig.instance.getPartitioner()), LocatorConfig.instance.getPartitioner()));

        primaryRanges = LocatorConfig.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.5"));
        assert primaryRanges.size() == 8;
        assert primaryRanges.contains(new Range<Token>(new StringToken("C", LocatorConfig.instance.getPartitioner()), new StringToken("D", LocatorConfig.instance.getPartitioner()), LocatorConfig.instance.getPartitioner()));
        assert primaryRanges.contains(new Range<Token>(new StringToken("E", LocatorConfig.instance.getPartitioner()), new StringToken("F", LocatorConfig.instance.getPartitioner()), LocatorConfig.instance.getPartitioner()));
        assert primaryRanges.contains(new Range<Token>(new StringToken("J", LocatorConfig.instance.getPartitioner()), new StringToken("K", LocatorConfig.instance.getPartitioner()), LocatorConfig.instance.getPartitioner()));
        // ranges from /127.0.0.1
        assert primaryRanges.contains(new Range<Token>(new StringToken("D", LocatorConfig.instance.getPartitioner()), new StringToken("E", LocatorConfig.instance.getPartitioner()), LocatorConfig.instance.getPartitioner()));
        // the next token to "H" in DC2 is "K" in /127.0.0.5, so (G, H] goes to /127.0.0.5
        assert primaryRanges.contains(new Range<Token>(new StringToken("G", LocatorConfig.instance.getPartitioner()), new StringToken("H", LocatorConfig.instance.getPartitioner()), LocatorConfig.instance.getPartitioner()));
        // ranges from /127.0.0.2
        assert primaryRanges.contains(new Range<Token>(new StringToken("B", LocatorConfig.instance.getPartitioner()), new StringToken("C", LocatorConfig.instance.getPartitioner()), LocatorConfig.instance.getPartitioner()));
        assert primaryRanges.contains(new Range<Token>(new StringToken("H", LocatorConfig.instance.getPartitioner()), new StringToken("I", LocatorConfig.instance.getPartitioner()), LocatorConfig.instance.getPartitioner()));
        assert primaryRanges.contains(new Range<Token>(new StringToken("I", LocatorConfig.instance.getPartitioner()), new StringToken("J", LocatorConfig.instance.getPartitioner()), LocatorConfig.instance.getPartitioner()));
    }
    @Test
    public void testPrimaryRangesWithSimpleStrategy() throws Exception
    {
        TokenMetadata metadata = LocatorConfig.instance.getTokenMetadata();
        metadata.clearUnsafe();

        metadata.updateNormalToken(new StringToken("A", LocatorConfig.instance.getPartitioner()), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("B", LocatorConfig.instance.getPartitioner()), InetAddress.getByName("127.0.0.2"));
        metadata.updateNormalToken(new StringToken("C", LocatorConfig.instance.getPartitioner()), InetAddress.getByName("127.0.0.3"));

        Map<String, String> configOptions = new HashMap<String, String>();
        configOptions.put("replication_factor", "2");

        Keyspace.clear("Keyspace1", databaseDescriptor.getSchema());
        KSMetaData meta = KSMetaDataFactory.instance.newKeyspace("Keyspace1", "SimpleStrategy", configOptions, false);
        databaseDescriptor.getSchema().setKeyspaceDefinition(meta);

        Collection<Range<Token>> primaryRanges = LocatorConfig.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.1"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("C", LocatorConfig.instance.getPartitioner()), new StringToken("A", LocatorConfig.instance.getPartitioner()), LocatorConfig.instance.getPartitioner()));

        primaryRanges = LocatorConfig.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.2"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("A", LocatorConfig.instance.getPartitioner()), new StringToken("B", LocatorConfig.instance.getPartitioner()), LocatorConfig.instance.getPartitioner()));

        primaryRanges = LocatorConfig.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.3"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("B", LocatorConfig.instance.getPartitioner()), new StringToken("C", LocatorConfig.instance.getPartitioner()), LocatorConfig.instance.getPartitioner()));
    }
}

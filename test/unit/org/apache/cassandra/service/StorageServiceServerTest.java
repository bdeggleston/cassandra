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

    public static final DatabaseDescriptor databaseDescriptor = SchemaLoader.databaseDescriptor;

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
//        IEndpointSnitch snitch = new PropertyFileSnitch();
//        DatabaseDescriptor.createMain(false, false).setEndpointSnitch(snitch);
        databaseDescriptor.getKeyspaceManager().setInitialized();
//        snitch.gossiperStarting();
    }

    @Test
    public void testRegularMode() throws ConfigurationException
    {
        SchemaLoader.mkdirs();
        SchemaLoader.cleanup();
        databaseDescriptor.getStorageService().initServer(0);
        for (String path : databaseDescriptor.getAllDataFileLocations())
        {
            // verify that storage directories are there.
            assertTrue(new File(path).exists());
        }
        // a proper test would be to call decommission here, but decommission() mixes both shutdown and datatransfer
        // calls.  This test is only interested in the shutdown-related items which a properly handled by just
        // stopping the client.
        //databaseDescriptor.getStorageService().decommission();
        databaseDescriptor.getStorageService().stopClient();
    }

    @Test
    public void testGetAllRangesEmpty()
    {
        List<Token> toks = Collections.emptyList();
        assertEquals(Collections.emptyList(), databaseDescriptor.getStorageService().getAllRanges(toks));
    }

    @Test
    public void testSnapshot() throws IOException
    {
        // no need to insert extra data, even an "empty" database will have a little information in the system keyspace
        databaseDescriptor.getStorageService().takeSnapshot("snapshot", new String[0]);
    }

    @Test
    public void testColumnFamilySnapshot() throws IOException
    {
        // no need to insert extra data, even an "empty" database will have a little information in the system keyspace
        databaseDescriptor.getStorageService().takeColumnFamilySnapshot(Keyspace.SYSTEM_KS, SystemKeyspace.SCHEMA_KEYSPACES_CF, "cf_snapshot");
    }

    @Test
    public void testPrimaryRangesWithNetworkTopologyStrategy() throws Exception
    {
        TokenMetadata metadata = databaseDescriptor.getLocatorConfig().getTokenMetadata();
        metadata.clearUnsafe();
        // DC1
        metadata.updateNormalToken(new StringToken("A", databaseDescriptor.getLocatorConfig().getPartitioner()), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("C", databaseDescriptor.getLocatorConfig().getPartitioner()), InetAddress.getByName("127.0.0.2"));
        // DC2
        metadata.updateNormalToken(new StringToken("B", databaseDescriptor.getLocatorConfig().getPartitioner()), InetAddress.getByName("127.0.0.4"));
        metadata.updateNormalToken(new StringToken("D", databaseDescriptor.getLocatorConfig().getPartitioner()), InetAddress.getByName("127.0.0.5"));

        Map<String, String> configOptions = new HashMap<String, String>();
        configOptions.put("DC1", "1");
        configOptions.put("DC2", "1");

        Keyspace.clear("Keyspace1", databaseDescriptor.getSchema());
        KSMetaData meta = databaseDescriptor.getKSMetaDataFactory().newKeyspace("Keyspace1", "NetworkTopologyStrategy", configOptions, false);
        databaseDescriptor.getSchema().setKeyspaceDefinition(meta);

        Collection<Range<Token>> primaryRanges = databaseDescriptor.getLocatorConfig().getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.1"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("D", databaseDescriptor.getLocatorConfig().getPartitioner()), new StringToken("A", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));

        primaryRanges = databaseDescriptor.getLocatorConfig().getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.2"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("B", databaseDescriptor.getLocatorConfig().getPartitioner()), new StringToken("C", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));

        primaryRanges = databaseDescriptor.getLocatorConfig().getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.4"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("A", databaseDescriptor.getLocatorConfig().getPartitioner()), new StringToken("B", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));

        primaryRanges = databaseDescriptor.getLocatorConfig().getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.5"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("C", databaseDescriptor.getLocatorConfig().getPartitioner()), new StringToken("D", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));
    }

    @Test
    public void testPrimaryRangesWithNetworkTopologyStrategyOneDCOnly() throws Exception
    {
        TokenMetadata metadata = databaseDescriptor.getLocatorConfig().getTokenMetadata();
        metadata.clearUnsafe();
        // DC1
        metadata.updateNormalToken(new StringToken("A", databaseDescriptor.getLocatorConfig().getPartitioner()), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("C", databaseDescriptor.getLocatorConfig().getPartitioner()), InetAddress.getByName("127.0.0.2"));
        // DC2
        metadata.updateNormalToken(new StringToken("B", databaseDescriptor.getLocatorConfig().getPartitioner()), InetAddress.getByName("127.0.0.4"));
        metadata.updateNormalToken(new StringToken("D", databaseDescriptor.getLocatorConfig().getPartitioner()), InetAddress.getByName("127.0.0.5"));

        Map<String, String> configOptions = new HashMap<String, String>();
        configOptions.put("DC2", "2");

        Keyspace.clear("Keyspace1", databaseDescriptor.getSchema());
        KSMetaData meta = databaseDescriptor.getKSMetaDataFactory().newKeyspace("Keyspace1", "NetworkTopologyStrategy", configOptions, false);
        databaseDescriptor.getSchema().setKeyspaceDefinition(meta);

        // endpoints in DC1 should not have primary range
        Collection<Range<Token>> primaryRanges = databaseDescriptor.getLocatorConfig().getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.1"));
        assert primaryRanges.isEmpty();

        primaryRanges = databaseDescriptor.getLocatorConfig().getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.2"));
        assert primaryRanges.isEmpty();

        // endpoints in DC2 should have primary ranges which also cover DC1
        primaryRanges = databaseDescriptor.getLocatorConfig().getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.4"));
        assert primaryRanges.size() == 2;
        assert primaryRanges.contains(new Range<Token>(new StringToken("D", databaseDescriptor.getLocatorConfig().getPartitioner()), new StringToken("A", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));
        assert primaryRanges.contains(new Range<Token>(new StringToken("A", databaseDescriptor.getLocatorConfig().getPartitioner()), new StringToken("B", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));

        primaryRanges = databaseDescriptor.getLocatorConfig().getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.5"));
        assert primaryRanges.size() == 2;
        assert primaryRanges.contains(new Range<Token>(new StringToken("C", databaseDescriptor.getLocatorConfig().getPartitioner()), new StringToken("D", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));
        assert primaryRanges.contains(new Range<Token>(new StringToken("B", databaseDescriptor.getLocatorConfig().getPartitioner()), new StringToken("C", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));
    }

    @Test
    public void testPrimaryRangesWithVnodes() throws Exception
    {
        TokenMetadata metadata = databaseDescriptor.getLocatorConfig().getTokenMetadata();
        metadata.clearUnsafe();
        // DC1
        Multimap<InetAddress, Token> dc1 = HashMultimap.create();
        dc1.put(InetAddress.getByName("127.0.0.1"), new StringToken("A", databaseDescriptor.getLocatorConfig().getPartitioner()));
        dc1.put(InetAddress.getByName("127.0.0.1"), new StringToken("E", databaseDescriptor.getLocatorConfig().getPartitioner()));
        dc1.put(InetAddress.getByName("127.0.0.1"), new StringToken("H", databaseDescriptor.getLocatorConfig().getPartitioner()));
        dc1.put(InetAddress.getByName("127.0.0.2"), new StringToken("C", databaseDescriptor.getLocatorConfig().getPartitioner()));
        dc1.put(InetAddress.getByName("127.0.0.2"), new StringToken("I", databaseDescriptor.getLocatorConfig().getPartitioner()));
        dc1.put(InetAddress.getByName("127.0.0.2"), new StringToken("J", databaseDescriptor.getLocatorConfig().getPartitioner()));
        metadata.updateNormalTokens(dc1);
        // DC2
        Multimap<InetAddress, Token> dc2 = HashMultimap.create();
        dc2.put(InetAddress.getByName("127.0.0.4"), new StringToken("B", databaseDescriptor.getLocatorConfig().getPartitioner()));
        dc2.put(InetAddress.getByName("127.0.0.4"), new StringToken("G", databaseDescriptor.getLocatorConfig().getPartitioner()));
        dc2.put(InetAddress.getByName("127.0.0.4"), new StringToken("L", databaseDescriptor.getLocatorConfig().getPartitioner()));
        dc2.put(InetAddress.getByName("127.0.0.5"), new StringToken("D", databaseDescriptor.getLocatorConfig().getPartitioner()));
        dc2.put(InetAddress.getByName("127.0.0.5"), new StringToken("F", databaseDescriptor.getLocatorConfig().getPartitioner()));
        dc2.put(InetAddress.getByName("127.0.0.5"), new StringToken("K", databaseDescriptor.getLocatorConfig().getPartitioner()));
        metadata.updateNormalTokens(dc2);

        Map<String, String> configOptions = new HashMap<String, String>();
        configOptions.put("DC2", "2");

        Keyspace.clear("Keyspace1", databaseDescriptor.getSchema());
        KSMetaData meta = databaseDescriptor.getKSMetaDataFactory().newKeyspace("Keyspace1", "NetworkTopologyStrategy", configOptions, false);
        databaseDescriptor.getSchema().setKeyspaceDefinition(meta);

        // endpoints in DC1 should not have primary range
        Collection<Range<Token>> primaryRanges = databaseDescriptor.getLocatorConfig().getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.1"));
        assert primaryRanges.isEmpty();

        primaryRanges = databaseDescriptor.getLocatorConfig().getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.2"));
        assert primaryRanges.isEmpty();

        // endpoints in DC2 should have primary ranges which also cover DC1
        primaryRanges = databaseDescriptor.getLocatorConfig().getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.4"));
        assert primaryRanges.size() == 4;
        assert primaryRanges.contains(new Range<Token>(new StringToken("A", databaseDescriptor.getLocatorConfig().getPartitioner()), new StringToken("B", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));
        assert primaryRanges.contains(new Range<Token>(new StringToken("F", databaseDescriptor.getLocatorConfig().getPartitioner()), new StringToken("G", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));
        assert primaryRanges.contains(new Range<Token>(new StringToken("K", databaseDescriptor.getLocatorConfig().getPartitioner()), new StringToken("L", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));
        // because /127.0.0.4 holds token "B" which is the next to token "A" from /127.0.0.1,
        // the node covers range (L, A]
        assert primaryRanges.contains(new Range<Token>(new StringToken("L", databaseDescriptor.getLocatorConfig().getPartitioner()), new StringToken("A", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));

        primaryRanges = databaseDescriptor.getLocatorConfig().getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.5"));
        assert primaryRanges.size() == 8;
        assert primaryRanges.contains(new Range<Token>(new StringToken("C", databaseDescriptor.getLocatorConfig().getPartitioner()), new StringToken("D", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));
        assert primaryRanges.contains(new Range<Token>(new StringToken("E", databaseDescriptor.getLocatorConfig().getPartitioner()), new StringToken("F", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));
        assert primaryRanges.contains(new Range<Token>(new StringToken("J", databaseDescriptor.getLocatorConfig().getPartitioner()), new StringToken("K", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));
        // ranges from /127.0.0.1
        assert primaryRanges.contains(new Range<Token>(new StringToken("D", databaseDescriptor.getLocatorConfig().getPartitioner()), new StringToken("E", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));
        // the next token to "H" in DC2 is "K" in /127.0.0.5, so (G, H] goes to /127.0.0.5
        assert primaryRanges.contains(new Range<Token>(new StringToken("G", databaseDescriptor.getLocatorConfig().getPartitioner()), new StringToken("H", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));
        // ranges from /127.0.0.2
        assert primaryRanges.contains(new Range<Token>(new StringToken("B", databaseDescriptor.getLocatorConfig().getPartitioner()), new StringToken("C", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));
        assert primaryRanges.contains(new Range<Token>(new StringToken("H", databaseDescriptor.getLocatorConfig().getPartitioner()), new StringToken("I", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));
        assert primaryRanges.contains(new Range<Token>(new StringToken("I", databaseDescriptor.getLocatorConfig().getPartitioner()), new StringToken("J", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));
    }
    @Test
    public void testPrimaryRangesWithSimpleStrategy() throws Exception
    {
        TokenMetadata metadata = databaseDescriptor.getLocatorConfig().getTokenMetadata();
        metadata.clearUnsafe();

        metadata.updateNormalToken(new StringToken("A", databaseDescriptor.getLocatorConfig().getPartitioner()), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("B", databaseDescriptor.getLocatorConfig().getPartitioner()), InetAddress.getByName("127.0.0.2"));
        metadata.updateNormalToken(new StringToken("C", databaseDescriptor.getLocatorConfig().getPartitioner()), InetAddress.getByName("127.0.0.3"));

        Map<String, String> configOptions = new HashMap<String, String>();
        configOptions.put("replication_factor", "2");

        Keyspace.clear("Keyspace1", databaseDescriptor.getSchema());
        KSMetaData meta = databaseDescriptor.getKSMetaDataFactory().newKeyspace("Keyspace1", "SimpleStrategy", configOptions, false);
        databaseDescriptor.getSchema().setKeyspaceDefinition(meta);

        Collection<Range<Token>> primaryRanges = databaseDescriptor.getLocatorConfig().getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.1"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("C", databaseDescriptor.getLocatorConfig().getPartitioner()), new StringToken("A", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));

        primaryRanges = databaseDescriptor.getLocatorConfig().getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.2"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("A", databaseDescriptor.getLocatorConfig().getPartitioner()), new StringToken("B", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));

        primaryRanges = databaseDescriptor.getLocatorConfig().getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.3"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("B", databaseDescriptor.getLocatorConfig().getPartitioner()), new StringToken("C", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));
    }
}

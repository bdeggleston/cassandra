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
package org.apache.cassandra.dht;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DBConfig;
import org.apache.cassandra.db.KeyspaceManager;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.LocatorConfig;
import org.apache.cassandra.streaming.StreamManager;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.TokenMetadata;

import static org.junit.Assert.*;

@RunWith(OrderedJUnit4ClassRunner.class)
public class BootStrapperTest
{
    public static final DatabaseDescriptor databaseDescriptor = DatabaseDescriptor.instance;

    @BeforeClass
    public static void setup() throws ConfigurationException
    {
        DatabaseDescriptor.init();
        SchemaLoader.startGossiper();
        SchemaLoader.prepareServer();
        SchemaLoader.schemaDefinition("BootStrapperTest");
    }

    @Test
    public void testSourceTargetComputation() throws UnknownHostException
    {
        final int[] clusterSizes = new int[] { 1, 3, 5, 10, 100};
        for (String keyspaceName : databaseDescriptor.getSchema().getNonSystemKeyspaces())
        {
            int replicationFactor = databaseDescriptor.getKeyspaceManager().open(keyspaceName).getReplicationStrategy().getReplicationFactor();
            for (int clusterSize : clusterSizes)
                if (clusterSize >= replicationFactor)
                    testSourceTargetComputation(keyspaceName, clusterSize, replicationFactor);
        }
    }

    private RangeStreamer testSourceTargetComputation(String keyspaceName, int numOldNodes, int replicationFactor) throws UnknownHostException
    {

        generateFakeEndpoints(numOldNodes);
        Token myToken = LocatorConfig.instance.getPartitioner().getRandomToken();
        InetAddress myEndpoint = InetAddress.getByName("127.0.0.1");

        TokenMetadata tmd = LocatorConfig.instance.getTokenMetadata();
        assertEquals(numOldNodes, tmd.sortedTokens().size());
        RangeStreamer s = new RangeStreamer(tmd, myEndpoint, "Bootstrap", DatabaseDescriptor.instance, databaseDescriptor.getSchema(), databaseDescriptor.getGossiper(), databaseDescriptor.getStreamManager(), databaseDescriptor.getKeyspaceManager(), databaseDescriptor.getDBConfig());
        IFailureDetector mockFailureDetector = new IFailureDetector()
        {
            public boolean isAlive(InetAddress ep)
            {
                return true;
            }

            public void interpret(InetAddress ep) { throw new UnsupportedOperationException(); }
            public void report(InetAddress ep) { throw new UnsupportedOperationException(); }
            public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener) { throw new UnsupportedOperationException(); }
            public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener) { throw new UnsupportedOperationException(); }
            public void remove(InetAddress ep) { throw new UnsupportedOperationException(); }
            public void forceConviction(InetAddress ep) { throw new UnsupportedOperationException(); }
        };
        s.addSourceFilter(new RangeStreamer.FailureDetectorSourceFilter(mockFailureDetector));
        s.addRanges(keyspaceName, databaseDescriptor.getKeyspaceManager().open(keyspaceName).getReplicationStrategy().getPendingAddressRanges(tmd, myToken, myEndpoint));

        Collection<Map.Entry<InetAddress, Collection<Range<Token>>>> toFetch = s.toFetch().get(keyspaceName);

        // Check we get get RF new ranges in total
        Set<Range<Token>> ranges = new HashSet<Range<Token>>();
        for (Map.Entry<InetAddress, Collection<Range<Token>>> e : toFetch)
            ranges.addAll(e.getValue());

        assertEquals(replicationFactor, ranges.size());

        // there isn't any point in testing the size of these collections for any specific size.  When a random partitioner
        // is used, they will vary.
        assert toFetch.iterator().next().getValue().size() > 0;
        assert !toFetch.iterator().next().getKey().equals(myEndpoint);
        return s;
    }

    private void generateFakeEndpoints(int numOldNodes) throws UnknownHostException
    {
        TokenMetadata tmd = LocatorConfig.instance.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner<?> p = LocatorConfig.instance.getPartitioner();

        for (int i = 1; i <= numOldNodes; i++)
        {
            // leave .1 for myEndpoint
            tmd.updateNormalToken(p.getRandomToken(), InetAddress.getByName("127.0.0." + (i + 1)));
        }
    }
}

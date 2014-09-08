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

package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.cassandra.db.KeyspaceManager;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.service.StorageService;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.StorageServiceAccessor;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.*;

public class SimpleStrategyTest
{
    public static final String KEYSPACE1 = "SimpleStrategyTest";

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1));
    }

    @Test
    public void tryValidKeyspace()
    {
        assert KeyspaceManager.instance.open(KEYSPACE1).getReplicationStrategy() != null;
    }

    @Test
    public void testBigIntegerEndpoints() throws UnknownHostException
    {
        List<Token> endpointTokens = new ArrayList<Token>();
        List<Token> keyTokens = new ArrayList<Token>();
        for (int i = 0; i < 5; i++) {
            endpointTokens.add(new BigIntegerToken(String.valueOf(10 * i), LocatorConfig.instance.getPartitioner()));
            keyTokens.add(new BigIntegerToken(String.valueOf(10 * i + 5), LocatorConfig.instance.getPartitioner()));
        }
        verifyGetNaturalEndpoints(endpointTokens.toArray(new Token[0]), keyTokens.toArray(new Token[0]));
    }

    @Test
    public void testStringEndpoints() throws UnknownHostException
    {
        IPartitioner partitioner = new OrderPreservingPartitioner();

        List<Token> endpointTokens = new ArrayList<Token>();
        List<Token> keyTokens = new ArrayList<Token>();
        for (int i = 0; i < 5; i++) {
            endpointTokens.add(new StringToken(String.valueOf((char)('a' + i * 2)), LocatorConfig.instance.getPartitioner()));
            keyTokens.add(partitioner.getToken(ByteBufferUtil.bytes(String.valueOf((char)('a' + i * 2 + 1)))));
        }
        verifyGetNaturalEndpoints(endpointTokens.toArray(new Token[0]), keyTokens.toArray(new Token[0]));
    }

    // given a list of endpoint tokens, and a set of key tokens falling between the endpoint tokens,
    // make sure that the Strategy picks the right endpoints for the keys.
    private void verifyGetNaturalEndpoints(Token[] endpointTokens, Token[] keyTokens) throws UnknownHostException
    {
        TokenMetadata tmd;
        AbstractReplicationStrategy strategy;
        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            tmd = new TokenMetadata(FailureDetector.instance, LocatorConfig.instance);
            strategy = getStrategy(keyspaceName, tmd);
            List<InetAddress> hosts = new ArrayList<InetAddress>();
            for (int i = 0; i < endpointTokens.length; i++)
            {
                InetAddress ep = InetAddress.getByName("127.0.0." + String.valueOf(i + 1));
                tmd.updateNormalToken(endpointTokens[i], ep);
                hosts.add(ep);
            }

            for (int i = 0; i < keyTokens.length; i++)
            {
                List<InetAddress> endpoints = strategy.getNaturalEndpoints(keyTokens[i]);
                assertEquals(strategy.getReplicationFactor(), endpoints.size());
                List<InetAddress> correctEndpoints = new ArrayList<InetAddress>();
                for (int j = 0; j < endpoints.size(); j++)
                    correctEndpoints.add(hosts.get((i + j + 1) % hosts.size()));
                assertEquals(new HashSet<InetAddress>(correctEndpoints), new HashSet<InetAddress>(endpoints));
            }
        }
    }

    @Test
    public void testGetEndpointsDuringBootstrap() throws UnknownHostException
    {
        // the token difference will be RING_SIZE * 2.
        final int RING_SIZE = 10;
        TokenMetadata tmd = new TokenMetadata(FailureDetector.instance, LocatorConfig.instance);
        TokenMetadata oldTmd = StorageServiceAccessor.setTokenMetadata(tmd, StorageService.instance);

        Token[] endpointTokens = new Token[RING_SIZE];
        Token[] keyTokens = new Token[RING_SIZE];

        for (int i = 0; i < RING_SIZE; i++)
        {
            endpointTokens[i] = new BigIntegerToken(String.valueOf(RING_SIZE * 2 * i), LocatorConfig.instance.getPartitioner());
            keyTokens[i] = new BigIntegerToken(String.valueOf(RING_SIZE * 2 * i + RING_SIZE), LocatorConfig.instance.getPartitioner());
        }

        List<InetAddress> hosts = new ArrayList<InetAddress>();
        for (int i = 0; i < endpointTokens.length; i++)
        {
            InetAddress ep = InetAddress.getByName("127.0.0." + String.valueOf(i + 1));
            tmd.updateNormalToken(endpointTokens[i], ep);
            hosts.add(ep);
        }

        // bootstrap at the end of the ring
        Token bsToken = new BigIntegerToken(String.valueOf(210), LocatorConfig.instance.getPartitioner());
        InetAddress bootstrapEndpoint = InetAddress.getByName("127.0.0.11");
        tmd.addBootstrapToken(bsToken, bootstrapEndpoint);

        AbstractReplicationStrategy strategy = null;
        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            strategy = getStrategy(keyspaceName, tmd);

            PendingRangeCalculatorService.calculatePendingRanges(strategy, keyspaceName, LocatorConfig.instance.getTokenMetadata());

            int replicationFactor = strategy.getReplicationFactor();

            for (int i = 0; i < keyTokens.length; i++)
            {
                Collection<InetAddress> endpoints = tmd.getWriteEndpoints(keyTokens[i], keyspaceName, strategy.getNaturalEndpoints(keyTokens[i]));
                assertTrue(endpoints.size() >= replicationFactor);

                for (int j = 0; j < replicationFactor; j++)
                {
                    //Check that the old nodes are definitely included
                    assertTrue(endpoints.contains(hosts.get((i + j + 1) % hosts.size())));
                }

                // bootstrapEndpoint should be in the endpoints for i in MAX-RF to MAX, but not in any earlier ep.
                if (i < RING_SIZE - replicationFactor)
                    assertFalse(endpoints.contains(bootstrapEndpoint));
                else
                    assertTrue(endpoints.contains(bootstrapEndpoint));
            }
        }

        StorageServiceAccessor.setTokenMetadata(oldTmd, StorageService.instance);
    }

    private AbstractReplicationStrategy getStrategy(String keyspaceName, TokenMetadata tmd)
    {
        KSMetaData ksmd = Schema.instance.getKSMetaData(keyspaceName);
        return AbstractReplicationStrategy.createReplicationStrategy(
                keyspaceName,
                ksmd.strategyClass,
                tmd,
                new SimpleSnitch(LocatorConfig.instance),
                ksmd.strategyOptions);
    }
}

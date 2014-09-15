package org.apache.cassandra.locator;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.KeyspaceManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OutboundTcpConnectionPool;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;

public class EC2SnitchTest
{
    private static String az;
    public static final DatabaseDescriptor databaseDescriptor = DatabaseDescriptor.instance;

    @BeforeClass
    public static void setup() throws Exception
    {
        SchemaLoader.mkdirs();
        SchemaLoader.cleanup();
        databaseDescriptor.getKeyspaceManager().setInitialized();
        databaseDescriptor.getStorageService().initServer(0);
    }

    private class TestEC2Snitch extends Ec2Snitch
    {
        public TestEC2Snitch(LocatorConfig locatorConfig1) throws IOException, ConfigurationException
        {
            super(locatorConfig1);
        }

        @Override
        String awsApiCall(String url) throws IOException, ConfigurationException
        {
            return az;
        }
    }

    @Test
    public void testRac() throws IOException, ConfigurationException
    {
        az = "us-east-1d";
        Ec2Snitch snitch = new TestEC2Snitch(databaseDescriptor.getLocatorConfig());
        InetAddress local = InetAddress.getByName("127.0.0.1");
        InetAddress nonlocal = InetAddress.getByName("127.0.0.7");

        databaseDescriptor.getGossiper().addSavedEndpoint(nonlocal);
        Map<ApplicationState,VersionedValue> stateMap = databaseDescriptor.getGossiper().getEndpointStateForEndpoint(nonlocal).getApplicationStateMap();
        stateMap.put(ApplicationState.DC, databaseDescriptor.getStorageService().valueFactory.datacenter("us-west"));
        stateMap.put(ApplicationState.RACK, databaseDescriptor.getStorageService().valueFactory.datacenter("1a"));

        assertEquals("us-west", snitch.getDatacenter(nonlocal));
        assertEquals("1a", snitch.getRack(nonlocal));

        assertEquals("us-east", snitch.getDatacenter(local));
        assertEquals("1d", snitch.getRack(local));
    }
    
    @Test
    public void testNewRegions() throws IOException, ConfigurationException
    {
        az = "us-east-2d";
        Ec2Snitch snitch = new TestEC2Snitch(databaseDescriptor.getLocatorConfig());
        InetAddress local = InetAddress.getByName("127.0.0.1");
        assertEquals("us-east-2", snitch.getDatacenter(local));
        assertEquals("2d", snitch.getRack(local));
    }

    @Test
    public void testEc2MRSnitch() throws UnknownHostException
    {
        InetAddress me = InetAddress.getByName("127.0.0.2");
        InetAddress com_ip = InetAddress.getByName("127.0.0.3");

        OutboundTcpConnectionPool pool = databaseDescriptor.getMessagingService().getConnectionPool(me);
        Assert.assertEquals(me, pool.endPoint());
        pool.reset(com_ip);
        Assert.assertEquals(com_ip, pool.endPoint());

        databaseDescriptor.getMessagingService().destroyConnectionPool(me);
        pool = databaseDescriptor.getMessagingService().getConnectionPool(me);
        Assert.assertEquals(com_ip, pool.endPoint());
    }

    @AfterClass
    public static void tearDown()
    {
        databaseDescriptor.getStorageService().stopClient();
    }
}

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
package org.apache.cassandra.locator;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.KeyspaceManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;

public class CloudstackSnitchTest
{
    private static String az;

    public static final DatabaseDescriptor databaseDescriptor = DatabaseDescriptor.createMain(false, false);

    @BeforeClass
    public static void setup() throws Exception
    {
        SchemaLoader.mkdirs();
        SchemaLoader.cleanup();
        databaseDescriptor.getKeyspaceManager().setInitialized();
        databaseDescriptor.getStorageService().initServer(0);
    }

    private class TestCloudstackSnitch extends CloudstackSnitch
    {
        public TestCloudstackSnitch(LocatorConfig locatorConfig1) throws IOException, ConfigurationException
        {
            super(locatorConfig1);
        }

        @Override
        String csMetadataEndpoint() throws ConfigurationException
        {
            return "";
        }

        @Override
        String csQueryMetadata(String endpoint) throws IOException, ConfigurationException
        {
            return az;
        }
    }

    @Test
    public void testRacks() throws IOException, ConfigurationException
    {
        az = "ch-gva-1";
        CloudstackSnitch snitch = new TestCloudstackSnitch(databaseDescriptor.getLocatorConfig());
        InetAddress local = InetAddress.getByName("127.0.0.1");
        InetAddress nonlocal = InetAddress.getByName("127.0.0.7");

        databaseDescriptor.getGossiper().addSavedEndpoint(nonlocal);
        Map<ApplicationState,VersionedValue> stateMap = databaseDescriptor.getGossiper().getEndpointStateForEndpoint(nonlocal).getApplicationStateMap();
        stateMap.put(ApplicationState.DC, databaseDescriptor.getStorageService().valueFactory.datacenter("ch-zrh"));
        stateMap.put(ApplicationState.RACK, databaseDescriptor.getStorageService().valueFactory.rack("2"));

        assertEquals("ch-zrh", snitch.getDatacenter(nonlocal));
        assertEquals("2", snitch.getRack(nonlocal));

        assertEquals("ch-gva", snitch.getDatacenter(local));
        assertEquals("1", snitch.getRack(local));

    }

    @Test
    public void testNewRegions() throws IOException, ConfigurationException
    {
        az = "ch-gva-1";
        CloudstackSnitch snitch = new TestCloudstackSnitch(databaseDescriptor.getLocatorConfig());
        InetAddress local = InetAddress.getByName("127.0.0.1");

        assertEquals("ch-gva", snitch.getDatacenter(local));
        assertEquals("1", snitch.getRack(local));
    }

    @AfterClass
    public static void tearDown()
    {
        databaseDescriptor.getStorageService().stopClient();
    }
}

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
package org.apache.cassandra.config;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.KeyspaceManager;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.SimpleStrategy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(OrderedJUnit4ClassRunner.class)
public class DatabaseDescriptorTest
{

    public static final DatabaseDescriptor databaseDescriptor = SchemaLoader.databaseDescriptor;

    public void testCFMetaDataSerialization() throws Exception
    {
        // test serialization of all defined test CFs.
        for (String keyspaceName : databaseDescriptor.getSchema().getNonSystemKeyspaces())
        {
            for (CFMetaData cfm : databaseDescriptor.getSchema().getKeyspaceMetaData(keyspaceName).values())
            {
                CFMetaData cfmDupe = databaseDescriptor.getCFMetaDataFactory().fromThrift(cfm.toThrift());
                assertNotNull(cfmDupe);
                assertEquals(cfm, cfmDupe);
            }
        }
    }

    @Test
    public void testKSMetaDataSerialization() throws ConfigurationException
    {
        for (KSMetaData ksm : databaseDescriptor.getSchema().getKeyspaceDefinitions())
        {
            if (ksm.name.equals(Keyspace.SYSTEM_KS))
                continue;
            // Not testing round-trip on the KsDef via serDe() because maps
            KSMetaData ksmDupe = databaseDescriptor.getKSMetaDataFactory().fromThrift(ksm.toThrift());
            assertNotNull(ksmDupe);
            assertEquals(ksm, ksmDupe);
        }
    }

    // this came as a result of CASSANDRA-995
    @Test
    public void testTransKsMigration() throws ConfigurationException
    {
        SchemaLoader.cleanupAndLeaveDirs();
        DatabaseDescriptor.createMain(false, false).loadSchemas();
        assertEquals(0, databaseDescriptor.getSchema().getNonSystemKeyspaces().size());

        databaseDescriptor.getGossiper().start((int)(System.currentTimeMillis() / 1000));
        databaseDescriptor.getKeyspaceManager().setInitialized();

        try
        {
            // add a few.
            DatabaseDescriptor.createMain(false, false).getMigrationManager().announceNewKeyspace(databaseDescriptor.getKSMetaDataFactory().testMetadata("ks0", SimpleStrategy.class, KSMetaData.optsWithRF(3)));
            DatabaseDescriptor.createMain(false, false).getMigrationManager().announceNewKeyspace(databaseDescriptor.getKSMetaDataFactory().testMetadata("ks1", SimpleStrategy.class, KSMetaData.optsWithRF(3)));

            assertNotNull(databaseDescriptor.getSchema().getKSMetaData("ks0"));
            assertNotNull(databaseDescriptor.getSchema().getKSMetaData("ks1"));

            databaseDescriptor.getSchema().clearKeyspaceDefinition(databaseDescriptor.getSchema().getKSMetaData("ks0"));
            databaseDescriptor.getSchema().clearKeyspaceDefinition(databaseDescriptor.getSchema().getKSMetaData("ks1"));

            assertNull(databaseDescriptor.getSchema().getKSMetaData("ks0"));
            assertNull(databaseDescriptor.getSchema().getKSMetaData("ks1"));

            DatabaseDescriptor.createMain(false, false).loadSchemas();

            assertNotNull(databaseDescriptor.getSchema().getKSMetaData("ks0"));
            assertNotNull(databaseDescriptor.getSchema().getKSMetaData("ks1"));
        }
        finally
        {
            databaseDescriptor.getGossiper().stop();
        }
    }

    @Test
    public void testConfigurationLoader() throws Exception
    {
        // By default, we should load from the yaml
        Config config = DatabaseDescriptor.loadConfig();
        assertEquals("Test Cluster", config.cluster_name);
        databaseDescriptor.getKeyspaceManager().setInitialized();

        // Now try custom loader
        ConfigurationLoader testLoader = new TestLoader();
        System.setProperty("cassandra.config.loader", testLoader.getClass().getName());

        config = DatabaseDescriptor.loadConfig();
        assertEquals("ConfigurationLoader Test", config.cluster_name);
    }

    public static class TestLoader implements ConfigurationLoader
    {
        public Config loadConfig() throws ConfigurationException
        {
            Config testConfig = new Config();
            testConfig.cluster_name = "ConfigurationLoader Test";;
            return testConfig;
        }
    }
}

/**
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.MutationFactory;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.locator.LocatorConfig;
import org.apache.cassandra.locator.SimpleStrategy;

import org.apache.cassandra.service.StorageService;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class KSMetaDataTest
{
    public static final DatabaseDescriptor databaseDescriptor = DatabaseDescriptor.instance;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.init();
    }

    @Test
    public void testToStringHasStrategyOptions() throws Exception
    {
        Map<String, String> options = new HashMap<String, String>();
        options.put("key1", "value1");
        options.put("key2", "value2");
        options.put("key3", "value3");

        KSMetaData ksMeta = new KSMetaData("test",
                                            SimpleStrategy.class,
                                            options,
                                            true,
                                            Collections.<CFMetaData>emptyList(),
                                            databaseDescriptor.getQueryProcessor(),
                                            LocatorConfig.instance,
                                            databaseDescriptor.getSystemKeyspace(),
                                            CFMetaDataFactory.instance,
                                            databaseDescriptor.getMutationFactory());

        assertTrue(ksMeta.toString().contains(options.toString()));
    }
}

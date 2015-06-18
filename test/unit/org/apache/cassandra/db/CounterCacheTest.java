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
package org.apache.cassandra.db;

import java.util.concurrent.ExecutionException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.CFMetaDataFactory;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class CounterCacheTest
{
    private static final String KEYSPACE1 = "CounterCacheTest";
    private static final String COUNTER1 = "Counter1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();

        CFMetaData counterTable = CFMetaDataFactory.instance.createBuilder(KEYSPACE1, COUNTER1, false, true, true)
                                  .addPartitionKey("key", Int32Type.instance)
                                  .addClusteringColumn("name", Int32Type.instance)
                                  .addRegularColumn("c", CounterColumnType.instance)
                                  .build();

        SchemaLoader.createKeyspace(KEYSPACE1,
                SimpleStrategy.class,
                KSMetaData.optsWithRF(1),
                counterTable);
    }

    @AfterClass
    public static void cleanup()
    {
        SchemaLoader.cleanupSavedCaches();
    }

    @Test
    public void testReadWrite()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(COUNTER1);
        CacheService.instance.invalidateCounterCache();

        Clustering c1 = CBuilder.create(cfs.metadata.comparator).add(ByteBufferUtil.bytes(1)).build();
        Clustering c2 = CBuilder.create(cfs.metadata.comparator).add(ByteBufferUtil.bytes(2)).build();
        ColumnDefinition cd = cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("c"));

        assertEquals(0, CacheService.instance.counterCache.size());
        assertNull(cfs.getCachedCounter(bytes(1), c1, cd, null));
        assertNull(cfs.getCachedCounter(bytes(1), c2, cd, null));
        assertNull(cfs.getCachedCounter(bytes(2), c1, cd, null));
        assertNull(cfs.getCachedCounter(bytes(2), c2, cd, null));

        cfs.putCachedCounter(bytes(1), c1, cd, null, ClockAndCount.create(1L, 1L));
        cfs.putCachedCounter(bytes(1), c2, cd, null, ClockAndCount.create(1L, 2L));
        cfs.putCachedCounter(bytes(2), c1, cd, null, ClockAndCount.create(2L, 1L));
        cfs.putCachedCounter(bytes(2), c2, cd, null, ClockAndCount.create(2L, 2L));

        assertEquals(ClockAndCount.create(1L, 1L), cfs.getCachedCounter(bytes(1), c1, cd, null));
        assertEquals(ClockAndCount.create(1L, 2L), cfs.getCachedCounter(bytes(1), c2, cd, null));
        assertEquals(ClockAndCount.create(2L, 1L), cfs.getCachedCounter(bytes(2), c1, cd, null));
        assertEquals(ClockAndCount.create(2L, 2L), cfs.getCachedCounter(bytes(2), c2, cd, null));
    }

    @Test
    public void testSaveLoad() throws ExecutionException, InterruptedException, WriteTimeoutException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(COUNTER1);
        CacheService.instance.invalidateCounterCache();

        new CounterMutation(new RowUpdateBuilder(cfs.metadata, 0, bytes(1)).clustering(1).add("c", 1L).build(), ConsistencyLevel.ONE).apply();
        new CounterMutation(new RowUpdateBuilder(cfs.metadata, 0, bytes(1)).clustering(2).add("c", 2L).build(), ConsistencyLevel.ONE).apply();
        new CounterMutation(new RowUpdateBuilder(cfs.metadata, 0, bytes(2)).clustering(1).add("c", 1L).build(), ConsistencyLevel.ONE).apply();
        new CounterMutation(new RowUpdateBuilder(cfs.metadata, 0, bytes(2)).clustering(2).add("c", 2L).build(), ConsistencyLevel.ONE).apply();

        assertEquals(4, CacheService.instance.counterCache.size());

        // flush the counter cache and invalidate
        CacheService.instance.counterCache.submitWrite(Integer.MAX_VALUE).get();
        CacheService.instance.invalidateCounterCache();
        assertEquals(0, CacheService.instance.counterCache.size());

        // load from cache and validate
        CacheService.instance.counterCache.loadSaved(cfs);
        assertEquals(4, CacheService.instance.counterCache.size());

        Clustering c1 = CBuilder.create(cfs.metadata.comparator).add(ByteBufferUtil.bytes(1)).build();
        Clustering c2 = CBuilder.create(cfs.metadata.comparator).add(ByteBufferUtil.bytes(2)).build();
        ColumnDefinition cd = cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("c"));

        assertEquals(ClockAndCount.create(1L, 1L), cfs.getCachedCounter(bytes(1), c1, cd, null));
        assertEquals(ClockAndCount.create(1L, 2L), cfs.getCachedCounter(bytes(1), c2, cd, null));
        assertEquals(ClockAndCount.create(1L, 1L), cfs.getCachedCounter(bytes(2), c1, cd, null));
        assertEquals(ClockAndCount.create(1L, 2L), cfs.getCachedCounter(bytes(2), c2, cd, null));
    }
}

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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import static org.apache.cassandra.Util.cellname;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class CounterCacheTest
{
    private static final String KEYSPACE1 = "CounterCacheTest";
    private static final String CF = "Counter1";

    public static final DatabaseDescriptor databaseDescriptor = DatabaseDescriptor.instance;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF).defaultValidator(CounterColumnType.instance));
    }

    @AfterClass
    public static void cleanup()
    {
        SchemaLoader.cleanupSavedCaches();
    }

    @Test
    public void testReadWrite()
    {
        ColumnFamilyStore cfs = databaseDescriptor.getKeyspaceManager().open(KEYSPACE1).getColumnFamilyStore(CF);
        databaseDescriptor.getCacheService().invalidateCounterCache();

        assertEquals(0, databaseDescriptor.getCacheService().counterCache.size());
        assertNull(cfs.getCachedCounter(bytes(1), cellname(1)));
        assertNull(cfs.getCachedCounter(bytes(1), cellname(2)));
        assertNull(cfs.getCachedCounter(bytes(2), cellname(1)));
        assertNull(cfs.getCachedCounter(bytes(2), cellname(2)));

        cfs.putCachedCounter(bytes(1), cellname(1), ClockAndCount.create(1L, 1L));
        cfs.putCachedCounter(bytes(1), cellname(2), ClockAndCount.create(1L, 2L));
        cfs.putCachedCounter(bytes(2), cellname(1), ClockAndCount.create(2L, 1L));
        cfs.putCachedCounter(bytes(2), cellname(2), ClockAndCount.create(2L, 2L));

        assertEquals(4, databaseDescriptor.getCacheService().counterCache.size());
        assertEquals(ClockAndCount.create(1L, 1L), cfs.getCachedCounter(bytes(1), cellname(1)));
        assertEquals(ClockAndCount.create(1L, 2L), cfs.getCachedCounter(bytes(1), cellname(2)));
        assertEquals(ClockAndCount.create(2L, 1L), cfs.getCachedCounter(bytes(2), cellname(1)));
        assertEquals(ClockAndCount.create(2L, 2L), cfs.getCachedCounter(bytes(2), cellname(2)));
    }

    @Test
    public void testSaveLoad() throws ExecutionException, InterruptedException, WriteTimeoutException
    {
        ColumnFamilyStore cfs = databaseDescriptor.getKeyspaceManager().open(KEYSPACE1).getColumnFamilyStore(CF);
        databaseDescriptor.getCacheService().invalidateCounterCache();

        ColumnFamily cells = ArrayBackedSortedColumns.factory.create(cfs.metadata, DBConfig.instance);
        cells.addColumn(new BufferCounterUpdateCell(cellname(1), 1L, FBUtilities.timestampMicros()));
        cells.addColumn(new BufferCounterUpdateCell(cellname(2), 2L, FBUtilities.timestampMicros()));
        databaseDescriptor.getCounterMutationFactory().create(MutationFactory.instance.create(KEYSPACE1, bytes(1), cells), ConsistencyLevel.ONE).apply();
        databaseDescriptor.getCounterMutationFactory().create(MutationFactory.instance.create(KEYSPACE1, bytes(2), cells), ConsistencyLevel.ONE).apply();

        // flush the counter cache and invalidate
        databaseDescriptor.getCacheService().counterCache.submitWrite(Integer.MAX_VALUE).get();
        databaseDescriptor.getCacheService().invalidateCounterCache();
        assertEquals(0, databaseDescriptor.getCacheService().counterCache.size());

        // load from cache and validate
        databaseDescriptor.getCacheService().counterCache.loadSaved(cfs);
        assertEquals(4, databaseDescriptor.getCacheService().counterCache.size());
        assertEquals(ClockAndCount.create(1L, 1L), cfs.getCachedCounter(bytes(1), cellname(1)));
        assertEquals(ClockAndCount.create(1L, 2L), cfs.getCachedCounter(bytes(1), cellname(2)));
        assertEquals(ClockAndCount.create(1L, 1L), cfs.getCachedCounter(bytes(2), cellname(1)));
        assertEquals(ClockAndCount.create(1L, 2L), cfs.getCachedCounter(bytes(2), cellname(2)));
    }
}

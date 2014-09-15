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

import java.nio.ByteBuffer;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.tracing.Tracing;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import static org.apache.cassandra.Util.cellname;
import static org.apache.cassandra.Util.dk;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class CounterMutationTest
{
    private static final String KEYSPACE1 = "CounterMutationTest";
    private static final String CF1 = "Counter1";
    private static final String CF2 = "Counter2";

    public static final DatabaseDescriptor databaseDescriptor = DatabaseDescriptor.createMain(false);

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF1).defaultValidator(CounterColumnType.instance),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF2).defaultValidator(CounterColumnType.instance));
    }

    @Test
    public void testSingleCell() throws WriteTimeoutException
    {
        ColumnFamilyStore cfs = databaseDescriptor.getKeyspaceManager().open(KEYSPACE1).getColumnFamilyStore(CF1);
        cfs.truncateBlocking();

        // Do the initial update (+1)
        ColumnFamily cells = ArrayBackedSortedColumns.factory.create(cfs.metadata, databaseDescriptor.getDBConfig());
        cells.addCounter(cellname(1), 1L);
        databaseDescriptor.getCounterMutationFactory().create(databaseDescriptor.getMutationFactory().create(KEYSPACE1, bytes(1), cells), ConsistencyLevel.ONE).apply();
        ColumnFamily current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1), databaseDescriptor), CF1, System.currentTimeMillis(), DatabaseDescriptor.createMain(false), databaseDescriptor.getTracing(), databaseDescriptor.getDBConfig()));
        assertEquals(1L, CounterContext.total(current.getColumn(cellname(1)).value()));

        // Make another increment (+2)
        cells = ArrayBackedSortedColumns.factory.create(cfs.metadata, databaseDescriptor.getDBConfig());
        cells.addCounter(cellname(1), 2L);
        databaseDescriptor.getCounterMutationFactory().create(databaseDescriptor.getMutationFactory().create(KEYSPACE1, bytes(1), cells), ConsistencyLevel.ONE).apply();
        current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1), databaseDescriptor), CF1, System.currentTimeMillis(), DatabaseDescriptor.createMain(false), databaseDescriptor.getTracing(), databaseDescriptor.getDBConfig()));
        assertEquals(3L, CounterContext.total(current.getColumn(cellname(1)).value()));

        // Decrement to 0 (-3)
        cells = ArrayBackedSortedColumns.factory.create(cfs.metadata, databaseDescriptor.getDBConfig());
        cells.addCounter(cellname(1), -3L);
        databaseDescriptor.getCounterMutationFactory().create(databaseDescriptor.getMutationFactory().create(KEYSPACE1, bytes(1), cells), ConsistencyLevel.ONE).apply();
        current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1), databaseDescriptor), CF1, System.currentTimeMillis(), DatabaseDescriptor.createMain(false), databaseDescriptor.getTracing(), databaseDescriptor.getDBConfig()));
        assertEquals(0L, CounterContext.total(current.getColumn(cellname(1)).value()));
        assertEquals(ClockAndCount.create(3L, 0L), cfs.getCachedCounter(bytes(1), cellname(1)));
    }

    @Test
    public void testTwoCells() throws WriteTimeoutException
    {
        ColumnFamilyStore cfs = databaseDescriptor.getKeyspaceManager().open(KEYSPACE1).getColumnFamilyStore(CF1);
        cfs.truncateBlocking();

        // Do the initial update (+1, -1)
        ColumnFamily cells = ArrayBackedSortedColumns.factory.create(cfs.metadata, databaseDescriptor.getDBConfig());
        cells.addCounter(cellname(1), 1L);
        cells.addCounter(cellname(2), -1L);
        databaseDescriptor.getCounterMutationFactory().create(databaseDescriptor.getMutationFactory().create(KEYSPACE1, bytes(1), cells), ConsistencyLevel.ONE).apply();
        ColumnFamily current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1), databaseDescriptor), CF1, System.currentTimeMillis(), DatabaseDescriptor.createMain(false), databaseDescriptor.getTracing(), databaseDescriptor.getDBConfig()));
        assertEquals(1L, CounterContext.total(current.getColumn(cellname(1)).value()));
        assertEquals(-1L, CounterContext.total(current.getColumn(cellname(2)).value()));

        // Make another increment (+2, -2)
        cells = ArrayBackedSortedColumns.factory.create(cfs.metadata, databaseDescriptor.getDBConfig());
        cells.addCounter(cellname(1), 2L);
        cells.addCounter(cellname(2), -2L);
        databaseDescriptor.getCounterMutationFactory().create(databaseDescriptor.getMutationFactory().create(KEYSPACE1, bytes(1), cells), ConsistencyLevel.ONE).apply();
        current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1), databaseDescriptor), CF1, System.currentTimeMillis(), DatabaseDescriptor.createMain(false), databaseDescriptor.getTracing(), databaseDescriptor.getDBConfig()));
        assertEquals(3L, CounterContext.total(current.getColumn(cellname(1)).value()));

        // Decrement to 0 (-3, +3)
        cells = ArrayBackedSortedColumns.factory.create(cfs.metadata, databaseDescriptor.getDBConfig());
        cells.addCounter(cellname(1), -3L);
        cells.addCounter(cellname(2), 3L);
        databaseDescriptor.getCounterMutationFactory().create(databaseDescriptor.getMutationFactory().create(KEYSPACE1, bytes(1), cells), ConsistencyLevel.ONE).apply();
        current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1), databaseDescriptor), CF1, System.currentTimeMillis(), DatabaseDescriptor.createMain(false), databaseDescriptor.getTracing(), databaseDescriptor.getDBConfig()));
        assertEquals(0L, CounterContext.total(current.getColumn(cellname(1)).value()));
        assertEquals(0L, CounterContext.total(current.getColumn(cellname(2)).value()));

        // Check the caches, separately
        assertEquals(ClockAndCount.create(3L, 0L), cfs.getCachedCounter(bytes(1), cellname(1)));
        assertEquals(ClockAndCount.create(3L, 0L), cfs.getCachedCounter(bytes(1), cellname(2)));
    }

    @Test
    public void testBatch() throws WriteTimeoutException
    {
        ColumnFamilyStore cfs1 = databaseDescriptor.getKeyspaceManager().open(KEYSPACE1).getColumnFamilyStore(CF1);
        ColumnFamilyStore cfs2 = databaseDescriptor.getKeyspaceManager().open(KEYSPACE1).getColumnFamilyStore(CF2);

        cfs1.truncateBlocking();
        cfs2.truncateBlocking();

        // Do the update (+1, -1), (+2, -2)
        ColumnFamily cells1 = ArrayBackedSortedColumns.factory.create(cfs1.metadata, databaseDescriptor.getDBConfig());
        cells1.addCounter(cellname(1), 1L);
        cells1.addCounter(cellname(2), -1L);

        ColumnFamily cells2 = ArrayBackedSortedColumns.factory.create(cfs2.metadata, databaseDescriptor.getDBConfig());
        cells2.addCounter(cellname(1), 2L);
        cells2.addCounter(cellname(2), -2L);

        Mutation mutation = databaseDescriptor.getMutationFactory().create(KEYSPACE1, bytes(1));
        mutation.add(cells1);
        mutation.add(cells2);

        databaseDescriptor.getCounterMutationFactory().create(mutation, ConsistencyLevel.ONE).apply();

        // Validate all values
        ColumnFamily current1 = cfs1.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1), databaseDescriptor), CF1, System.currentTimeMillis(), DatabaseDescriptor.createMain(false), databaseDescriptor.getTracing(), databaseDescriptor.getDBConfig()));
        ColumnFamily current2 = cfs2.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1), databaseDescriptor), CF2, System.currentTimeMillis(), DatabaseDescriptor.createMain(false), databaseDescriptor.getTracing(), databaseDescriptor.getDBConfig()));

        assertEquals(1L, CounterContext.total(current1.getColumn(cellname(1)).value()));
        assertEquals(-1L, CounterContext.total(current1.getColumn(cellname(2)).value()));
        assertEquals(2L, CounterContext.total(current2.getColumn(cellname(1)).value()));
        assertEquals(-2L, CounterContext.total(current2.getColumn(cellname(2)).value()));

        // Check the caches, separately
        assertEquals(ClockAndCount.create(1L, 1L), cfs1.getCachedCounter(bytes(1), cellname(1)));
        assertEquals(ClockAndCount.create(1L, -1L), cfs1.getCachedCounter(bytes(1), cellname(2)));
        assertEquals(ClockAndCount.create(1L, 2L), cfs2.getCachedCounter(bytes(1), cellname(1)));
        assertEquals(ClockAndCount.create(1L, -2L), cfs2.getCachedCounter(bytes(1), cellname(2)));
    }

    @Test
    public void testDeletes() throws WriteTimeoutException
    {
        ColumnFamilyStore cfs = databaseDescriptor.getKeyspaceManager().open(KEYSPACE1).getColumnFamilyStore(CF1);
        cfs.truncateBlocking();

        // Do the initial update (+1, -1)
        ColumnFamily cells = ArrayBackedSortedColumns.factory.create(cfs.metadata, databaseDescriptor.getDBConfig());
        cells.addCounter(cellname(1), 1L);
        cells.addCounter(cellname(2), 1L);
        databaseDescriptor.getCounterMutationFactory().create(databaseDescriptor.getMutationFactory().create(KEYSPACE1, bytes(1), cells), ConsistencyLevel.ONE).apply();
        ColumnFamily current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1), databaseDescriptor), CF1, System.currentTimeMillis(), DatabaseDescriptor.createMain(false), databaseDescriptor.getTracing(), databaseDescriptor.getDBConfig()));
        assertEquals(1L, CounterContext.total(current.getColumn(cellname(1)).value()));
        assertEquals(1L, CounterContext.total(current.getColumn(cellname(2)).value()));

        // Remove the first counter, increment the second counter
        cells = ArrayBackedSortedColumns.factory.create(cfs.metadata, databaseDescriptor.getDBConfig());
        cells.addTombstone(cellname(1), (int) System.currentTimeMillis() / 1000, FBUtilities.timestampMicros());
        cells.addCounter(cellname(2), 1L);
        databaseDescriptor.getCounterMutationFactory().create(databaseDescriptor.getMutationFactory().create(KEYSPACE1, bytes(1), cells), ConsistencyLevel.ONE).apply();
        current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1), databaseDescriptor), CF1, System.currentTimeMillis(), DatabaseDescriptor.createMain(false), databaseDescriptor.getTracing(), databaseDescriptor.getDBConfig()));
        assertNull(current.getColumn(cellname(1)));
        assertEquals(2L, CounterContext.total(current.getColumn(cellname(2)).value()));

        // Increment the first counter, make sure it's still shadowed by the tombstone
        cells = ArrayBackedSortedColumns.factory.create(cfs.metadata, databaseDescriptor.getDBConfig());
        cells.addCounter(cellname(1), 1L);
        databaseDescriptor.getCounterMutationFactory().create(databaseDescriptor.getMutationFactory().create(KEYSPACE1, bytes(1), cells), ConsistencyLevel.ONE).apply();
        current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1), databaseDescriptor), CF1, System.currentTimeMillis(), DatabaseDescriptor.createMain(false), databaseDescriptor.getTracing(), databaseDescriptor.getDBConfig()));
        assertNull(current.getColumn(cellname(1)));

        // Get rid of the complete partition
        Mutation mutation = databaseDescriptor.getMutationFactory().create(KEYSPACE1, bytes(1));
        mutation.delete(CF1, FBUtilities.timestampMicros());
        databaseDescriptor.getCounterMutationFactory().create(mutation, ConsistencyLevel.ONE).apply();
        current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1), databaseDescriptor), CF1, System.currentTimeMillis(), DatabaseDescriptor.createMain(false), databaseDescriptor.getTracing(), databaseDescriptor.getDBConfig()));
        assertNull(current.getColumn(cellname(1)));
        assertNull(current.getColumn(cellname(2)));

        // Increment both counters, ensure that both stay dead
        cells = ArrayBackedSortedColumns.factory.create(cfs.metadata, databaseDescriptor.getDBConfig());
        cells.addCounter(cellname(1), 1L);
        cells.addCounter(cellname(2), 1L);
        databaseDescriptor.getCounterMutationFactory().create(databaseDescriptor.getMutationFactory().create(KEYSPACE1, bytes(1), cells), ConsistencyLevel.ONE).apply();
        current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1), databaseDescriptor), CF1, System.currentTimeMillis(), DatabaseDescriptor.createMain(false), databaseDescriptor.getTracing(), databaseDescriptor.getDBConfig()));
        assertNull(current.getColumn(cellname(1)));
        assertNull(current.getColumn(cellname(2)));
    }

    @Test
    public void testDuplicateCells() throws WriteTimeoutException
    {
        ColumnFamilyStore cfs = databaseDescriptor.getKeyspaceManager().open(KEYSPACE1).getColumnFamilyStore(CF1);
        cfs.truncateBlocking();

        ColumnFamily cells = ArrayBackedSortedColumns.factory.create(cfs.metadata, databaseDescriptor.getDBConfig());
        cells.addCounter(cellname(1), 1L);
        cells.addCounter(cellname(1), 2L);
        cells.addCounter(cellname(1), 3L);
        cells.addCounter(cellname(1), 4L);
        databaseDescriptor.getCounterMutationFactory().create(databaseDescriptor.getMutationFactory().create(KEYSPACE1, bytes(1), cells), ConsistencyLevel.ONE).apply();

        ColumnFamily current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1), databaseDescriptor), CF1, System.currentTimeMillis(), DatabaseDescriptor.createMain(false), databaseDescriptor.getTracing(), databaseDescriptor.getDBConfig()));
        ByteBuffer context = current.getColumn(cellname(1)).value();
        assertEquals(10L, CounterContext.total(context));
        assertEquals(ClockAndCount.create(1L, 10L), CounterContext.getLocalClockAndCount(context, databaseDescriptor.getSystemKeyspace().getLocalHostId()));
        assertEquals(ClockAndCount.create(1L, 10L), cfs.getCachedCounter(bytes(1), cellname(1)));
    }
}

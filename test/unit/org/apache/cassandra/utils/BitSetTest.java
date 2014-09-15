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
package org.apache.cassandra.utils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import com.google.common.collect.Lists;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DBConfig;
import org.junit.BeforeClass;
import org.junit.Test;

import org.junit.Assert;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.KeyGenerator.RandomStringGenerator;
import org.apache.cassandra.utils.obs.IBitSet;
import org.apache.cassandra.utils.obs.OffHeapBitSet;
import org.apache.cassandra.utils.obs.OpenBitSet;

import static org.junit.Assert.assertEquals;

public class BitSetTest
{
    public static final DatabaseDescriptor databaseDescriptor = DatabaseDescriptor.createMain(false);

    /**
     * Test bitsets in a "real-world" environment, i.e., bloom filters
     */
    @Test
    public void compareBitSets()
    {
        BloomFilter bf2 = (BloomFilter) FilterFactory.getFilter(FilterTestHelper.ELEMENTS / 2, FilterTestHelper.MAX_FAILURE_RATE, false, databaseDescriptor.getDBConfig().offHeapAllocator, databaseDescriptor.getDBConfig().murmur3BloomFilterSerializer);
        BloomFilter bf3 = (BloomFilter) FilterFactory.getFilter(FilterTestHelper.ELEMENTS / 2, FilterTestHelper.MAX_FAILURE_RATE, true, databaseDescriptor.getDBConfig().offHeapAllocator, databaseDescriptor.getDBConfig().murmur3BloomFilterSerializer);

        RandomStringGenerator gen1 = new KeyGenerator.RandomStringGenerator(new Random().nextInt(), FilterTestHelper.ELEMENTS);

        // make sure both bitsets are empty.
        compare(bf2.bitset, bf3.bitset);

        while (gen1.hasNext())
        {
            ByteBuffer key = gen1.next();
            bf2.add(key);
            bf3.add(key);
        }

        compare(bf2.bitset, bf3.bitset);
    }

    private static final Random random = new Random();

    /**
     * Test serialization and de-serialization in-memory
     */
    @Test
    public void testOffHeapSerialization() throws IOException
    {
        OffHeapBitSet bs = new OffHeapBitSet(100000, DatabaseDescriptor.createMain(false).getoffHeapMemoryAllocator());
        populateAndReserialize(bs);
    }

    @Test
    public void testOffHeapCompatibility() throws IOException
    {
        OpenBitSet bs = new OpenBitSet(100000);
        populateAndReserialize(bs);
    }

    private void populateAndReserialize(IBitSet bs) throws IOException
    {
        for (long i = 0; i < bs.capacity(); i++)
            if (random.nextBoolean())
                bs.set(i);

        DataOutputBuffer out = new DataOutputBuffer();
        bs.serialize(out);
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(out.getData()));
        OffHeapBitSet newbs = OffHeapBitSet.deserialize(in, DatabaseDescriptor.createMain(false).getoffHeapMemoryAllocator());
        compare(bs, newbs);
    }

    private void compare(IBitSet bs, IBitSet newbs)
    {
        assertEquals(bs.capacity(), newbs.capacity());
        for (long i = 0; i < bs.capacity(); i++)
            Assert.assertEquals(bs.get(i), newbs.get(i));
    }

    @Test
    public void testBitClear() throws IOException
    {
        int size = Integer.MAX_VALUE / 4000;
        OffHeapBitSet bitset = new OffHeapBitSet(size, DatabaseDescriptor.createMain(false).getoffHeapMemoryAllocator());
        List<Integer> randomBits = Lists.newArrayList();
        for (int i = 0; i < 10; i++)
            randomBits.add(random.nextInt(size));

        for (long randomBit : randomBits)
            bitset.set(randomBit);

        for (long randomBit : randomBits)
            Assert.assertEquals(true, bitset.get(randomBit));

        for (long randomBit : randomBits)
            bitset.clear(randomBit);

        for (long randomBit : randomBits)
            Assert.assertEquals(false, bitset.get(randomBit));
        bitset.close();
    }
}

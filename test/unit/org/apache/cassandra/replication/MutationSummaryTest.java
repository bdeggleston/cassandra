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
package org.apache.cassandra.replication;

import java.io.IOException;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.CassandraTestBase;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MutationSummaryTest extends CassandraTestBase
{
    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
        ClusterMetadataTestHelper.setInstanceForTest();
    }

    @Test
    public void testSerializationVersions() throws IOException
    {
        TableId tableId = TableId.generate();
        CoordinatorLogId logId = new CoordinatorLogId(1, 100);
        Range<Token> range1 = new Range<>(new ByteOrderedPartitioner.BytesToken("a".getBytes()),
                                         new ByteOrderedPartitioner.BytesToken("m".getBytes()));
        Range<Token> range2 = new Range<>(new ByteOrderedPartitioner.BytesToken("m".getBytes()),
                                         new ByteOrderedPartitioner.BytesToken("z".getBytes()));

        MutationSummary.Builder builder = new MutationSummary.Builder(tableId);
        builder.updateMaxSealedEpoch(range1, 42L);
        builder.updateMaxSealedEpoch(range2, 84L);
        Offsets.Immutable.Builder offsetsBuilder = new Offsets.Immutable.Builder(logId);
        offsetsBuilder.add(1);
        offsetsBuilder.add(2);
        builder.builderForLog(logId).reconciled.addAll(offsetsBuilder.build());
        MutationSummary summary = builder.build();

        Map<Range<Token>, Long> epochs = summary.maxSealedEpochsByRange();
        assertEquals(2, epochs.size());
        assertEquals(Long.valueOf(42L), epochs.get(range1));
        assertEquals(Long.valueOf(84L), epochs.get(range2));

        // V2 round-trip retains maxSealedEpochsByRange
        try (DataOutputBuffer output = new DataOutputBuffer())
        {
            Serializers.testSerde(output, MutationSummary.serializer, summary, Version.V2);
        }

        // V1 round-trip defaults maxSealedEpochsByRange to empty map
        try (DataOutputBuffer output = new DataOutputBuffer())
        {
            MutationSummary.serializer.serialize(summary, output, Version.V1);
            org.apache.cassandra.io.util.DataInputBuffer in = new org.apache.cassandra.io.util.DataInputBuffer(output.unsafeGetBufferAndFlip(), false);
            MutationSummary deserializedV1 = MutationSummary.serializer.deserialize(in, Version.V1);
            assertTrue(deserializedV1.maxSealedEpochsByRange().isEmpty());
            assertEquals(summary.tableId(), deserializedV1.tableId());
            assertEquals(summary.size(), deserializedV1.size());
        }
    }
}

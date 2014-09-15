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
package org.apache.cassandra.io.sstable;

import java.io.File;
import java.util.List;

import com.google.common.io.Files;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DBConfig;
import org.apache.cassandra.db.KeyspaceManager;
import org.apache.cassandra.locator.LocatorConfig;
import org.apache.cassandra.tracing.Tracing;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.OutputHandler;

import static org.junit.Assert.assertEquals;

public class SSTableLoaderTest
{
    public static final String KEYSPACE1 = "SSTableLoaderTest";
    public static final String CF_STANDARD = "Standard1";

    public static final DatabaseDescriptor databaseDescriptor = DatabaseDescriptor.instance;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD));
        setup();
    }

    public static void setup() throws Exception
    {
        StorageService.instance.initServer();
    }

    @Test
    public void testLoadingSSTable() throws Exception
    {
        File tempdir = Files.createTempDir();
        File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KEYSPACE1 + File.separator + CF_STANDARD);
        assert dataDir.mkdirs();
        CFMetaData cfmeta = databaseDescriptor.getSchema().getCFMetaData(KEYSPACE1, CF_STANDARD);
        SSTableSimpleUnsortedWriter writer = new SSTableSimpleUnsortedWriter(dataDir,
                                                                             cfmeta,
                                                                             databaseDescriptor.getLocatorConfig().getPartitioner(),
                                                                             1, databaseDescriptor.getDBConfig());
        DecoratedKey key = Util.dk("key1", databaseDescriptor);
        writer.newRow(key.getKey());
        writer.addColumn(ByteBufferUtil.bytes("col1"), ByteBufferUtil.bytes(100), 1);
        writer.close();

        SSTableLoader loader = new SSTableLoader(dataDir, new SSTableLoader.Client(DatabaseDescriptor.instance, databaseDescriptor.getDBConfig())
        {
            public void init(String keyspace)
            {
                for (Range<Token> range : databaseDescriptor.getLocatorConfig().getLocalRanges(KEYSPACE1))
                    addRangeForEndpoint(range, DatabaseDescriptor.instance.getBroadcastAddress());
                setPartitioner(databaseDescriptor.getLocatorConfig().getPartitioner());
            }

            public CFMetaData getCFMetaData(String keyspace, String cfName)
            {
                return databaseDescriptor.getSchema().getCFMetaData(keyspace, cfName);
            }
        }, new OutputHandler.SystemOutput(false, false), DatabaseDescriptor.instance, databaseDescriptor.getSSTableReaderFactory());

        loader.stream().get();

        List<Row> rows = Util.getRangeSlice(databaseDescriptor.getKeyspaceManager().open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD), DatabaseDescriptor.instance, databaseDescriptor.getTracing());
        assertEquals(1, rows.size());
        assertEquals(key, rows.get(0).key);
        assertEquals(ByteBufferUtil.bytes(100), rows.get(0).cf.getColumn(Util.cellname("col1")).value());
    }
}

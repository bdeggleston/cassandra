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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.MigrationManager;

public class SSTableUpgradeTest
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableUpgradeTest.class);

    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tbl";
    private static final String DDL = String.format("CREATE TABLE %s.%s (k int, c int, v1 blob, v2 blob, PRIMARY KEY (k, c))", KEYSPACE, TABLE);
    private static final Random random = new Random(0);

    @BeforeClass
    public static void setupClass() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.startGossiper();
        CFMetaData cfm = CFMetaData.compile(DDL, KEYSPACE);
        KSMetaData ksm = KSMetaData.testMetadata(KEYSPACE, SimpleStrategy.class, KSMetaData.optsWithRF(1), cfm);
        MigrationManager.announceNewKeyspace(ksm);
    }

    private ByteBuffer createBuffer(int size)
    {
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
    }

    private static void copySSTables(ColumnFamilyStore cfs, String destination) throws IOException
    {
        cfs.forceBlockingFlush();
        File dstDir = new File(destination);
        if (dstDir.exists())
            FileUtils.deleteRecursive(dstDir);
        dstDir.mkdirs();

        for (File srcDir : cfs.directories.getCFDirectories())
        {
            for (File file : srcDir.listFiles())
            {
                if (!file.isFile())
                    continue;
                File dstFile = new File(dstDir, file.getName());
                Files.copy(file.toPath(), dstFile.toPath());
            }
        }
    }

    @Test
    public void export14803() throws IOException
    {
        String dir = "/Users/blakeeggleston/code/cassandra-3/test/data/upgrade-sstables/14803";
        logger.info(System.getProperties().toString());
        int k = 100;
        int c = 0;

        String insert = String.format("INSERT INTO %s.%s (k, c, v1, v2) VALUES (?, ?, ?, ?)", KEYSPACE, TABLE);
        int size = DatabaseDescriptor.getColumnIndexSize();
        for (int i=0; i<2; i++)
        {
            QueryProcessor.executeOnceInternal(insert, k, c, createBuffer(size+1), createBuffer(size+1));
            c++;
        }
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        copySSTables(cfs, dir);
    }
}

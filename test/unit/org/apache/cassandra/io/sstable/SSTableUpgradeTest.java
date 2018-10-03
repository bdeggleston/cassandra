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
import java.nio.file.Files;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.KeyspaceParams;

public class SSTableUpgradeTest
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableUpgradeTest.class);

    private static final String TEST_DATA_PROP = "test-data";
    private static final File UPGRADE_SSTABLES_SRC;
    static
    {
        String testData = System.getProperty(TEST_DATA_PROP);
        if (testData == null)
        {
            testData = System.getProperty("user.dir") + "/test/data";

        }
        UPGRADE_SSTABLES_SRC = new File(testData + "/upgrade-sstables");
    }

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.setDaemonInitialized();
        SchemaLoader.prepareServer();
    }

    @Test
    public void test14803() throws Exception
    {
        String KEYSPACE = "ks";
        String TABLE = "tbl";
        String DDL = String.format("CREATE TABLE %s.%s (k int, c int, v1 blob, v2 blob, PRIMARY KEY (k, c))", KEYSPACE, TABLE);
        CFMetaData cfm = CFMetaData.compile(DDL, KEYSPACE);
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), cfm);

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);

        File src = new File(UPGRADE_SSTABLES_SRC, "14803");
        File dst = cfs.getDirectories().getCFDirectories().get(0);

        for (File srcFile : src.listFiles())
        {
            if (!srcFile.isFile())
                continue;

            File dstFile = new File(dst, srcFile.getName());
            Files.copy(srcFile.toPath(), dstFile.toPath());
        }
        cfs.loadNewSSTables();

        UntypedResultSet forward = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE k=100", KEYSPACE, TABLE));
        UntypedResultSet reverse = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM %s.%s WHERE k=100 ORDER BY c DESC", KEYSPACE, TABLE));

        logger.info("{} - {}", forward.size(), reverse.size());
        Assert.assertFalse(forward.isEmpty());
        Assert.assertEquals(forward.size(), reverse.size());


    }
}

package org.apache.cassandra.db;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.io.File;
import java.io.IOException;

import org.apache.cassandra.config.Schema;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.SimpleStrategy;

import static org.apache.cassandra.Util.column;
import static org.apache.cassandra.db.KeyspaceTest.assertColumns;

public class RecoveryManager3Test
{
    private static final String KEYSPACE1 = "RecoveryManager3Test1";
    private static final String CF_STANDARD1 = "Standard1";

    private static final String KEYSPACE2 = "RecoveryManager3Test2";
    private static final String CF_STANDARD3 = "Standard3";

    public static final DatabaseDescriptor databaseDescriptor = DatabaseDescriptor.createMain(false, false);

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
        SchemaLoader.createKeyspace(KEYSPACE2,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE2, CF_STANDARD3));
    }

    @Test
    public void testMissingHeader() throws IOException
    {
        Keyspace keyspace1 = databaseDescriptor.getKeyspaceManager().open(KEYSPACE1);
        Keyspace keyspace2 = databaseDescriptor.getKeyspaceManager().open(KEYSPACE2);

        Mutation rm;
        DecoratedKey dk = Util.dk("keymulti", databaseDescriptor);
        ColumnFamily cf;

        cf = ArrayBackedSortedColumns.factory.create(KEYSPACE1, "Standard1", databaseDescriptor.getSchema(), databaseDescriptor.getDBConfig());
        cf.addColumn(column("col1", "val1", 1L));
        rm = databaseDescriptor.getMutationFactory().create(KEYSPACE1, dk.getKey(), cf);
        rm.apply();

        cf = ArrayBackedSortedColumns.factory.create(KEYSPACE2, "Standard3", databaseDescriptor.getSchema(), databaseDescriptor.getDBConfig());
        cf.addColumn(column("col2", "val2", 1L));
        rm = databaseDescriptor.getMutationFactory().create(KEYSPACE2, dk.getKey(), cf);
        rm.apply();

        keyspace1.getColumnFamilyStore("Standard1").clearUnsafe();
        keyspace2.getColumnFamilyStore("Standard3").clearUnsafe();

        // nuke the header
        for (File file : new File(DatabaseDescriptor.createMain(false, false).getCommitLogLocation()).listFiles())
        {
            if (file.getName().endsWith(".header"))
                FileUtils.deleteWithConfirm(file);
        }

        databaseDescriptor.getCommitLog().resetUnsafe(); // disassociate segments from live CL
        databaseDescriptor.getCommitLog().recover();

        assertColumns(Util.getColumnFamily(keyspace1, dk, "Standard1", databaseDescriptor), "col1");
        assertColumns(Util.getColumnFamily(keyspace2, dk, "Standard3", databaseDescriptor), "col2");
    }
}

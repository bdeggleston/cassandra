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

package org.apache.cassandra.distributed.test.accord;

import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.AccordTxnBuilder;
import org.apache.cassandra.service.accord.db.AccordData;

import static org.apache.cassandra.service.accord.db.AccordUpdate.UpdatePredicate.Type.*;

public class AccordIntegrationTest extends TestBaseImpl
{
    private static void assertRow(Cluster cluster, String query, int k, int c, int v)
    {
        Object[][] result = cluster.coordinator(1).execute(query, ConsistencyLevel.QUORUM);
        Assert.assertArrayEquals(new Object[]{new Object[] {k, c, v}}, result);
    }

    @Test
    public void testQuery() throws Throwable
    {
        String keyspace = "ks" + System.currentTimeMillis();
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + keyspace + ".tbl (k int, c int, v int, primary key (k, c))");
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.createEpochFromConfigUnsafe()));
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.setCacheSize(0)));
            cluster.coordinator(1).execute("INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (1, 0, 3);", ConsistencyLevel.ALL);

            String query = "BEGIN TRANSACTION;\n" +
                           "SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0 AS row1;\n" +
                           "SELECT * FROM " + keyspace + ".tbl WHERE k=1 AND c=0 AS row2;\n" +
                           "INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (0, 0, 1);\n" +
                           "COMMIT TRANSACTION IF row1 NOT EXISTS AND row2.v=3;";
            Object[][] result = cluster.coordinator(1).execute(query, ConsistencyLevel.ANY);
            Assert.assertNull(result);

            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0", 0, 0, 1);
        }
    }

    @Test
    public void variableSubstitution() throws Throwable
    {
        String keyspace = "ks" + System.currentTimeMillis();
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + keyspace + ".tbl1 (k int, c int, v int, primary key (k, c))");
            cluster.schemaChange("CREATE TABLE " + keyspace + ".tbl2 (k int, c int, v int, primary key (k, c))");
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.createEpochFromConfigUnsafe()));
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.setCacheSize(0)));
            cluster.coordinator(1).execute("INSERT INTO " + keyspace + ".tbl1 (k, c, v) VALUES (1, 2, 3);", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO " + keyspace + ".tbl2 (k, c, v) VALUES (2, 2, 4);", ConsistencyLevel.ALL);

            String query = "BEGIN TRANSACTION;\n" +
                           "SELECT * FROM " + keyspace + ".tbl1 WHERE k=1 AND c=2 AS row1;\n" +
                           "SELECT * FROM " + keyspace + ".tbl2 WHERE k=2 AND c=2 AS row2;\n" +
                           "UPDATE " + keyspace + ".tbl1 SET v=row2.v WHERE k=1 AND c=2;\n" +
                           "COMMIT TRANSACTION IF\n" +
                           "  row1.v = 3\n" +
                           "  AND row2.v=4;";
            Object[][] result = cluster.coordinator(1).execute(query, ConsistencyLevel.ANY);
            Assert.assertNull(result);

            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl1 WHERE k=1 AND c=2", 1, 2, 4);
        }
    }

    @Test
    public void multiKeyMultiQuery() throws Throwable
    {
        String keyspace = "ks" + System.currentTimeMillis();

        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + keyspace + ".tbl (k int, c int, v int, primary key (k, c))");
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.createEpochFromConfigUnsafe()));

            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.setCacheSize(0)));

            cluster.get(1).runOnInstance(() -> {
                AccordTxnBuilder txnBuilder = new AccordTxnBuilder();
                txnBuilder.withRead("SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0");
                txnBuilder.withRead("SELECT * FROM " + keyspace + ".tbl WHERE k=1 AND c=0");
                txnBuilder.withWrite("INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (0, 0, 0)");
                txnBuilder.withWrite("INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (1, 0, 0)");
                txnBuilder.withCondition(keyspace, "tbl", 0, 0, NOT_EXISTS);
                try
                {
                    AccordData result = (AccordData) AccordService.instance.node.coordinate(txnBuilder.build()).get();
                    Assert.assertNotNull(result);
                }
                catch (InterruptedException | ExecutionException e)
                {
                    throw new AssertionError(e);
                }
            });

            cluster.get(1).runOnInstance(() -> {
                AccordTxnBuilder txnBuilder = new AccordTxnBuilder();
                txnBuilder.withRead("SELECT * FROM " + keyspace + ".tbl WHERE k=1 AND c=0");
                txnBuilder.withRead("SELECT * FROM " + keyspace + ".tbl WHERE k=2 AND c=0");
                txnBuilder.withWrite("INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (1, 0, 1)");
                txnBuilder.withWrite("INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (2, 0, 1)");
                txnBuilder.withCondition(keyspace, "tbl", 1, 0, "v", EQUAL, 0);
                try
                {
                    AccordData result = (AccordData) AccordService.instance.node.coordinate(txnBuilder.build()).get();
                    Assert.assertNotNull(result);
                }
                catch (InterruptedException | ExecutionException e)
                {
                    throw new AssertionError(e);
                }
            });
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0", 0, 0, 0);
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=1 AND c=0", 1, 0, 1);
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=2 AND c=0", 2, 0, 1);
        }
    }
}

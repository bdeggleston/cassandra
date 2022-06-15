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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.accord.AccordService;


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
            Assert.assertTrue((Boolean) result[0][0]);

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
    public void additionAssignment() throws Throwable
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
                           "UPDATE " + keyspace + ".tbl1 SET v+=5 WHERE k=1 AND c=2;\n" +
                           "UPDATE " + keyspace + ".tbl2 SET v-=2 WHERE k=2 AND c=2;\n" +
                           "COMMIT TRANSACTION;";
            Object[][] result = cluster.coordinator(1).execute(query, ConsistencyLevel.ANY);
            Assert.assertTrue((Boolean) result[0][0]);

            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl1 WHERE k=1 AND c=2", 1, 2, 8);
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl2 WHERE k=2 AND c=2", 2, 2, 2);
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

            String query1 = "BEGIN TRANSACTION;\n" +
                            "SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0 AS select1;\n" +
                            "SELECT * FROM " + keyspace + ".tbl WHERE k=1 AND c=0 AS select2;\n" +
                            "INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (0, 0, 0);\n" +
                            "INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (1, 0, 0);\n" +
                            "COMMIT TRANSACTION IF select1 NOT EXISTS;";
            Object[][] result1 = cluster.coordinator(1).execute(query1, ConsistencyLevel.ANY);
            Assert.assertTrue((Boolean) result1[0][0]);

            String query2 = "BEGIN TRANSACTION;\n" +
                            "SELECT * FROM " + keyspace + ".tbl WHERE k=1 AND c=0 AS select1;\n" +
                            "SELECT * FROM " + keyspace + ".tbl WHERE k=2 AND c=0 AS select2" +
                            "INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (1, 0, 1)" +
                            "INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (2, 0, 1)" +
                            "COMMIT TRANSACTION IF select1.v = 0;";
            Object[][] result2 = cluster.coordinator(1).execute(query2, ConsistencyLevel.ANY);
            Assert.assertTrue((Boolean) result2[0][0]);

            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0", 0, 0, 0);
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=1 AND c=0", 1, 0, 1);
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=2 AND c=0", 2, 0, 1);
        }
    }

    @Test
    public void demoTest() throws Throwable
    {
        String keyspace = "ks" + System.currentTimeMillis();

        try (Cluster cluster = init(Cluster.build(3).start()))
        {
            cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':3};");
            cluster.schemaChange("CREATE TABLE IF NOT EXISTS ks.org_docs ( org_name text, doc_id int, contents_version int static, title text, permissions int, PRIMARY KEY (org_name, doc_id) );");
            cluster.schemaChange("CREATE TABLE IF NOT EXISTS ks.org_users ( org_name text, user text, members_version int static, permissions int, PRIMARY KEY (org_name, user) );");
            cluster.schemaChange("CREATE TABLE IF NOT EXISTS ks.user_docs ( user text, doc_id int, title text, org_name text, permissions int, PRIMARY KEY (user, doc_id) );");

            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.createEpochFromConfigUnsafe()));
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.setCacheSize(0)));

            cluster.coordinator(1).execute("INSERT INTO ks.org_users (org_name, user, members_version, permissions) VALUES ('demo', 'blake', 5, 777);\n", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO ks.org_users (org_name, user, members_version, permissions) VALUES ('demo', 'scott', 5, 777);\n", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO ks.org_docs (org_name, doc_id, contents_version, title, permissions) VALUES ('demo', 100, 5, 'README', 644);\n", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO ks.user_docs (user, doc_id, title, org_name, permissions) VALUES ('blake', 1, 'recipes', NULL, 777);\n", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO ks.user_docs (user, doc_id, title, org_name, permissions) VALUES ('blake', 100, 'README', 'demo', 644);\n", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO ks.user_docs (user, doc_id, title, org_name, permissions) VALUES ('scott', 2, 'to do list', NULL, 777);\n", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO ks.user_docs (user, doc_id, title, org_name, permissions) VALUES ('scott', 100, 'README', 'demo', 644);\n", ConsistencyLevel.ALL);

            String addDoc =  "BEGIN TRANSACTION;\n" +
                             "    SELECT * FROM ks.org_users WHERE org_name='demo' LIMIT 1 as current_users;\n" +
                             "    UPDATE ks.org_docs SET title='slides.key', permissions=777, contents_version += 1 WHERE org_name='demo' AND doc_id=101 AS existing;\n" +
                             "    UPDATE ks.user_docs SET title='slides.key', permissions=777 WHERE user='blake' AND doc_id=101;\n" +
                             "    UPDATE ks.user_docs SET title='slides.key', permissions=777 WHERE user='scott' AND doc_id=101;\n" +
                             "COMMIT TRANSACTION IF current_users.members_version=5 AND existing NOT EXISTS;";
            Object[][] result1 = cluster.coordinator(1).execute(addDoc, ConsistencyLevel.ANY);
            Assert.assertTrue((Boolean) result1[0][0]);

            String addUser = "BEGIN TRANSACTION;\n" +
                             "    SELECT * FROM ks.org_docs WHERE org_name='demo' LIMIT 1 AS contents;\n" +
                             "    UPDATE ks.org_users SET permissions=777, members_version += 1 WHERE org_name='demo' AND user='benedict' AS existing;\n" +
                             "    UPDATE ks.user_docs SET title='README', permissions=644 WHERE user='benedict' AND doc_id=100;\n" +
                             "    UPDATE ks.user_docs SET title='slides.key', permissions=777 WHERE user='benedict' AND doc_id=101;\n" +
                             "COMMIT TRANSACTION IF contents.contents_version=6 AND existing NOT EXISTS;";
            Object[][] result2 = cluster.coordinator(1).execute(addUser, ConsistencyLevel.ANY);
            Assert.assertTrue((Boolean) result2[0][0]);

//            String query2 = "BEGIN TRANSACTION;\n" +
//                            "SELECT * FROM " + keyspace + ".tbl WHERE k=1 AND c=0 AS select1;\n" +
//                            "SELECT * FROM " + keyspace + ".tbl WHERE k=2 AND c=0 AS select2" +
//                            "INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (1, 0, 1)" +
//                            "INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (2, 0, 1)" +
//                            "COMMIT TRANSACTION IF select1.v = 0;";
//            Object[][] result2 = cluster.coordinator(1).execute(query2, ConsistencyLevel.ANY);
//            Assert.assertTrue((Boolean) result2[0][0]);
//
//            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0", 0, 0, 0);
//            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=1 AND c=0", 1, 0, 1);
//            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=2 AND c=0", 2, 0, 1);
        }

    }
}

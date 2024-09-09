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

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.utils.concurrent.Future;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class AccordRepairTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(AccordRepairTest.class);

    @Test
    public void name() throws Exception
    {
        DatabaseDescriptor.clientInitialization();
        final int numNodes = 3;
        final int writePerKey = 5;
        final int numKeys = 10000;
        try (Cluster cluster = Cluster.build(numNodes)
                .withoutVNodes()
                .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK)
                        .set("write_request_timeout", "10s")
                        .set("transaction_timeout", "15s")
                        .set("transaction_timeout", "15s") .set("disk_failure_policy", "ignore"))
                .start())
        {
            String ksName = "ks";
            String tblName = "tbl";
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication={'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange(format("CREATE TABLE %s.%s (k int primary key, v int) WITH transactional_mode='full' AND fast_path={'size':2};", ksName, tblName));

            ExecutorPlus queryExecutor = ExecutorFactory.Global.executorFactory().pooled("queries", 25);
            int totalQueries = numKeys * writePerKey;

            ExecutorService repairExecutor = ExecutorFactory.Global.executorFactory().pooled("repairs", 1);
            Future<?> preRepairFuture = null;
            Future<?> repairFuture = null;
            Future<?> future = null;

            for (int key = 0; key < numKeys; key++)
            {
                for (int query = 0; query < writePerKey; query++)
                {
                    int queryNum = (key * query) + query;
                    int nodeNum = (queryNum % numNodes) + 1;
                    int finalKey = key;
                    int finalQuery = query;
                    future = queryExecutor.submit(() -> {
                        cluster.coordinator(nodeNum).execute(format("BEGIN TRANSACTION\n" +
                                "INSERT INTO %s.%s (k, v) VALUES (%s, %s);\n" +
                                "COMMIT TRANSACTION", ksName, tblName, finalKey, finalQuery), ConsistencyLevel.ANY);
                    });

                    if (queryNum == totalQueries/3)
                    {
                        preRepairFuture = future;
                        repairFuture = preRepairFuture.map(r -> nodetool(cluster.get(1), "repair", ksName), repairExecutor);
                    }
                }
            }

            preRepairFuture.await(5, TimeUnit.MINUTES);
            repairFuture.get(1, TimeUnit.MINUTES);
            future.await(5, TimeUnit.MINUTES);
            Thread.sleep(10000);
            logger.info(">>>>>> Closing");

//            nodetool(cluster.get(1), "repair", ksName);
//            for (int i = 0; i < 500; i++)
//            {
//                int nodeNum = (i % numNodes) + 1;
//            }
        }
    }
}

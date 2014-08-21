/**
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

package org.apache.cassandra.service;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.KeyspaceManager;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class PendingRangeCalculatorService
{
    public static final PendingRangeCalculatorService instance = new PendingRangeCalculatorService(Schema.instance, ClusterState.instance);

    private static Logger logger = LoggerFactory.getLogger(PendingRangeCalculatorService.class);
    private final JMXEnabledThreadPoolExecutor executor = new JMXEnabledThreadPoolExecutor(1, Integer.MAX_VALUE, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(1), new NamedThreadFactory("PendingRangeCalculator"), "internal");

    private AtomicInteger updateJobs = new AtomicInteger(0);

    private final Schema schema;
    private final ClusterState clusterState;

    public PendingRangeCalculatorService(Schema schema, ClusterState clusterState)
    {
        assert schema != null;
        assert clusterState != null;

        this.schema = schema;
        this.clusterState = clusterState;

        executor.setRejectedExecutionHandler(new RejectedExecutionHandler()
        {
            public void rejectedExecution(Runnable r, ThreadPoolExecutor e)
            {
                finishUpdate();
            }
        }
        );
    }

    private static class PendingRangeTask implements Runnable
    {

        private final PendingRangeCalculatorService service;
        private final Schema schema;

        private PendingRangeTask(PendingRangeCalculatorService service, Schema schema)
        {
            assert service != null;
            assert schema != null;

            this.service = service;
            this.schema = schema;
        }

        public void run()
        {
            long start = System.currentTimeMillis();
            for (String keyspaceName : schema.getNonSystemKeyspaces())
            {
                service.calculatePendingRanges(KeyspaceManager.instance.open(keyspaceName, schema).getReplicationStrategy(), keyspaceName);
            }
            service.finishUpdate();
            logger.debug("finished calculation for {} keyspaces in {}ms", schema.getNonSystemKeyspaces().size(), System.currentTimeMillis() - start);
        }
    }

    private void finishUpdate()
    {
        updateJobs.decrementAndGet();
    }

    public void update()
    {
        updateJobs.incrementAndGet();
        executor.submit(new PendingRangeTask(this, schema));
    }

    public void blockUntilFinished()
    {
        // We want to be sure the job we're blocking for is actually finished and we can't trust the TPE's active job count
        while (updateJobs.get() > 0)
        {
            try
            {
                Thread.sleep(100);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
    }


    // public & static for testing purposes
    public void calculatePendingRanges(AbstractReplicationStrategy strategy, String keyspaceName)
    {
        clusterState.getTokenMetadata().calculatePendingRanges(strategy, keyspaceName);
    }
}

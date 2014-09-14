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
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.KeyspaceManager;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.LocatorConfig;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class PendingRangeCalculatorService
{
    public static final PendingRangeCalculatorService instance = new PendingRangeCalculatorService(DatabaseDescriptor.instance, Tracing.instance);

    private static Logger logger = LoggerFactory.getLogger(PendingRangeCalculatorService.class);
    private final JMXEnabledThreadPoolExecutor executor;

    private AtomicInteger updateJobs = new AtomicInteger(0);

    private final DatabaseDescriptor databaseDescriptor;
    public PendingRangeCalculatorService(DatabaseDescriptor databaseDescriptor, Tracing tracing)
    {
        assert tracing != null;

        this.databaseDescriptor = databaseDescriptor;

        executor = new JMXEnabledThreadPoolExecutor(1, Integer.MAX_VALUE, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(1), new NamedThreadFactory("PendingRangeCalculator"), "internal", tracing);
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

        private final Schema schema;
        private final KeyspaceManager keyspaceManager;
        private final PendingRangeCalculatorService pendingRangeCalculatorService;
        private final LocatorConfig locatorConfig;


        private PendingRangeTask(Schema schema, KeyspaceManager keyspaceManager, PendingRangeCalculatorService pendingRangeCalculatorService, LocatorConfig locatorConfig)
        {
            this.schema = schema;
            this.keyspaceManager = keyspaceManager;
            this.pendingRangeCalculatorService = pendingRangeCalculatorService;
            this.locatorConfig = locatorConfig;
        }

        public void run()
        {
            long start = System.currentTimeMillis();
            for (String keyspaceName : schema.getNonSystemKeyspaces())
            {
                calculatePendingRanges(keyspaceManager.open(keyspaceName).getReplicationStrategy(), keyspaceName, locatorConfig.getTokenMetadata());
            }
            pendingRangeCalculatorService.finishUpdate();
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
        executor.submit(new PendingRangeTask(databaseDescriptor.getSchema(),
                                             databaseDescriptor.getKeyspaceManager(),
                                             this,
                                             databaseDescriptor.getLocatorConfig()));
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
    public static void calculatePendingRanges(AbstractReplicationStrategy strategy, String keyspaceName, TokenMetadata tokenMetadata)
    {
        tokenMetadata.calculatePendingRanges(strategy, keyspaceName);
    }
}

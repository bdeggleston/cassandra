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
package org.apache.cassandra.concurrent;

import java.util.EnumMap;
import java.util.concurrent.*;

import org.apache.cassandra.tracing.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.utils.FBUtilities;

import org.apache.cassandra.config.DatabaseDescriptor;


/**
 * This class manages executor services for Messages recieved: each Message requests
 * running on a specific "stage" for concurrency control; hence the Map approach,
 * even though stages (executors) are not created dynamically.
 */
public class StageManager
{
    private static final Logger logger = LoggerFactory.getLogger(StageManager.class);

    public static final long KEEPALIVE = 60; // seconds to keep "extra" threads alive for when idle

    private final EnumMap<Stage, TracingAwareExecutorService> stages = new EnumMap<Stage, TracingAwareExecutorService>(Stage.class);

    private final DatabaseDescriptor databaseDescriptor;

    public StageManager(DatabaseDescriptor databaseDescriptor, Tracing tracing)
    {
        assert databaseDescriptor != null;
        assert tracing != null;

        this.databaseDescriptor = databaseDescriptor;

        stages.put(Stage.MUTATION, multiThreadedLowSignalStage(Stage.MUTATION, databaseDescriptor.getConcurrentWriters()));
        stages.put(Stage.COUNTER_MUTATION, multiThreadedLowSignalStage(Stage.COUNTER_MUTATION, databaseDescriptor.getConcurrentCounterWriters()));
        stages.put(Stage.READ, multiThreadedLowSignalStage(Stage.READ, databaseDescriptor.getConcurrentReaders()));
        stages.put(Stage.REQUEST_RESPONSE, multiThreadedLowSignalStage(Stage.REQUEST_RESPONSE, FBUtilities.getAvailableProcessors()));
        stages.put(Stage.INTERNAL_RESPONSE, multiThreadedStage(Stage.INTERNAL_RESPONSE, FBUtilities.getAvailableProcessors(), tracing));
        // the rest are all single-threaded
        stages.put(Stage.GOSSIP, new JMXEnabledThreadPoolExecutor(Stage.GOSSIP, tracing, databaseDescriptor.shouldInitializeJMX()));
        stages.put(Stage.ANTI_ENTROPY, new JMXEnabledThreadPoolExecutor(Stage.ANTI_ENTROPY, tracing, databaseDescriptor.shouldInitializeJMX()));
        stages.put(Stage.MIGRATION, new JMXEnabledThreadPoolExecutor(Stage.MIGRATION, tracing, databaseDescriptor.shouldInitializeJMX()));
        stages.put(Stage.MISC, new JMXEnabledThreadPoolExecutor(Stage.MISC, tracing, databaseDescriptor.shouldInitializeJMX()));
        stages.put(Stage.READ_REPAIR, multiThreadedStage(Stage.READ_REPAIR, FBUtilities.getAvailableProcessors(), tracing));
        stages.put(Stage.TRACING, tracingExecutor());
    }

    private ExecuteOnlyExecutor tracingExecutor()
    {
        RejectedExecutionHandler reh = new RejectedExecutionHandler()
        {
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor)
            {
                databaseDescriptor.getMessagingService().incrementDroppedMessages(MessagingService.Verb._TRACE);
            }
        };
        return new ExecuteOnlyExecutor(1,
                                       1,
                                       KEEPALIVE,
                                       TimeUnit.SECONDS,
                                       new ArrayBlockingQueue<Runnable>(1000),
                                       new NamedThreadFactory(Stage.TRACING.getJmxName()),
                                       reh);
    }

    private JMXEnabledThreadPoolExecutor multiThreadedStage(Stage stage, int numThreads, Tracing tracing)
    {
        return new JMXEnabledThreadPoolExecutor(numThreads,
                                                KEEPALIVE,
                                                TimeUnit.SECONDS,
                                                new LinkedBlockingQueue<Runnable>(),
                                                new NamedThreadFactory(stage.getJmxName()),
                                                stage.getJmxType(),
                                                tracing,
                                                databaseDescriptor.shouldInitializeJMX());
    }

    private TracingAwareExecutorService multiThreadedLowSignalStage(Stage stage, int numThreads)
    {
        return JMXEnabledSharedExecutorPool.SHARED.newExecutor(numThreads,
                                                               Integer.MAX_VALUE,
                                                               stage.getJmxName(),
                                                               stage.getJmxType(),
                                                               databaseDescriptor,
                                                               databaseDescriptor.getTracing());
    }

    /**
     * Retrieve a stage from the StageManager
     * @param stage name of the stage to be retrieved.
     */
    public TracingAwareExecutorService getStage(Stage stage)
    {
        return stages.get(stage);
    }

    /**
     * This method shuts down all registered stages.
     */
    public void shutdownNow()
    {
        for (Stage stage : Stage.values())
        {
            stages.get(stage).shutdownNow();
        }
    }

    /**
     * A TPE that disallows submit so that we don't need to worry about unwrapping exceptions on the
     * tracing stage.  See CASSANDRA-1123 for background.
     */
    private class ExecuteOnlyExecutor extends ThreadPoolExecutor implements TracingAwareExecutorService
    {
        public ExecuteOnlyExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler)
        {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
        }

        public void execute(Runnable command, TraceState state)
        {
            assert state == null;
            super.execute(command);
        }

        public void maybeExecuteImmediately(Runnable command)
        {
            execute(command);
        }

        @Override
        public Future<?> submit(Runnable task)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Future<T> submit(Callable<T> task)
        {
            throw new UnsupportedOperationException();
        }
    }
}

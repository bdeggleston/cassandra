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

package org.apache.cassandra.reads.repair;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.Futures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.net.AsyncOneResponse;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.Tracing;

/**
 * 'Classic' read repair. Doesn't allow the client read to return until
 *  updates have been written to nodes needing correction.
 */
public class BlockingReadRepair implements IReadRepairStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(BlockingReadRepair.class);

    private final ReadCommand command;
    private final List<InetAddress> endpoints;
    private final long queryStartNanoTime;
    private final ConsistencyLevel consistency;
    List<AsyncOneResponse<?>> responses;


    public BlockingReadRepair(ReadCommand command,
                              List<InetAddress> endpoints,
                              long queryStartNanoTime,
                              ConsistencyLevel consistency)
    {
        this.command = command;
        this.endpoints = endpoints;
        this.queryStartNanoTime = queryStartNanoTime;
        this.consistency = consistency;

        responses = new ArrayList<>(endpoints.size());
    }

    public void onDigestMismatch(Collection<InetAddress> contacted)
    {

    }

    public void doForegroundReadRepair()
    {

    }

    public void doBackgroundReadRepair()
    {

    }

    public UnfilteredPartitionIterators.MergeListener getMergeListener(InetAddress[] endpoints)
    {
        return new PartitionIteratorMergeListener(endpoints, command, this);
    }

    protected AsyncOneResponse sendMutation(InetAddress endpoint, Mutation mutation)
    {
        // use a separate verb here because we don't want these to be get the white glove hint-
        // on-timeout behavior that a "real" mutation gets
        Tracing.trace("Sending read-repair-mutation to {}", endpoint);
        MessageOut<Mutation> msg = mutation.createMessage(MessagingService.Verb.READ_REPAIR);
        return MessagingService.instance().sendRR(msg, endpoint);
    }

    public void reportMutation(InetAddress endpoint, Mutation mutation)
    {
        responses.add(sendMutation(endpoint, mutation));
    }

    public void awaitMutationDelivery(long timeout)
    {
        try
        {
            Futures.allAsList(responses).get(timeout, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException ex)
        {
            // We got all responses, but timed out while repairing
            Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
            int blockFor = consistency.blockFor(keyspace);
            if (Tracing.isTracing())
                Tracing.trace("Timed out while read-repairing after receiving all {} data and digest responses", blockFor);
            else
                logger.debug("Timeout while read-repairing after receiving all {} data and digest responses", blockFor);

            throw new ReadTimeoutException(consistency, blockFor - 1, blockFor, true);
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }
}

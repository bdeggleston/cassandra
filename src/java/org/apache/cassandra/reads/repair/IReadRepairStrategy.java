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
import java.util.List;
import java.util.concurrent.Future;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.service.DigestResolver;
import org.apache.cassandra.service.ResponseResolver;
import org.apache.cassandra.tracing.TraceState;

public interface IReadRepairStrategy
{
    /**
     * Used by DataResolver to generate corrections as the partition iterator is consumed
     */
    UnfilteredPartitionIterators.MergeListener getMergeListener(InetAddress[] endpoints);

    public Future<PartitionIterator> beginForegroundRepair(DigestResolver digestResolver, List<InetAddress> allEndpoints, List<InetAddress> contactedEndpoints);

    public void awaitForegroundRepairFinish() throws ReadTimeoutException;

    public void maybeBeginBackgroundRepair(ResponseResolver resolver);

    public void backgroundDigestRepair(DigestResolver digestResolver, TraceState traceState);

    static IReadRepairStrategy create(ReadCommand command, List<InetAddress> endpoints, long queryStartNanoTime, ConsistencyLevel consistency)
    {
        return new BlockingReadRepair(command, endpoints, queryStartNanoTime, consistency);
    }
}

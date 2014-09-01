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
package org.apache.cassandra.service.pager;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.StorageService;

/**
 * Pages a RangeSliceCommand whose predicate is a name query.
 *
 * Note: this only work for NamesQueryFilter that have countCQL3Rows() set,
 * because this assumes the pageSize is counted in number of internal rows
 * returned. More precisely, this doesn't do in-row paging so this assumes
 * that the counter returned by columnCounter() will count 1 for each internal
 * row.
 */
public class RangeNamesQueryPager extends AbstractQueryPager
{
    private final PartitionRangeReadCommand command;
    private volatile DecoratedKey lastReturnedKey;

    public RangeNamesQueryPager(PartitionRangeReadCommand command, ConsistencyLevel consistencyLevel, boolean localQuery, PagingState state)
    {
        super(consistencyLevel, localQuery, command.metadata(), command.limits());
        this.command = command;
        assert command.isNamesQuery();

        if (state != null)
        {
            lastReturnedKey = StorageService.getPartitioner().decorateKey(state.partitionKey);
            restoreState(lastReturnedKey, state.remaining, state.remainingInPartition);
        }
    }

    public PagingState state()
    {
        return lastReturnedKey == null
             ? null
             : new PagingState(lastReturnedKey.getKey(), null, maxRemaining(), remainingInPartition());
    }

    protected PartitionIterator queryNextPage(int pageSize, ConsistencyLevel consistencyLevel, boolean localQuery)
    throws RequestExecutionException
    {
        PartitionRangeReadCommand pageCmd = command.withUpdatedLimit(command.limits().forPaging(pageSize));
        if (lastReturnedKey != null)
            pageCmd = pageCmd.forSubRange(makeExcludingKeyBounds(lastReturnedKey));

        return localQuery ? pageCmd.executeInternal() : pageCmd.execute(consistencyLevel, null);
    }

    protected void recordLast(DecoratedKey key, Row last)
    {
        lastReturnedKey = key;
    }

    private AbstractBounds<PartitionPosition> makeExcludingKeyBounds(PartitionPosition lastReturnedKey)
    {
        // We return a range that always exclude lastReturnedKey, since we've already
        // returned it.
        AbstractBounds<PartitionPosition> bounds = command.dataRange().keyRange();
        if (bounds instanceof Range || bounds instanceof Bounds)
        {
            return new Range<PartitionPosition>(lastReturnedKey, bounds.right);
        }
        else
        {
            return new ExcludingBounds<PartitionPosition>(lastReturnedKey, bounds.right);
        }
    }
}

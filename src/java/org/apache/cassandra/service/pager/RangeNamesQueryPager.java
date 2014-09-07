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

import java.util.List;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.StorageProxy;

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
    private final RangeSliceCommand command;
    private volatile DecoratedKey lastReturnedKey;

    private final StorageProxy storageProxy;
    private final IPartitioner partitioner;

    // Don't use directly, use QueryPagers method instead
    RangeNamesQueryPager(RangeSliceCommand command, Schema schema, ConsistencyLevel consistencyLevel, boolean localQuery, StorageProxy storageProxy, IPartitioner partitioner)
    {
        super(consistencyLevel, command.maxResults, localQuery, command.keyspace, command.columnFamily, schema, command.predicate, command.timestamp);
        this.command = command;
        assert columnFilter instanceof NamesQueryFilter && ((NamesQueryFilter)columnFilter).countCQL3Rows();

        this.storageProxy = storageProxy;
        this.partitioner = partitioner;
    }

    RangeNamesQueryPager(RangeSliceCommand command, Schema schema, ConsistencyLevel consistencyLevel, boolean localQuery, PagingState state, StorageProxy storageProxy, IPartitioner partitioner)
    {
        this(command, schema, consistencyLevel, localQuery, storageProxy, partitioner);

        if (state != null)
        {
            lastReturnedKey = this.partitioner.decorateKey(state.partitionKey);
            restoreState(state.remaining, true);
        }
    }

    public PagingState state()
    {
        return lastReturnedKey == null
             ? null
             : new PagingState(lastReturnedKey.getKey(), null, maxRemaining());
    }

    protected List<Row> queryNextPage(int pageSize, ConsistencyLevel consistencyLevel, boolean localQuery)
    throws RequestExecutionException
    {
        AbstractRangeCommand pageCmd = command.withUpdatedLimit(pageSize);
        if (lastReturnedKey != null)
            pageCmd = pageCmd.forSubRange(makeExcludingKeyBounds(lastReturnedKey));

        return localQuery
             ? pageCmd.executeLocally()
             : storageProxy.getRangeSlice(pageCmd, consistencyLevel);
    }

    protected boolean containsPreviousLast(Row first)
    {
        // When querying the next page, we create a bound that exclude the lastReturnedKey
        return false;
    }

    protected boolean recordLast(Row last)
    {
        lastReturnedKey = last.key;
        // We return false as that means "can that last be in the next query?"
        return false;
    }

    protected boolean isReversed()
    {
        return false;
    }

    private AbstractBounds<RowPosition> makeExcludingKeyBounds(RowPosition lastReturnedKey)
    {
        // We return a range that always exclude lastReturnedKey, since we've already
        // returned it.
        AbstractBounds<RowPosition> bounds = command.keyRange;
        if (bounds instanceof Range || bounds instanceof Bounds)
        {
            return new Range<RowPosition>(lastReturnedKey, bounds.right, partitioner);
        }
        else
        {
            return new ExcludingBounds<RowPosition>(lastReturnedKey, bounds.right, partitioner);
        }
    }
}

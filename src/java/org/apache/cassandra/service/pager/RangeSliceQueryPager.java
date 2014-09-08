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
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;

/**
 * Pages a RangeSliceCommand whose predicate is a slice query.
 *
 * Note: this only work for CQL3 queries for now (because thrift queries expect
 * a different limit on the rows than on the columns, which complicates it).
 */
public class RangeSliceQueryPager extends AbstractQueryPager
{
    private final RangeSliceCommand command;
    private volatile DecoratedKey lastReturnedKey;
    private volatile CellName lastReturnedName;

    private final StorageProxy storageProxy;
    private final MessagingService messagingService;
    private final IPartitioner partitioner;

    // Don't use directly, use QueryPagers method instead
    RangeSliceQueryPager(RangeSliceCommand command, Schema schema, ConsistencyLevel consistencyLevel, boolean localQuery, StorageProxy storageProxy, MessagingService messagingService, IPartitioner partitioner)
    {
        super(consistencyLevel, command.maxResults, localQuery, command.keyspace, command.columnFamily, schema, command.predicate, command.timestamp);
        this.command = command;
        assert columnFilter instanceof SliceQueryFilter;
        this.storageProxy = storageProxy;
        this.partitioner = partitioner;
        this.messagingService = messagingService;
    }

    RangeSliceQueryPager(RangeSliceCommand command, Schema schema, ConsistencyLevel consistencyLevel, boolean localQuery, PagingState state, StorageProxy storageProxy, MessagingService messagingService, IPartitioner partitioner)
    {
        this(command, schema, consistencyLevel, localQuery, storageProxy, messagingService, partitioner);

        if (state != null)
        {
            lastReturnedKey = this.partitioner.decorateKey(state.partitionKey);
            lastReturnedName = cfm.comparator.cellFromByteBuffer(state.cellName);
            restoreState(state.remaining, true);
        }
    }

    public PagingState state()
    {
        return lastReturnedKey == null
             ? null
             : new PagingState(lastReturnedKey.getKey(), lastReturnedName.toByteBuffer(), maxRemaining());
    }

    protected List<Row> queryNextPage(int pageSize, ConsistencyLevel consistencyLevel, boolean localQuery)
    throws RequestExecutionException
    {
        SliceQueryFilter sf = (SliceQueryFilter)columnFilter;
        AbstractBounds<RowPosition> keyRange = lastReturnedKey == null ? command.keyRange : makeIncludingKeyBounds(lastReturnedKey);
        Composite start = lastReturnedName == null ? sf.start() : lastReturnedName;
        PagedRangeCommand pageCmd = new PagedRangeCommand(command.keyspace,
                                                          command.columnFamily,
                                                          command.timestamp,
                                                          keyRange,
                                                          sf,
                                                          start,
                                                          sf.finish(),
                                                          command.rowFilter,
                                                          pageSize,
                                                          command.countCQL3Rows,
                                                          messagingService.pagedRangeCommandSerializer);

        return localQuery
             ? pageCmd.executeLocally()
             : storageProxy.getRangeSlice(pageCmd, consistencyLevel);
    }

    protected boolean containsPreviousLast(Row first)
    {
        if (lastReturnedKey == null || !lastReturnedKey.equals(first.key))
            return false;

        // Same as SliceQueryPager, we ignore a deleted column
        Cell firstCell = isReversed() ? lastCell(first.cf) : firstCell(first.cf);
        return !first.cf.deletionInfo().isDeleted(firstCell)
            && firstCell.isLive(timestamp())
            && lastReturnedName.equals(firstCell.name());
    }

    protected boolean recordLast(Row last)
    {
        lastReturnedKey = last.key;
        lastReturnedName = (isReversed() ? firstCell(last.cf) : lastCell(last.cf)).name();
        return true;
    }

    protected boolean isReversed()
    {
        return ((SliceQueryFilter)command.predicate).reversed;
    }

    private AbstractBounds<RowPosition> makeIncludingKeyBounds(RowPosition lastReturnedKey)
    {
        // We always include lastReturnedKey since we may still be paging within a row,
        // and PagedRangeCommand will move over if we're not anyway
        AbstractBounds<RowPosition> bounds = command.keyRange;
        if (bounds instanceof Range || bounds instanceof Bounds)
        {
            return new Bounds<RowPosition>(lastReturnedKey, bounds.right, partitioner);
        }
        else
        {
            return new IncludingExcludingBounds<RowPosition>(lastReturnedKey, bounds.right, partitioner);
        }
    }
}

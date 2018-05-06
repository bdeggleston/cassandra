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
package org.apache.cassandra.db;

import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Generic abstraction for read queries.
 * <p>
 * The main implementation of this is {@link ReadCommand} but we have this interface because
 * {@link SinglePartitionReadCommand.Group} is also consider as a "read query" but is not a
 * {@code ReadCommand}.
 */
public interface QueryGroup extends ReadQuery, ReadExecutable.Distributed
{

    public ReadContext getReadContext();

    QueryGroup EMPTY = new QueryGroup()
    {
        public PartitionIterator execute(ConsistencyLevel consistency, ClientState clientState, long queryStartNanoTime) throws RequestExecutionException
        {
            return EmptyIterators.partition();
        }

        public PartitionIterator executeInternal(ReadContext context)
        {
            return EmptyIterators.partition();
        }

        public UnfilteredPartitionIterator executeLocally(ReadContext context)
        {
            return EmptyIterators.unfilteredPartition(context.metadata());
        }

        public DataLimits limits()
        {
            // What we return here doesn't matter much in practice. However, returning DataLimits.NONE means
            // "no particular limit", which makes SelectStatement.execute() take the slightly more complex "paging"
            // path. Not a big deal but it's easy enough to return a limit of 0 rows which avoids this.
            return DataLimits.cqlLimits(0);
        }

        @Override
        public ReadContext getReadContext()
        {
            return ReadContext.empty();
        }

        public QueryPager getPager(PagingState state, ProtocolVersion protocolVersion)
        {
            return QueryPager.EMPTY;
        }

        public boolean selectsKey(DecoratedKey key)
        {
            return false;
        }

        public boolean selectsClustering(DecoratedKey key, Clustering clustering)
        {
            return false;
        }

        @Override
        public boolean selectsFullPartition()
        {
            return false;
        }
    };

    public static QueryGroup wrap(ReadCommand command)
    {
        ReadHandler handler = command.getCfs().getReadHandler();
        return new QueryGroup()
        {

            @Override
            public PartitionIterator execute(ConsistencyLevel consistency, ClientState clientState, long queryStartNanoTime) throws RequestExecutionException
            {
                return command.execute(consistency, clientState, queryStartNanoTime);
            }

            @Override
            public PartitionIterator executeInternal(ReadContext context)
            {
                return handler.getReadExecutable(context, command).executeInternal(context);
            }

            @Override
            public UnfilteredPartitionIterator executeLocally(ReadContext context)
            {
                return handler.executeLocally(context, command);
            }

            @Override
            public QueryPager getPager(PagingState pagingState, ProtocolVersion protocolVersion)
            {
                return command.getPager(pagingState, protocolVersion);
            }

            @Override
            public DataLimits limits()
            {
                return command.limits();
            }

            @Override
            public ReadContext getReadContext()
            {
                return handler.contextForCommand(command);
            }

            @Override
            public boolean selectsKey(DecoratedKey key)
            {
                return command.selectsKey(key);
            }

            @Override
            public boolean selectsClustering(DecoratedKey key, Clustering clustering)
            {
                return command.selectsClustering(key, clustering);
            }

            @Override
            public boolean selectsFullPartition()
            {
                return command.selectsFullPartition();
            }
        };
    }

}

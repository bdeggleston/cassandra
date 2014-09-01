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

/**
 * Generic abstraction for read queries.
 * <p>
 * The main implementation of this is {@link ReadCommand} but we have this interface because
 * {@link SinglePartitionReadCommand.Group} is also consider as a "read query" but is not a
 * {@code ReadCommand}.
 */
public interface ReadQuery
{
    public static final ReadQuery EMPTY = new ReadQuery()
    {
        public PartitionIterator execute(ConsistencyLevel consistency, ClientState clientState) throws RequestExecutionException
        {
            return PartitionIterators.EMPTY;
        }

        public PartitionIterator executeInternal()
        {
            return PartitionIterators.EMPTY;
        }

        public DataLimits limits()
        {
            // What we return here doesn't matter much in practice. However, returning DataLimits.NONE means
            // "no particular limit", which makes SelectStatement.execute() take the slightly more complex "paging"
            // path. Not a big deal but it's easy enough to return a limit of 0 rows which avoids this.
            return DataLimits.cqlLimits(0);
        }

        public QueryPager getPager(ConsistencyLevel consistency, ClientState clientState, PagingState state)
        {
            return QueryPager.EMPTY;
        }

        public QueryPager getLocalPager()
        {
            return QueryPager.EMPTY;
        }
    };

    /**
     * Executes the query at the provided consistency level.
     *
     * @param consistency the consistency level to achieve for the query.
     * @param clientState the {@code ClientState} for the query. In practice, this can be null unless
     * {@code consistency} is a serial consistency.
     *
     * @return the result of the query.
     */
    public PartitionIterator execute(ConsistencyLevel consistency, ClientState clientState) throws RequestExecutionException;

    /**
     * Execute the query for internal queries (that is, it basically executes the query locally).
     *
     * @return the result of the query.
     */
    public PartitionIterator executeInternal();

    /**
     * Returns a pager for the query.
     *
     * @param consistency the consistency level to achieve for the query.
     * @param clientState the {@code ClientState} for the query. In practice, this can be null unless
     * {@code consistency} is a serial consistency.
     * @param pagingState the {@code PagingState} to start from if this is a paging continuation. This can be
     * {@code null} if this is the start of paging.
     *
     * @return a pager for the query.
     */
    public QueryPager getPager(ConsistencyLevel consistency, ClientState clientState, PagingState pagingState);

    /**
     * Returns a pager for the query that executes the query locally.
     *
     * @return a pager for the query.
     */
    public QueryPager getLocalPager();

    /**
     * The limits for the query.
     *
     * @return The limits for the query.
     */
    public DataLimits limits();
}

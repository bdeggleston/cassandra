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

import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.transport.ProtocolVersion;

public interface ReadExecutable
{

    /**
     * Execute the query for internal queries (that is, it basically executes the query locally).
     *
     * @param context the {@code ReadContext} protecting the read.
     * @return the result of the query.
     */
    public PartitionIterator executeInternal(ReadContext context);

    /**
     * Execute the query locally. This is similar to {@link QueryGroup#executeInternal(ReadContext)}
     * but it returns an unfiltered partition iterator that can be merged later on.
     *
     * @param context the {@code ReadContext} protecting the read.
     * @return the result of the read query.
     */
    public UnfilteredPartitionIterator executeLocally(ReadContext context);

    public interface Local extends ReadExecutable
    {
        /**
         * Directly query the storage without dealing with caches, limits, and most metrics
         * @param context
         * @return
         */
        public UnfilteredPartitionIterator executeDirect(ReadContext context);
    }

    public interface Distributed extends ReadExecutable
    {
        // FIXME: distributed and local logic should be separate
        /**
         * Executes the query at the provided consistency level.
         *
         * @param consistency the consistency level to achieve for the query.
         * @param clientState the {@code ClientState} for the query. In practice, this can be null unless
         * {@code consistency} is a serial consistency.
         *
         * @return the result of the query.
         */
        public PartitionIterator execute(ConsistencyLevel consistency, ClientState clientState, long queryStartNanoTime) throws RequestExecutionException;
    }
}

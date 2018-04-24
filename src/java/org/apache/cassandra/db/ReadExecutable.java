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
     * Starts a new read operation.
     * <p>
     * This must be called before {@link executeInternal} and passed to it to protect the read.
     * The returned object <b>must</b> be closed on all path and it is thus strongly advised to
     * use it in a try-with-ressource construction.
     *
     * @return a newly started execution controller for this {@code ReadGroup}.
     */
    public ReadExecutionController executionController();

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

    /**
     * Execute the query for internal queries (that is, it basically executes the query locally).
     *
     * @param controller the {@code ReadExecutionController} protecting the read.
     * @return the result of the query.
     */
    public PartitionIterator executeInternal(ReadExecutionController controller);

    /**
     * Execute the query locally. This is similar to {@link ReadGroup#executeInternal(ReadExecutionController)}
     * but it returns an unfiltered partition iterator that can be merged later on.
     *
     * @param executionController the {@code ReadExecutionController} protecting the read.
     * @return the result of the read query.
     */
    public UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController);

    /**
     * Returns a pager for the query.
     *
     * @param pagingState the {@code PagingState} to start from if this is a paging continuation. This can be
     * {@code null} if this is the start of paging.
     * @param protocolVersion the protocol version to use for the paging state of that pager.
     *
     * @return a pager for the query.
     */
    public QueryPager getPager(PagingState pagingState, ProtocolVersion protocolVersion);
}

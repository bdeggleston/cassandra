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

public interface ReadHandler
{
    /**
     * Starts a new read operation.
     * <p>
     * This must be called before {@link executeInternal} and passed to it to protect the read.
     * The returned object <b>must</b> be closed on all path and it is thus strongly advised to
     * use it in a try-with-ressource construction.
     *
     * @return a newly started execution controller for this {@code QueryGroup}.
     */
    ReadContext contextForCommand(ReadCommand command);

    ReadContext getContext(ReadContext indexReadCtx, WriteContext writeContext);

    default ReadContext getContext()
    {
        return getContext(null, null);
    }

    ReadExecutable.Local getReadExecutable(ReadContext ctx, ReadCommand command);


    default UnfilteredPartitionIterator executeLocally(ReadContext ctx, ReadCommand command)
    {
        return getReadExecutable(ctx, command).executeLocally(ctx);
    }

    default UnfilteredPartitionIterator executeDirect(ReadContext ctx, ReadCommand command)
    {
        return getReadExecutable(ctx, command).executeDirect(ctx);
    }

    default PartitionIterator executeInternal(ReadContext context, ReadCommand command)
    {
        return getReadExecutable(context, command).executeInternal(context);
    }

}

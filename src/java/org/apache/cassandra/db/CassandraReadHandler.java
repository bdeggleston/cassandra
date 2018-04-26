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

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class CassandraReadHandler implements ReadHandler
{
    private final ColumnFamilyStore cfs;

    public CassandraReadHandler(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
    }

    @Override
    public ReadContext contextForCommand(ReadCommand command)
    {
        Preconditions.checkArgument(command.metadata().id.equals(cfs.metadata.id));
        ColumnFamilyStore indexCfs = maybeGetIndexCfs(cfs, command);

        if (indexCfs == null)
        {
            return new CassandraReadContext(cfs.readOrdering.start(), cfs.metadata(), null, null);
        }
        else
        {
            OpOrder.Group baseOp = null;
            WriteContext writeContext = null;
            ReadContext indexContext = null;
            // OpOrder.start() shouldn't fail, but better safe than sorry.
            try
            {
                baseOp = cfs.readOrdering.start();
                indexContext = indexCfs.getReadHandler().getContext();
                // TODO: this should perhaps not open and maintain a writeOp for the full duration, but instead only *try* to delete stale entries, without blocking if there's no room
                // as it stands, we open a writeOp and keep it open for the duration to ensure that should this CF get flushed to make room we don't block the reclamation of any room being made
                writeContext = cfs.keyspace.getWriteHandler().createContextForRead();
                return new CassandraReadContext(baseOp, cfs.metadata(), indexContext, writeContext);
            }
            catch (RuntimeException e)
            {
                // Note that must have writeContext == null since ReadOrderGroup ctor can't fail
                assert writeContext == null;
                try
                {
                    if (baseOp != null)
                        baseOp.close();
                }
                finally
                {
                    if (indexContext != null)
                        indexContext.close();
                }
                throw e;
            }
        }
    }

    private static ColumnFamilyStore maybeGetIndexCfs(ColumnFamilyStore baseCfs, ReadCommand command)
    {
        Index index = command.getIndex(baseCfs);
        return index == null ? null : index.getBackingTable().orElse(null);
    }

    @Override
    public ReadContext getContext(ReadContext indexReadCtx, WriteContext writeContext)
    {
        return new CassandraReadContext(cfs.readOrdering.start(), cfs.metadata(), null, null);
    }

    @Override
    public ReadExecutable getReadExecutable(ReadContext ctx, ReadCommand command)
    {
        if (command instanceof SinglePartitionReadCommand)
        {
            return new CassandraSinglePartitionRead((SinglePartitionReadCommand) command);
        }
        else if (command instanceof PartitionRangeReadCommand)
        {
            return new CassandraPartitionRangeRead((PartitionRangeReadCommand) command);
        }
        else
        {
            throw new IllegalArgumentException("Unsupported command type: " + command.getClass().getName());
        }
    }
}

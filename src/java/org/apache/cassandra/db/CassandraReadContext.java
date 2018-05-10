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

import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class CassandraReadContext implements ReadContext
{
    private final OpOrder.Group op;
    private final TableMetadata metadata;
    private final ReadContext indexReadCtx;
    private final WriteContext writeCtx;

    public CassandraReadContext(OpOrder.Group op, TableMetadata metadata, ReadContext indexReadCtx, WriteContext writeCtx)
    {
        this.op = op;
        this.metadata = metadata;
        this.indexReadCtx = indexReadCtx;
        this.writeCtx = writeCtx;
    }

    @Override
    public void close()
    {
        try
        {
            if (op != null)
                op.close();
        }
        finally
        {
            if (indexReadCtx != null)
            {
                try
                {
                    indexReadCtx.close();
                }
                finally
                {
                    writeCtx.close();
                }
            }
        }
    }

    @Override
    public WriteContext getWriteContext()
    {
        return writeCtx;
    }

    @Override
    public ReadContext getIndexReadContext()
    {
        return indexReadCtx;
    }

    @Override
    public TableMetadata metadata()
    {
        return metadata;
    }

    @Override
    public boolean validForReadOn(ColumnFamilyStore cfs)
    {
        return op != null && cfs.metadata.id.equals(metadata.id);
    }
}

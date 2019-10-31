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
package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.utils.concurrent.OpOrder;

public final class HeapAllocator extends AbstractAllocator
{
    public static final HeapAllocator instance = new HeapAllocator();

    /**
     * Normally you should use HeapAllocator.instance, since there is no per-Allocator state.
     * This is exposed so that the reflection done by Memtable works when SlabAllocator is disabled.
     */
    private HeapAllocator() {}

    public ByteBuffer allocate(int size)
    {
        return ByteBuffer.allocate(size);
    }

    public boolean allocatingOnHeap()
    {
        return true;
    }
    public Row.Builder cloningBTreeRowBuilder()
    {
        return cloningBTreeRowBuilder(null);
    }

    public Cell cloneCell(Cell from, OpOrder.Group opGroup)
    {
        byte[] val = Arrays.copyOf(from.value(), from.valueSize());
        CellPath path = from.path() == null ? null : from.path().copy(this);
        return new BufferCell(from.column(), from.timestamp(), from.ttl(), from.localDeletionTime(), val, path);
    }
}

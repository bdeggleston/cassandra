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
package org.apache.cassandra.db.rows;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.cassandra.db.ExpirationDateOverflowHandling;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.AbstractAllocator;

public class BufferCell extends AbstractCell
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new BufferCell(ColumnMetadata.regularColumn("", "", "", ByteType.instance), 0L, 0, 0, ByteArrayUtil.EMPTY_BYTE_ARRAY, null));

    private final long timestamp;
    private final int ttl;
    private final int localDeletionTime;

    private final byte[] value;
    private final CellPath path;

    public BufferCell(ColumnMetadata column, long timestamp, int ttl, int localDeletionTime, byte[] value, CellPath path)
    {
        super(column);
        assert !column.isPrimaryKeyColumn();
        assert column.isComplex() == (path != null);
        this.timestamp = timestamp;
        this.ttl = ttl;
        this.localDeletionTime = localDeletionTime;
        this.value = value;
        this.path = path;
    }

    public static BufferCell live(ColumnMetadata column, long timestamp, byte[] value)
    {
        return live(column, timestamp, value, null);
    }

    public static BufferCell live(ColumnMetadata column, long timestamp, byte[] value, CellPath path)
    {
        return new BufferCell(column, timestamp, NO_TTL, NO_DELETION_TIME, value, path);
    }

    public static BufferCell expiring(ColumnMetadata column, long timestamp, int ttl, int nowInSec, byte[] value)
    {
        return expiring(column, timestamp, ttl, nowInSec, value, null);
    }

    public static BufferCell expiring(ColumnMetadata column, long timestamp, int ttl, int nowInSec, byte[] value, CellPath path)
    {
        assert ttl != NO_TTL;
        return new BufferCell(column, timestamp, ttl, ExpirationDateOverflowHandling.computeLocalExpirationTime(nowInSec, ttl), value, path);
    }

    public static BufferCell tombstone(ColumnMetadata column, long timestamp, int nowInSec)
    {
        return tombstone(column, timestamp, nowInSec, null);
    }

    public static BufferCell tombstone(ColumnMetadata column, long timestamp, int nowInSec, CellPath path)
    {
        return new BufferCell(column, timestamp, NO_TTL, nowInSec, ByteArrayUtil.EMPTY_BYTE_ARRAY, path);
    }

    public long timestamp()
    {
        return timestamp;
    }

    public int ttl()
    {
        return ttl;
    }

    public int localDeletionTime()
    {
        return localDeletionTime;
    }

    public byte[] value()
    {
        return value;
    }

    public int valueSize()
    {
        return value.length;
    }

    public void writeValue(DataOutput out) throws IOException
    {
        out.write(value, 0, value.length);
    }

    public int compareValue(Cell that)
    {
        return Cells.compare(this, that, null);
    }

    public int compareValue(Cell that, AbstractType<?> type)
    {
        return Cells.compare(this, that, type);
    }

    public CellPath path()
    {
        return path;
    }

    public Cell withUpdatedColumn(ColumnMetadata newColumn)
    {
        return new BufferCell(newColumn, timestamp, ttl, localDeletionTime, value, path);
    }

    public Cell withUpdatedValue(byte[] newValue)
    {
        return new BufferCell(column, timestamp, ttl, localDeletionTime, newValue, path);
    }

    public Cell withUpdatedTimestampAndLocalDeletionTime(long newTimestamp, int newLocalDeletionTime)
    {
        return new BufferCell(column, newTimestamp, ttl, newLocalDeletionTime, value, path);
    }

    public Cell copy(AbstractAllocator allocator, OpOrder.Group opGroup)
    {
        if (value.length == 0)
            return this;

        return allocator.cloneCell(this, opGroup);
    }

    public long unsharedHeapSizeExcludingData()
    {
        return EMPTY_SIZE + ObjectSizes.sizeOfArray(value) + (path == null ? 0 : path.unsharedHeapSizeExcludingData());
    }
}

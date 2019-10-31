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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.google.common.primitives.Ints;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.MemoryUtil;
import org.apache.cassandra.utils.memory.NativeAllocator;

public class NativeCell extends AbstractCell
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new NativeCell());

    private static final long HAS_CELLPATH = 0;
    private static final long TIMESTAMP = 1;
    private static final long TTL = 9;
    private static final long DELETION = 13;
    private static final long LENGTH = 17;
    private static final long VALUE = 21;

    private final long peer;

    private NativeCell()
    {
        super(null);
        this.peer = 0;
    }

    public NativeCell(NativeAllocator allocator,
                      OpOrder.Group writeOp,
                      Cell cell)
    {
        this(allocator,
             writeOp,
             cell.column(),
             cell.timestamp(),
             cell.ttl(),
             cell.localDeletionTime(),
             cell.value(),
             cell.path());
    }

    public static int totalSize(int valueLength, ColumnMetadata column, CellPath path)
    {
        long size = simpleSize(valueLength);

        assert column.isComplex() == (path != null);
        if (path != null)
        {
            assert path.size() == 1;
            size += 4 + path.get(0).remaining();
        }

        if (size > Integer.MAX_VALUE)
            throw new IllegalStateException();

        return Ints.checkedCast(size);
    }

    public NativeCell(NativeAllocator allocator,
                      OpOrder.Group writeOp,
                      ColumnMetadata column,
                      long timestamp,
                      int ttl,
                      int localDeletionTime,
                      byte[] value,
                      CellPath path)
    {
        this(allocator.allocate(totalSize(value.length, column, path), writeOp),
             column, timestamp, ttl, localDeletionTime, value, path);
    }

    public NativeCell(long peer,
                      ColumnMetadata column,
                      long timestamp,
                      int ttl,
                      int localDeletionTime,
                      byte[] value,
                      CellPath path)
    {
        super(column);
        this.peer = peer;
        MemoryUtil.setByte(peer + HAS_CELLPATH, (byte)(path == null ? 0 : 1));
        MemoryUtil.setLong(peer + TIMESTAMP, timestamp);
        MemoryUtil.setInt(peer + TTL, ttl);
        MemoryUtil.setInt(peer + DELETION, localDeletionTime);
        MemoryUtil.setInt(peer + LENGTH, value.length);
        MemoryUtil.setBytes(peer + VALUE, value, 0, value.length);

        if (path != null)
        {
            ByteBuffer pathbuffer = path.get(0);
            assert pathbuffer.order() == ByteOrder.BIG_ENDIAN;

            long offset = peer + VALUE + value.length;
            MemoryUtil.setInt(offset, pathbuffer.remaining());
            MemoryUtil.setBytes(offset + 4, pathbuffer);
        }
    }

    public NativeCell(long peer, NativeCell from, int size)
    {
        super(from.column);
        this.peer = peer;
        MemoryUtil.setBytes(from.peer, this.peer, size);
    }

    private static long simpleSize(int length)
    {
        return VALUE + length;
    }

    public long timestamp()
    {
        return MemoryUtil.getLong(peer + TIMESTAMP);
    }

    public int ttl()
    {
        return MemoryUtil.getInt(peer + TTL);
    }

    public int localDeletionTime()
    {
        return MemoryUtil.getInt(peer + DELETION);
    }

    public byte[] value()
    {
        int length = valueSize();
        byte[] val = new byte[length];
        MemoryUtil.getBytes(peer + VALUE, val, 0, length);
        return val;
    }

    public int valueSize()
    {
        return MemoryUtil.getInt(peer + LENGTH);
    }

    public int compareValue(Cell that)
    {
        return Cells.compare(this, that, null);
    }

    public int compareValue(Cell that, AbstractType<?> type)
    {
        return Cells.compare(this, that, type);
    }

    public void writeValue(DataOutput out) throws IOException
    {
        out.write(value());
    }

    public CellPath path()
    {
        if (MemoryUtil.getByte(peer+ HAS_CELLPATH) == 0)
            return null;

        long offset = peer + VALUE + MemoryUtil.getInt(peer + LENGTH);
        int size = MemoryUtil.getInt(offset);
        return CellPath.create(MemoryUtil.getByteBuffer(offset + 4, size, ByteOrder.BIG_ENDIAN));
    }

    public Cell withUpdatedValue(byte[] newValue)
    {
        throw new UnsupportedOperationException();
    }

    public Cell withUpdatedTimestampAndLocalDeletionTime(long newTimestamp, int newLocalDeletionTime)
    {
        return new BufferCell(column, newTimestamp, ttl(), newLocalDeletionTime, value(), path());
    }

    public Cell withUpdatedColumn(ColumnMetadata column)
    {
        return new BufferCell(column, timestamp(), ttl(), localDeletionTime(), value(), path());
    }

    public long unsharedHeapSizeExcludingData()
    {
        return EMPTY_SIZE;
    }

}

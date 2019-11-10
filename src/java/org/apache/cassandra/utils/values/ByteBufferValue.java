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

package org.apache.cassandra.utils.values;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;

import com.google.common.base.Preconditions;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.MemoryUtil;

public class ByteBufferValue implements Value
{
    private static final ByteBufferValue EMPTY = new ByteBufferValue(ByteBufferUtil.EMPTY_BYTE_BUFFER);
    private static final long EMPTY_SIZE = ObjectSizes.measure(EMPTY);
    private final ByteBuffer value;

    public ByteBufferValue(ByteBuffer value)
    {
        this.value = value;
    }

    private void maybeUnderflow(int offset, int size)
    {
        if (offset + size > size())
            throw new ValueUnderflowException();
    }

    public boolean equals(Object o)
    {
        return Values.equals(this, o);
    }

    public int hashCode()
    {
        return Values.hashCode(this);
    }

    public int size()
    {
        return value.remaining();
    }

    public boolean isArrayBacked()
    {
        return false;
    }

    public byte[] array()
    {
        if (value.hasArray() && value.position() == 0 && value.limit() == value.capacity())
            return value.array();

        byte[] array = new byte[size()];
        value.get(array);
        return array;
    }

    public boolean isBufferBacked()
    {
        return true;
    }

    public ByteBuffer buffer()
    {
        return value;
    }

    public ByteBuffer safeBuffer()
    {
        return value.duplicate();
    }

    public boolean hasPointer()
    {
        throw new UnsupportedOperationException("TODO");
    }

    public long pointer()
    {
        throw new UnsupportedOperationException("TODO");
    }

    public void write(DataOutputPlus out) throws IOException
    {
        out.write(value);
    }

    public void write(ByteBuf buf)
    {
        buf.writeBytes(value.duplicate());
    }

    public void copyTo(byte[] dst, int srcOffset, int size)
    {
        maybeUnderflow(srcOffset, size);;
        ByteBufferUtil.copyBytes(value, value.position() + srcOffset, dst, 0, size);
    }

    public void copyTo(ByteBuffer dst)
    {
        copyTo(dst, 0, dst.position(), size());
    }

    public void copyTo(ByteBuffer dst, int srcPos, int dstPos, int size)
    {
        Preconditions.checkArgument(dst.remaining() >= size - (dstPos - dst.position()));
        maybeUnderflow(srcPos, size);;
        ByteBufferUtil.copyBytes(value, value.position() + srcPos, dst, dstPos, size);
    }

    public void copyTo(long pointer)
    {
        MemoryUtil.setBytes(pointer, value);
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE + ObjectSizes.sizeOnHeapOf(value);
    }

    public long unsharedHeapSizeExcludingData()
    {
        return EMPTY_SIZE + ObjectSizes.sizeOnHeapExcludingData(value);
    }

    public long getLong(int offset)
    {
        maybeUnderflow(offset, 8);
        return value.getLong(value.position() + offset);
    }

    public int getInt(int offset)
    {
        maybeUnderflow(offset, 4);
        return value.getInt(value.position() + offset);
    }

    public short getShort(int offset)
    {
        maybeUnderflow(offset, 2);
        return value.getShort(value.position() + offset);
    }

    public byte getByte(int offset)
    {
        maybeUnderflow(offset, 1);
        return value.get(value.position() + offset);
    }

    public float getFloat(int offset)
    {
        maybeUnderflow(offset, 4);
        return value.getFloat(value.position() + offset);
    }

    public double getDouble(int offset)
    {
        maybeUnderflow(offset, 8);
        return value.getDouble(value.position() + offset);
    }

    public String getString(int position, int length, Charset charset) throws CharacterCodingException
    {
        maybeUnderflow(position, length);
        return ByteBufferUtil.string(value, value.position() + position, length, charset);
    }

    public Value getMinimal()
    {
        return value.position() > 0 || value.hasRemaining() || !value.hasArray()
               ? new ByteBufferValue(ByteBufferUtil.clone(value))
               : this;
    }

    public boolean isBigEndian()
    {
        return value.order() == ByteOrder.BIG_ENDIAN;
    }

    public static final Factory FACTORY = new Factory()
    {
        public Value read(DataInput in, int length) throws IOException
        {
            return new ByteBufferValue(ByteBufferUtil.read(in, length));
        }

        public Value read(long pointer, int length)
        {
            return new ByteBufferValue(MemoryUtil.getByteBuffer(pointer, length, ByteOrder.BIG_ENDIAN));
        }

        public Value readWithShortLength(DataInput in) throws IOException
        {
            return new ByteBufferValue(ByteBufferUtil.readWithShortLength(in));
        }

        public Value readWithVIntLength(DataInputPlus in) throws IOException
        {
            return new ByteBufferValue(ByteBufferUtil.readWithVIntLength(in));
        }

        public Value empty()
        {
            return EMPTY;
        }

        public Value of(Value v)
        {
            return new ByteBufferValue(v.safeBuffer());
        }

        public Value of(byte b)
        {
            return new ByteBufferValue(ByteBufferUtil.bytes(b));
        }

        public Value of(short s)
        {
            return new ByteBufferValue(ByteBufferUtil.bytes(s));
        }

        public Value of(int i)
        {
            return new ByteBufferValue(ByteBufferUtil.bytes(i));
        }

        public Value of(long n)
        {
            return new ByteBufferValue(ByteBufferUtil.bytes(n));
        }

        public Value of(float f)
        {
            return new ByteBufferValue(ByteBufferUtil.bytes(f));
        }

        public Value of(double d)
        {
            return new ByteBufferValue(ByteBufferUtil.bytes(d));
        }

        public Value of(String s)
        {
            return new ByteBufferValue(ByteBufferUtil.bytes(s));
        }

        public Value of(String s, Charset charset)
        {
            return new ByteBufferValue(ByteBufferUtil.bytes(s, charset));
        }

        public Value of(ByteBuffer buffer)
        {
            return new ByteBufferValue(buffer.duplicate());
        }

        public Value of(byte[] bytes)
        {
            return new ByteBufferValue(ByteBuffer.wrap(bytes));
        }

        public Value fromHex(String s)
        {
            return new ByteBufferValue(ByteBufferUtil.hexToBytes(s));
        }
    };

}

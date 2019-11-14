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
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.util.Arrays;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.MemoryUtil;

public class ByteArrayValue implements Value
{
    private static final ByteArrayValue EMPTY = new ByteArrayValue(ByteArrayUtil.EMPTY_BYTE_ARRAY);
    private static final long EMPTY_SIZE = ObjectSizes.measure(EMPTY);
    private final byte[] value;

    public ByteArrayValue(byte[] value)
    {
        this.value = value;
    }

    private void maybeUnderflow(int offset, int size)
    {
        if (offset + size > size())
            throw new ValueUnderflowException();
    }

    private byte[] slice(int offset, int length)
    {
        maybeUnderflow(offset, length);
        if (offset == 0 && length == size())
            return value;

        return Arrays.copyOfRange(value, offset, offset + length);
    }

    public String toString()
    {
        return "ByteArrayValue{" + Values.toHex(this) + '}';
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
        return value.length;
    }

    public boolean isArrayBacked()
    {
        return true;
    }

    public byte[] array()
    {
        return value;
    }

    public boolean isBufferBacked()
    {
        return false;
    }

    public ByteBuffer buffer()
    {
        return ByteBuffer.wrap(value);
    }

    public ByteBuffer safeBuffer()
    {
        return ByteBuffer.wrap(value);
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
        buf.writeBytes(value);
    }

    public void copyTo(byte[] dst, int srcOffset, int size)
    {
        maybeUnderflow(srcOffset, size);;
        ByteArrayUtil.copyBytes(value, srcOffset, dst, 0, size);
    }

    public void copyTo(ByteBuffer dst)
    {
        copyTo(dst, 0, dst.position(), size());
    }

    public void copyTo(ByteBuffer dst, int srcPos, int dstPos, int size)
    {
        Preconditions.checkArgument(dst.remaining() >= size - (dstPos - dst.position()));
        maybeUnderflow(srcPos, size);;
        ByteArrayUtil.copyBytes(value, srcPos, dst, dstPos, size);
    }

    public void copyTo(long pointer)
    {
        MemoryUtil.setBytes(pointer, value, 0, size());
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE + ObjectSizes.sizeOfArray(value) + value.length;
    }

    public long unsharedHeapSizeExcludingData()
    {
        return EMPTY_SIZE + ObjectSizes.sizeOfArray(value);
    }

    public long getLong(int offset)
    {
        maybeUnderflow(offset, 8);
        return ByteArrayUtil.getLong(value, offset);
    }

    public int getInt(int offset)
    {
        maybeUnderflow(offset, 4);
        return ByteArrayUtil.getInt(value, offset);
    }

    public short getShort(int offset)
    {
        maybeUnderflow(offset, 2);
        return ByteArrayUtil.getShort(value, offset);
    }

    public byte getByte(int offset)
    {
        maybeUnderflow(offset, 1);
        return value[offset];
    }

    public float getFloat(int offset)
    {
        maybeUnderflow(offset, 4);
        return ByteArrayUtil.getFloat(value, offset);
    }

    public double getDouble(int offset)
    {
        maybeUnderflow(offset, 8);
        return ByteArrayUtil.getDouble(value, offset);
    }

    public String getString(int position, int length, Charset charset) throws CharacterCodingException
    {
        maybeUnderflow(position, length);
        if (position == 0 && length == size())
            return new String(value, charset);

        return new String(slice(position, length), charset);
    }

    public Value getMinimal()
    {
        return this;
    }

    public boolean isBigEndian()
    {
        return true;
    }

    public static final Factory FACTORY = new Factory()
    {
        public Value read(DataInput in, int length) throws IOException
        {
            byte[] b = new byte[length];
            in.readFully(b);
            return new ByteArrayValue(b);
        }

        public Value read(long pointer, int length)
        {
            byte[] b = new byte[length];
            MemoryUtil.getBytes(pointer, b, 0, length);
            return new ByteArrayValue(b);
        }

        public Value readWithShortLength(DataInput in) throws IOException
        {
            return read(in, in.readUnsignedShort());
        }

        public Value readWithVIntLength(DataInputPlus in) throws IOException
        {
            return read(in, Ints.checkedCast(in.readUnsignedVInt()));
        }

        public Value empty()
        {
            return EMPTY;
        }

        public Value of(Value v)
        {
            return new ByteArrayValue(v.array());
        }

        public Value of(byte b)
        {
            return new ByteArrayValue(new byte[]{b});
        }

        public Value of(short s)
        {
            return new ByteArrayValue(ByteArrayUtil.bytes(s));
        }

        public Value of(int i)
        {
            return new ByteArrayValue(ByteArrayUtil.bytes(i));
        }

        public Value of(long n)
        {
            return new ByteArrayValue(ByteArrayUtil.bytes(n));
        }

        public Value of(float f)
        {
            return new ByteArrayValue(ByteArrayUtil.bytes(f));
        }

        public Value of(double d)
        {
            return new ByteArrayValue(ByteArrayUtil.bytes(d));
        }

        public Value of(String s)
        {
            return new ByteArrayValue(ByteArrayUtil.bytes(s));
        }

        public Value of(String s, Charset charset)
        {
            return new ByteArrayValue(ByteArrayUtil.bytes(s, charset));
        }

        public Value of(ByteBuffer buffer)
        {
            byte[] b = new byte[buffer.remaining()];
            buffer.duplicate().get(b);
            return new ByteArrayValue(b);
        }

        public Value of(byte[] bytes)
        {
            return new ByteArrayValue(bytes);
        }

        public Value fromHex(String s)
        {
            return new ByteArrayValue(ByteArrayUtil.hexToBytes(s));
        }
    };
}

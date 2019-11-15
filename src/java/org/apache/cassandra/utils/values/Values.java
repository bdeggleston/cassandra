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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

public class Values
{
    public static final Value EMPTY = ByteArrayValue.EMPTY;
    public static final Value UNSET = ByteArrayValue.UNSET;

    private static final Value.Factory FACTORY = ByteArrayValue.FACTORY;

    public static int compareUnsigned(Value left, Value right)
    {
        if (left.isBufferBacked() && right.isBufferBacked())
        {
            return ByteBufferUtil.compareUnsigned(left.buffer(), right.buffer());
        }
        else
        {
            return ByteArrayUtil.compareUnsigned(left.array(), right.array());
        }
    }

    public static int compare(Value left, Value right)
    {
        return compareUnsigned(left, right);
    }

    public static void writeWithShortLength(Value value, DataOutputPlus out) throws IOException
    {
        out.writeShort(value.size());
        value.write(out);
    }

    public static void writeWithVIntLength(Value value, DataOutputPlus out) throws IOException
    {
        out.writeUnsignedVInt(value.size());
        value.write(out);
    }

    public static int getShortLength(Value value, int offset)
    {
        int length = (value.getByte(offset) & 0xFF) << 8;
        return length | (value.getByte(offset + 1) & 0xFF);
    }

    public static Value readWithShortLength(DataInput in) throws IOException
    {
        return FACTORY.readWithShortLength(in);
    }

    public static Value readWithShortLength(Value in, int offset)
    {
        int size = in.getShort(offset);
        return slice(in, offset + 2, size);
    }

    public static int sizeWithShortLength(Value in)
    {
        return 2 + (in == null ? 0 : in.size());
    }

    public static Value readWithVIntLength(DataInputPlus in) throws IOException
    {
        return FACTORY.readWithVIntLength(in);
    }

    public static void skipWithVIntLength(DataInputPlus in) throws IOException
    {
        ByteBufferUtil.skipWithVIntLength(in);
    }

    public static int serializedSizeWithShortLength(Value value)
    {
        int size = value.size();
        return TypeSizes.sizeof((short)size) + size;
    }

    public static int serializedSizeWithVIntLength(Value value)
    {
        int size = value.size();
        return TypeSizes.sizeofUnsignedVInt(size) + size;
    }
    /**
     * @param in data input
     * @throws IOException if an I/O error occurs.
     */
    public static void skipShortLength(DataInputPlus in) throws IOException
    {
        ByteBufferUtil.skipShortLength(in);
    }

    public static Value hexToValue(String source)
    {
        return FACTORY.fromHex(source);
    }

    public static String toHex(Value value)
    {
        if (value.isBufferBacked())
        {
            return ByteBufferUtil.bytesToHex(value.buffer());
        }
        else
        {
            return ByteArrayUtil.bytesToHex(value.array());
        }
    }

    public static Value valueOf(String s)
    {
        return FACTORY.of(s);
    }

    public static Value valueOf(ByteBuffer s)
    {
        if (s == ByteBufferUtil.UNSET_BYTE_BUFFER)
            return UNSET;
        if (s == ByteBufferUtil.EMPTY_BYTE_BUFFER)
            return EMPTY;
        return FACTORY.of(s);
    }

    public static Value valueOf(byte s)
    {
        return FACTORY.of(s);
    }

    public static Value valueOf(byte[] s)
    {
        return FACTORY.of(s);
    }

    public static Value valueOf(short i)
    {
        return FACTORY.of(i);
    }

    public static Value valueOf(int i)
    {
        return FACTORY.of(i);
    }

    public static Value valueOf(long i)
    {
        return FACTORY.of(i);
    }

    public static Value valueOf(float f)
    {
        return FACTORY.of(f);
    }

    public static Value valueOf(double d)
    {
        return FACTORY.of(d);
    }

    public static Value valueOf(UUID uuid)
    {
        return valueOf(UUIDGen.decompose(uuid));
    }

    public static Value valueOf(String s, Charset charset)
    {
        return FACTORY.of(s, charset);
    }

    public static Value read(DataInputPlus in, int length) throws IOException
    {
        return FACTORY.read(in, length);
    }

    public static Value read(long pointer, int length)
    {
        return FACTORY.read(pointer, length);
    }

    public static Value slice(Value input, int offset, int size)
    {
        Preconditions.checkArgument(offset + size <= input.size());
        byte[] bytes = new byte[size];
        input.copyTo(bytes, offset, size);
        return FACTORY.of(bytes);
    }

    public static Value readWithLength(DataInput in) throws IOException
    {
        int length = in.readInt();
        if (length < 0)
            throw new IOException("Corrupt (negative) value length encountered");
        return FACTORY.read(in, length);
    }

    public static void writeWithLength(Value value, DataOutputPlus out) throws IOException
    {
        out.writeInt(value.size());
        value.write(out);
    }

    public static int compareSlice(Value v1, int offset1, Value v2, int offset2, int len)
    {
        if (v1.isBufferBacked() && v2.isBufferBacked())
        {
            ByteBuffer b1 = v1.buffer();
            ByteBuffer b2 = v2.buffer();
            return ByteBufferUtil.compareSubArrays(b1, b1.position() + offset1, b2, b2.position() + offset2, len);
        }
        else
        {
            return ByteArrayUtil.compareUnsigned(v1.array(), offset1, v2.array(), offset2, len);
        }
    }

    public static boolean startsWith(Value value, Value prefix)
    {
        Preconditions.checkArgument(value.isBufferBacked() && prefix.isBufferBacked(), "TODO");
        return ByteBufferUtil.startsWith(value.buffer(), prefix.buffer());
    }

    public static boolean endsWith(Value value, Value suffix)
    {
        Preconditions.checkArgument(value.isBufferBacked() && suffix.isBufferBacked(), "TODO");
        return ByteBufferUtil.endsWith(value.buffer(), suffix.buffer());
    }

    public static boolean contains(Value value, Value subValue)
    {
        Preconditions.checkArgument(value.isBufferBacked() && subValue.isBufferBacked(), "TODO");
        return ByteBufferUtil.contains(value.buffer(), subValue.buffer());
    }

    static int hashCode(Value value)
    {
        int h = 1;
        for (int i=0, size=value.size(); i<size; i++)
            h = 31 * h + (int)value.getByte(i);

        return h;
    }

    static boolean equals(Value left, Object right)
    {
        if (left == right)
            return true;
        if (!(right instanceof Value))
            return false;
        return compare(left, (Value) right) == 0;
    }

    public static long sizeOnHeapOf(Value[] values)
    {
        long size = 0;
        for (Value value: values)
        {
            size += value.unsharedHeapSize();
        }
        return size;
    }

    public static long sizeOnHeapExcludingData(Value[] values)
    {
        long size = 0;
        for (Value value: values)
        {
            size += value.unsharedHeapSizeExcludingData();
        }
        return size;
    }

    public static Value[] toValues(ByteBuffer[] buffers)
    {
        Value[] values = new Value[buffers.length];
        for (int i=0; i<buffers.length; i++)
            values[i] = Values.valueOf(buffers[i]);
        return values;
    }

    public static List<Value> toValues(List<ByteBuffer> buffers)
    {
        List<Value> values = new ArrayList<>(buffers.size());
        for (int i=0, size=buffers.size(); i<size; i++)
            values.add(valueOf(buffers.get(i)));
        return values;
    }
}

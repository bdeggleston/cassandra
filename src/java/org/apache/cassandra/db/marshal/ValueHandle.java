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

package org.apache.cassandra.db.marshal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.util.UUID;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.values.Value;
import org.apache.cassandra.utils.values.Values;

public class ValueHandle implements DataHandle<Value>
{
    public static final DataHandle<Value> instance = new ValueHandle();

    private static final Value TRUE = Values.valueOf((byte) 1);
    private static final Value FALSE = Values.valueOf((byte) 0);

    private ValueHandle() {}

    public int size(Value value)
    {
        return value.size();
    }

    public void write(Value value, DataOutputPlus out) throws IOException
    {
        value.write(out);
    }

    public void write(Value value, ByteBuffer out)
    {
        value.copyTo(out);
        out.position(out.position() + value.size());
    }

    public Value read(DataInputPlus in, int length) throws IOException
    {
        return Values.read(in, length);
    }

    public Value slice(Value input, int offset, int length)
    {
        return Values.slice(input, offset, length);
    }

    public int compareUnsigned(Value left, Value right)
    {
        return Values.compareUnsigned(left, right);
    }

    public ByteBuffer toBuffer(Value value)
    {
        return value.buffer();
    }

    public ByteBuffer toSafeBuffer(Value value)
    {
        return value.safeBuffer();
    }

    public byte[] toArray(Value value)
    {
        return value.array();
    }

    public byte[] toArray(Value value, int offset, int length)
    {
        if (length == value.size())
            return value.array();
        byte[] out = new byte[length];
        value.copyTo(out, offset, length);;
        return out;
    }

    public String toString(Value value, Charset charset) throws CharacterCodingException
    {
        return value.getString(0, charset);
    }

    public Value valueOf(UUID v)
    {
        return UUIDGen.toValue(v);
    }

    public String toHex(Value value)
    {
        return Values.toHex(value);
    }

    public byte toByte(Value value)
    {
        return value.getByte(0);
    }

    public byte getByte(Value value, int offset)
    {
        return value.getByte(offset);
    }

    public short toShort(Value value)
    {
        return value.getShort(0);
    }

    public short getShort(Value value, int offset)
    {
        return value.getShort(offset);
    }

    public int toInt(Value value)
    {
        return value.getInt(0);
    }

    public int getInt(Value value, int offset)
    {
        return value.getInt(offset);
    }

    public long toLong(Value value)
    {
        return value.getLong(0);
    }

    public long getLong(Value value, int offset)
    {
        return value.getLong(offset);
    }

    public float toFloat(Value value)
    {
        return value.getFloat(0);
    }

    public double toDouble(Value value)
    {
        return value.getDouble(0);
    }

    public UUID toUUID(Value value)
    {
        return UUIDGen.getUUID(value);
    }

    public Value empty()
    {
        return Values.EMPTY;
    }

    public Value valueOf(byte[] bytes)
    {
        return Values.valueOf(bytes);
    }

    public Value valueOf(ByteBuffer bytes)
    {
        return Values.valueOf(bytes);
    }

    public Value valueOf(String s, Charset charset)
    {
        return Values.valueOf(s, charset);
    }

    public Value valueOf(boolean v)
    {
        return v ? TRUE : FALSE;
    }

    public Value valueOf(byte v)
    {
        return Values.valueOf(v);
    }

    public Value valueOf(short v)
    {
        return Values.valueOf(v);
    }

    public Value valueOf(int v)
    {
        return Values.valueOf(v);
    }

    public Value valueOf(long v)
    {
        return Values.valueOf(v);
    }

    public Value valueOf(float v)
    {
        return Values.valueOf(v);
    }

    public Value valueOf(double v)
    {
        return Values.valueOf(v);
    }
}

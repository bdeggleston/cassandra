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
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.UUIDGen;

public class ByteBufferHandle implements DataHandle<ByteBuffer>
{
    public static final DataHandle<ByteBuffer> instance = new ByteBufferHandle();
    private ByteBufferHandle() {}

    public int size(ByteBuffer value)
    {
        return value.remaining();
    }

    public void write(ByteBuffer value, DataOutputPlus out) throws IOException
    {
        out.write(value);
    }

    public ByteBuffer read(DataInputPlus in, int length) throws IOException
    {
        return ByteBufferUtil.read(in, length);
    }

    public int compareUnsigned(ByteBuffer left, ByteBuffer right)
    {
        return FastByteOperations.compareUnsigned(left, right);
    }

    public ByteBuffer toBuffer(ByteBuffer value)
    {
        return value;
    }

    public ByteBuffer toSafeBuffer(ByteBuffer value)
    {
        return value.duplicate();
    }

    public byte[] toArray(ByteBuffer value)
    {
        return ByteBufferUtil.getArray(value);
    }

    public String toString(ByteBuffer value, Charset charset) throws CharacterCodingException
    {
        return ByteBufferUtil.string(value, charset);
    }

    public String toHex(ByteBuffer value)
    {
        return ByteBufferUtil.bytesToHex(value);
    }

    public byte toByte(ByteBuffer value)
    {
        return ByteBufferUtil.toByte(value);
    }

    public byte getByte(ByteBuffer value, int offset)
    {
        return value.get(value.position() + offset);
    }

    public short toShort(ByteBuffer value)
    {
        return ByteBufferUtil.toShort(value);
    }

    public short getShort(ByteBuffer value, int offset)
    {
        return value.getShort(value.position() + offset);
    }

    public int toInt(ByteBuffer value)
    {
        return ByteBufferUtil.toInt(value);
    }

    public long toLong(ByteBuffer value)
    {
        return ByteBufferUtil.toLong(value);
    }

    public long getLong(ByteBuffer value, int offset)
    {
        return value.getLong(value.position() + offset);
    }

    public float toFloat(ByteBuffer value)
    {
        return ByteBufferUtil.toFloat(value);
    }

    public double toDouble(ByteBuffer value)
    {
        return ByteBufferUtil.toDouble(value);
    }

    public UUID toUUID(ByteBuffer value)
    {
        return UUIDGen.getUUID(value);
    }
}

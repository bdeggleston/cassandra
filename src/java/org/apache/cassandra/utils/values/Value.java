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
import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ByteBufferUtil;

public interface Value extends Comparable<Value>
{
    // TODO: write to operations

    int size();

    default int sizeFromOffset(int offset)
    {
        return size() - offset;
    }

    default boolean isEmpty()
    {
        return size() == 0;
    }

    default int compareTo(Value that)
    {
        if (this.isBufferBacked() && that.isBufferBacked())
        {
            return ByteBufferUtil.compareUnsigned(this.buffer(), that.buffer());
        }
        else
        {
            return ByteArrayUtil.compare(this.array(), that.array());
        }
    }

    boolean isArrayBacked();

    byte[] array();

    boolean isBufferBacked();

    ByteBuffer buffer();

    /**
     * returns a bytebuffer that can be modified
     */
    ByteBuffer safeBuffer();

    /**
     * requires unsafe support to work
     */
    boolean hasPointer();
    long pointer();

    void write(DataOutputPlus out) throws IOException;
    void write(ByteBuf buf);
    void copyTo(byte[] dst, int srcOffset, int size);
    void copyTo(ByteBuffer dst);
    void copyTo(ByteBuffer dst, int srcPos, int dstPos, int size);
    void copyTo(long pointer);
    long unsharedHeapSize();
    long unsharedHeapSizeExcludingData();

    byte getByte(int offset);

    long getLong(int offset);

    int getInt(int offset);

    short getShort(int offset);

    float getFloat(int offset);

    double getDouble(int offset);

    public String getString(int offset, int length, Charset charset) throws CharacterCodingException;

    default String getString(int offset, Charset charset) throws CharacterCodingException
    {
        return getString(offset, size(), charset);
    }

    default String getString(int offset) throws CharacterCodingException
    {
        return getString(offset, StandardCharsets.UTF_8);
    }

    /**
     * If the value occupies only part of a larger buffer, allocate a new value that is only
     * as large as necessary.
     */
    Value getMinimal();

    boolean isBigEndian();

    public interface Factory
    {
        Value read(DataInput in, int length) throws IOException;
        Value read(long pointer, int length);
        Value readWithShortLength(DataInput in) throws IOException;
        Value readWithVIntLength(DataInputPlus in) throws IOException;
        Value empty();

        Value of(Value v);
        Value of(byte b);
        Value of(short s);
        Value of(int i);
        Value of(long n);
        Value of(float f);
        Value of(double d);
        Value of(String s);
        Value of(String s, Charset charset);
        Value of(ByteBuffer buffer);
        Value of(byte[] bytes);
        Value fromHex(String s);
    }


}

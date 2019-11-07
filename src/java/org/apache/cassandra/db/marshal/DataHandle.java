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

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public interface DataHandle<V>
{
    int size(V value);

    default int sizeFromOffset(V value, int offset)
    {
        return size(value) - offset;
    }

    void write(V value, DataOutputPlus out) throws IOException;
    V read(DataInputPlus in, int length) throws IOException;

    int compareUnsigned(V left, V right);

    ByteBuffer toBuffer(V value);

    /**
     * returns a modifiable buffer
     */
    ByteBuffer toSafeBuffer(V value);

    byte[] toArray(V value);
    String toString(V value, Charset charset) throws CharacterCodingException;
    String toHex(V value);

    byte toByte(V value);
    byte getByte(V value, int offset);
    short getShort(V value, int offset);
    int toInt(V value);
    long toLong(V value);
    long getLong(V value, int offset);
    float toFloat(V value);
    double toDouble(V value);
    UUID toUUID(V value);

    default void writeWithVIntLength(V value, DataOutputPlus out) throws IOException
    {
        out.writeUnsignedVInt(size(value));
        write(value, out);
    }

    default boolean isEmpty(V value)
    {
        return size(value) == 0;
    }

    default int sizeWithVIntLength(V value)
    {
        int size = size(value);
        return TypeSizes.sizeofUnsignedVInt(size) + size;
    }
}

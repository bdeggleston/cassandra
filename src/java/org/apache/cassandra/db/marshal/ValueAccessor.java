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
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringBoundOrBoundary;
import org.apache.cassandra.db.ClusteringBoundary;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;

import static org.apache.cassandra.db.ClusteringPrefix.Kind.*;

public interface ValueAccessor<V>
{
    public interface ObjectFactory<V>
    {
        Cell<V> cell(ColumnMetadata column, long timestamp, int ttl, int localDeletionTime, V value, CellPath path);
        Clustering<V> clustering(V... values);
        Clustering<V> clustering();
        ClusteringBound<V> bound(ClusteringPrefix.Kind kind, V... values);
        ClusteringBound<V> bound(ClusteringPrefix.Kind kind);
        ClusteringBoundary<V> boundary(ClusteringPrefix.Kind kind, V... values);
        default ClusteringBoundOrBoundary<V> boundOrBoundary(ClusteringPrefix.Kind kind, V... values)
        {
            return kind.isBoundary() ? boundary(kind, values) : bound(kind, values);
        }

        default ClusteringBound<V> inclusiveOpen(boolean reversed, V[] boundValues)
        {
            return bound(reversed ? INCL_END_BOUND : INCL_START_BOUND, boundValues);
        }

        default ClusteringBound<V> exclusiveOpen(boolean reversed, V[] boundValues)
        {
            return bound(reversed ? EXCL_END_BOUND : EXCL_START_BOUND, boundValues);
        }

        default ClusteringBound<V> inclusiveClose(boolean reversed, V[] boundValues)
        {
            return bound(reversed ? INCL_START_BOUND : INCL_END_BOUND, boundValues);
        }

        default ClusteringBound<V> exclusiveClose(boolean reversed, V[] boundValues)
        {
            return bound(reversed ? EXCL_START_BOUND : EXCL_END_BOUND, boundValues);
        }

        default ClusteringBoundary<V> inclusiveCloseExclusiveOpen(boolean reversed, V[] boundValues)
        {
            return boundary(reversed ? EXCL_END_INCL_START_BOUNDARY : INCL_END_EXCL_START_BOUNDARY, boundValues);
        }

        default ClusteringBoundary<V> exclusiveCloseInclusiveOpen(boolean reversed, V[] boundValues)
        {
            return boundary(reversed ? INCL_END_EXCL_START_BOUNDARY : EXCL_END_INCL_START_BOUNDARY, boundValues);
        }
    }

    int size(V value);

    default int sizeFromOffset(V value, int offset)
    {
        return size(value) - offset;
    }

    V[] createArray(int length);

    void write(V value, DataOutputPlus out) throws IOException;
    void write(V value, ByteBuffer out);

    <V2> void copyTo(V src, int srcOffset, V2 dst, ValueAccessor<V2> dstAccessor, int dstOffset, int size);

    void copyByteArrayTo(byte[] src, int srcOffset, V dst, ValueAccessor<V> dstAccessor, int dstOffset, int size);
    void copyByteBufferTo(ByteBuffer src, int srcOffset, V dst, ValueAccessor<V> dstAccessor, int dstOffset, int size);

    void digest(V value, int offset, int size, Digest digest);
    default void digest(V value, Digest digest)
    {
        digest(value, 0, size(value), digest);
    }
    V read(DataInputPlus in, int length) throws IOException;
    V slice(V input, int offset, int length);
    default V sliceWithShortLength(V input, int offset)
    {
        int size = getShort(input, offset);
        return slice(input, offset + 2, size);
    }

    default int sizeWithShortLength(V value)
    {
        return 2 + size(value);
    }

    default int getShortLength(V v, int position)
    {
        int length = (getByte(v, position) & 0xFF) << 8;
        return length | (getByte(v, position + 1) & 0xFF);
    }

    int compareUnsigned(V left, V right);

    <V2> int compare(V left, V2 right, ValueAccessor<V2> accessor);

    int compareByteArrayTo(byte[] left, V right, ValueAccessor<V> accessor);
    int compareByteBufferTo(ByteBuffer left, V right, ValueAccessor<V> accessor);

    default int hashCode(V value)
    {
        if (value == null)
            return 0;

        int result = 1;
        for (int i=0, isize=size(value); i<isize; i++)
            result = 31 * result + (int) getByte(value, i);

        return result;
    }

    ByteBuffer toBuffer(V value);

    byte[] toArray(V value);
    byte[] toArray(V value, int offset, int length);
    String toString(V value, Charset charset) throws CharacterCodingException;
    default String toString(V value) throws CharacterCodingException
    {
        return toString(value, StandardCharsets.UTF_8);
    }
    String toHex(V value);

    byte toByte(V value);
    byte getByte(V value, int offset);
    short toShort(V value);
    short getShort(V value, int offset);
    int toInt(V value);
    int getInt(V value, int offset);
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

    int putByte(V dst, int offset, byte value);
    int putShort(V dst, int offset, short value);
    int putInt(V dst, int offset, int value);
    int putFloat(V dst, int offset, float value);
    int putLong(V dst, int offset, long value);
    int putDouble(V dst, int offset, double value);
    int put(V dst, int offset, V src);

    default boolean isEmpty(V value)
    {
        return size(value) == 0;
    }

    default int sizeWithVIntLength(V value)
    {
        int size = size(value);
        return TypeSizes.sizeofUnsignedVInt(size) + size;
    }

    V empty();
    V valueOf(byte[] bytes);
    V valueOf(ByteBuffer bytes);
    V valueOf(String s, Charset charset);
    V valueOf(UUID v);
    V valueOf(boolean v);
    V valueOf(byte v);
    V valueOf(short v);
    V valueOf(int v);
    V valueOf(long v);
    V valueOf(float v);
    V valueOf(double v);

    <V2> V convert(V2 src, ValueAccessor<V2> accessor);

    V allocate(int size);

    ObjectFactory<V> factory();

    public static <L, R> int compareUnsigned(L left, ValueAccessor<L> leftAccessor, R right, ValueAccessor<R> rightAccessor)
    {
        return leftAccessor.compare(left, right, rightAccessor);
    }

    public static <L, R> int compare(L left, ValueAccessor<L> leftAccessor, R right, ValueAccessor<R> rightAccessor)
    {
        return compareUnsigned(left, leftAccessor, right, rightAccessor);
    }

    public static <L, R> int compare(ValueAware<L> left, ValueAware<R> right)
    {
        return compare(left.value(), left.accessor(), right.value(), right.accessor());
    }

    public static <L, R> boolean equals(L left, ValueAccessor<L> accessorL, R right, ValueAccessor<R> accessorR)
    {
        return compare(left, accessorL, right, accessorR) == 0;
    }

    public static <L, R> boolean equals(ValueAware<L> left, ValueAware<R> right)
    {
        return equals(left.value(), left.accessor(), right.value(), right.accessor());
    }
}

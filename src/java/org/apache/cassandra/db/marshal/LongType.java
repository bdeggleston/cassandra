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

import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.LongSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public class LongType extends NumberType<Long>
{
    public static final LongType instance = new LongType();

    LongType() {super(ComparisonType.CUSTOM);} // singleton

    public boolean isEmptyValueMeaningless()
    {
        return true;
    }

    public <V> int compareCustom(V left, V right, DataHandle<V> handle)
    {
        return compareLongs(left, right, handle);
    }

    public static <V> int compareLongs(V left, V right, DataHandle<V> handle)
    {
        if (handle.isEmpty(left)|| handle.isEmpty(right))
            return handle.size(left) - handle.size(right);

        int diff = handle.getByte(left, 0) - handle.getByte(right, 0);
        if (diff != 0)
            return diff;

        return handle.compareUnsigned(left, right);
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {
        // Return an empty ByteBuffer for an empty string.
        if (source.isEmpty())
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        long longType;

        try
        {
            longType = Long.parseLong(source);
        }
        catch (Exception e)
        {
            throw new MarshalException(String.format("Unable to make long from '%s'", source), e);
        }

        return decompose(longType);
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        try
        {
            if (parsed instanceof String)
                return new Constants.TValue(fromString((String) parsed));

            Number parsedNumber = (Number) parsed;
            if (!(parsedNumber instanceof Integer || parsedNumber instanceof Long))
                throw new MarshalException(String.format("Expected a bigint value, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));

            return new Constants.TValue(getSerializer().serialize(parsedNumber.longValue()));
        }
        catch (ClassCastException exc)
        {
            throw new MarshalException(String.format(
                    "Expected a bigint value, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));
        }
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return getSerializer().deserialize(buffer).toString();
    }

    @Override
    public boolean isValueCompatibleWithInternal(AbstractType<?> otherType)
    {
        return this == otherType || otherType == DateType.instance || otherType == TimestampType.instance;
    }

    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.BIGINT;
    }

    public TypeSerializer<Long> getSerializer()
    {
        return LongSerializer.instance;
    }

    @Override
    public int valueLengthIfFixed()
    {
        return 8;
    }

    @Override
    protected int toInt(ByteBuffer value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected float toFloat(ByteBuffer value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected long toLong(ByteBuffer value)
    {
        return ByteBufferUtil.toLong(value);
    }

    public ByteBuffer add(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return ByteBufferUtil.bytes(leftType.toLong(left) + rightType.toLong(right));
    }

    public ByteBuffer substract(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return ByteBufferUtil.bytes(leftType.toLong(left) - rightType.toLong(right));
    }

    public ByteBuffer multiply(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return ByteBufferUtil.bytes(leftType.toLong(left) * rightType.toLong(right));
    }

    public ByteBuffer divide(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return ByteBufferUtil.bytes(leftType.toLong(left) / rightType.toLong(right));
    }

    public ByteBuffer mod(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return ByteBufferUtil.bytes(leftType.toLong(left) % rightType.toLong(right));
    }

    public ByteBuffer negate(ByteBuffer input)
    {
        return ByteBufferUtil.bytes(-toLong(input));
    }
}

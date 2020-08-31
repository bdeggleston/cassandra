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

package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;

public abstract class TypeSerializer<T>
{
    public abstract <V> V serialize(T value, ValueAccessor<V> accessor);

    public final ByteBuffer serializeBuffer(T value)
    {
        return serialize(value, ByteBufferAccessor.instance);
    }

    public abstract <V> T deserialize(V value, ValueAccessor<V> accessor);

    /*
     * Does not modify the position or limit of the buffer even temporarily.
     */
    public final T deserialize(ByteBuffer bytes)
    {
        return deserialize(bytes, ByteBufferAccessor.instance);
    }

    /*
     * Validate that the byte array is a valid sequence for the type this represents.
     * This guarantees deserialize() can be called without errors.
     */
    public abstract <V> void validate(V value, ValueAccessor<V> accessor) throws MarshalException;

    /*
     * Does not modify the position or limit of the buffer even temporarily.
     */
    public final void validate(ByteBuffer bytes) throws MarshalException
    {
        validate(bytes, ByteBufferAccessor.instance);
    }

    public abstract String toString(T value);

    public abstract Class<T> getType();

    public final String toCQLLiteral(ByteBuffer buffer)
    {
        return toCQLLiteral(buffer, ByteBufferAccessor.instance);
    }

    public <V> String toCQLLiteral(V value, ValueAccessor<V> accessor)
    {
        return value == null || accessor.isEmpty(value)
               ? "null"
               : toString(deserialize(value, accessor));
    }
}


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

import org.apache.cassandra.db.marshal.ByteBufferHandle;
import org.apache.cassandra.db.marshal.DataHandle;
import org.apache.cassandra.db.marshal.ValueHandle;
import org.apache.cassandra.utils.values.Value;

public interface TypeSerializer<T>
{
    public <V> V serialize(T value, DataHandle<V> handle);

    public <V> T deserialize(V value, DataHandle<V> handle);

    /*
     * Does not modify the position or limit of the buffer even temporarily.
     */
    default T deserialize(ByteBuffer bytes)
    {
        return deserialize(bytes, ByteBufferHandle.instance);
    }

    default T deserialize(Value value)
    {
        return deserialize(value, ValueHandle.instance);
    }

    /*
     * Validate that the byte array is a valid sequence for the type this represents.
     * This guarantees deserialize() can be called without errors.
     */
    <T> void validate(T value, DataHandle<T> handle) throws MarshalException;

    /*
     * Does not modify the position or limit of the buffer even temporarily.
     */
    default void validate(ByteBuffer bytes) throws MarshalException
    {
        validate(bytes, ByteBufferHandle.instance);
    }

    default void validate(Value value) throws MarshalException
    {
        validate(value, ValueHandle.instance);
    }

    public String toString(T value);

    public Class<T> getType();

    public default String toCQLLiteral(ByteBuffer buffer)
    {
        return buffer == null || !buffer.hasRemaining()
             ? "null"
             : toString(deserialize(buffer));
    }
}


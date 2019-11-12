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

import org.apache.cassandra.db.marshal.DataHandle;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;

public class DoubleSerializer implements TypeSerializer<Double>
{
    public static final DoubleSerializer instance = new DoubleSerializer();

    public <V> Double deserialize(V value, DataHandle<V> handle)
    {
        if (handle.isEmpty(value))
            return null;
        return handle.toDouble(value);
    }

    public <V> V serialize(Double value, DataHandle<V> handle)
    {
        return (value == null) ? handle.empty() : handle.valueOf(value);
    }

    public <T> void validate(T value, DataHandle<T> handle) throws MarshalException
    {
        if (handle.size(value) != 8 && handle.size(value) != 0)
            throw new MarshalException(String.format("Expected 8 or 0 byte value for a double (%d)", handle.size(value)));
    }

    public String toString(Double value)
    {
        return value == null ? "" : value.toString();
    }

    public Class<Double> getType()
    {
        return Double.class;
    }
}

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

import org.apache.cassandra.db.marshal.ValueAccessor;

public class ByteSerializer extends TypeSerializer<Byte>
{
    public static final ByteSerializer instance = new ByteSerializer();

    public <V> Byte deserialize(V value, ValueAccessor<V> handle)
    {
        return value == null || handle.isEmpty(value) ? null : handle.toByte(value);
    }

    public <V> V serializeBuffer(Byte value, ValueAccessor<V> handle)
    {
        return value == null ? handle.empty() : handle.valueOf(value);
    }

    public <V> void validate(V value, ValueAccessor<V> handle) throws MarshalException
    {
        if (handle.size(value) != 1)
            throw new MarshalException(String.format("Expected 1 byte for a tinyint (%d)", handle.size(value)));
    }

    public String toString(Byte value)
    {
        return value == null ? "" : String.valueOf(value);
    }

    public Class<Byte> getType()
    {
        return Byte.class;
    }
}

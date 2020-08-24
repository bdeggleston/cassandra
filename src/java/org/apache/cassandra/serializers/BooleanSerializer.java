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

public class BooleanSerializer extends TypeSerializer<Boolean>
{

    public static final BooleanSerializer instance = new BooleanSerializer();

    public <V> Boolean deserialize(V value, ValueAccessor<V> accessor)
    {
        if (value == null || accessor.isEmpty(value))
            return null;

        return accessor.getByte(value, 0) != 0;
    }

    public <V> V serializeBuffer(Boolean value, ValueAccessor<V> accessor)
    {
        if (value == null)
            return accessor.empty();
        return accessor.valueOf(value);
    }

    public <V> void validate(V value, ValueAccessor<V> accessor) throws MarshalException
    {
        if (accessor.size(value) != 1 && accessor.size(value) != 0)
            throw new MarshalException(String.format("Expected 1 or 0 byte value (%d)", accessor.size(value)));
    }

    public String toString(Boolean value)
    {
        return value == null ? "" : value.toString();
    }

    public Class<Boolean> getType()
    {
        return Boolean.class;
    }
}

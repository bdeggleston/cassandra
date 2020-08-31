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

public class Int32Serializer extends TypeSerializer<Integer>
{
    public static final Int32Serializer instance = new Int32Serializer();

    public <V> Integer deserialize(V value, ValueAccessor<V> accessor)
    {
        return accessor.isEmpty(value) ? null : accessor.toInt(value);
    }

    public <V> V serialize(Integer value, ValueAccessor<V> accessor)
    {
        return value == null ? accessor.empty() : accessor.valueOf(value);
    }

    public <V> void validate(V value, ValueAccessor<V> accessor) throws MarshalException
    {
        if (accessor.size(value) != 4 && accessor.size(value) != 0)
            throw new MarshalException(String.format("Expected 4 or 0 byte int (%d)", accessor.size(value)));
    }

    public String toString(Integer value)
    {
        return value == null ? "" : String.valueOf(value);
    }

    public Class<Integer> getType()
    {
        return Integer.class;
    }
}

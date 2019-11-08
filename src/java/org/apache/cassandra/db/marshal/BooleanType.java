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
import org.apache.cassandra.serializers.BooleanSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BooleanType extends AbstractType<Boolean>
{
    private static final Logger logger = LoggerFactory.getLogger(BooleanType.class);

    public static final BooleanType instance = new BooleanType();

    BooleanType() {super(ComparisonType.CUSTOM);} // singleton

    public boolean isEmptyValueMeaningless()
    {
        return true;
    }

    public <V> int compareCustom(V left, V right, DataHandle<V> handle)
    {
        if (handle.isEmpty(left) || handle.isEmpty(right))
            return handle.size(left) - handle.size(right);

        // False is 0, True is anything else, makes False sort before True.
        int v1 = handle.getByte(left, 0) == 0 ? 0 : 1;
        int v2 = handle.getByte(right, 0) == 0 ? 0 : 1;
        return v1 - v2;
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {

        if (source.isEmpty()|| source.equalsIgnoreCase(Boolean.FALSE.toString()))
            return decomposeBuffer(false);

        if (source.equalsIgnoreCase(Boolean.TRUE.toString()))
            return decomposeBuffer(true);

        throw new MarshalException(String.format("Unable to make boolean from '%s'", source));
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        if (parsed instanceof String)
            return new Constants.TValue(fromString((String) parsed));
        else if (!(parsed instanceof Boolean))
            throw new MarshalException(String.format(
                    "Expected a boolean value, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));

        return new Constants.TValue(getSerializer().serialize((Boolean) parsed, ByteBufferHandle.instance));
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return getSerializer().deserialize(buffer).toString();
    }

    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.BOOLEAN;
    }

    public TypeSerializer<Boolean> getSerializer()
    {
        return BooleanSerializer.instance;
    }

    @Override
    public int valueLengthIfFixed()
    {
        return 1;
    }
}

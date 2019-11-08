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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public class InetAddressSerializer implements TypeSerializer<InetAddress>
{
    public static final InetAddressSerializer instance = new InetAddressSerializer();

    public <V> InetAddress deserialize(V value, DataHandle<V> handle)
    {
        if (handle.isEmpty(value))
            return null;

        try
        {
            return InetAddress.getByAddress(handle.toArray(value));
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    public ByteBuffer serialize(InetAddress value)
    {
        return value == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : ByteBuffer.wrap(value.getAddress());
    }

    public <T> void validate(T value, DataHandle<T> handle) throws MarshalException
    {
        if (handle.isEmpty(value))
            return;

        try
        {
            InetAddress.getByAddress(handle.toArray(value));
        }
        catch (UnknownHostException e)
        {
            throw new MarshalException(String.format("Expected 4 or 16 byte inetaddress; got %s", handle.toHex(value)));
        }
    }

    public String toString(InetAddress value)
    {
        return value == null ? "" : value.getHostAddress();
    }

    public Class<InetAddress> getType()
    {
        return InetAddress.class;
    }
}

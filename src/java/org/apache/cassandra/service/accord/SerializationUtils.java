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

package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;

public class SerializationUtils
{
    public static <T> ByteBuffer serialize(T item, IVersionedSerializer<T> serializer)
    {
        int version = MessagingService.current_version;
        long size = serializer.serializedSize(item, version) + TypeSizes.INT_SIZE;
        try (DataOutputBuffer out = new DataOutputBuffer((int) size))
        {
            out.writeInt(version);
            serializer.serialize(item, out, version);
            return out.buffer(false);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static <T> ByteBuffer[] serialize(List<T> items, IVersionedSerializer<T> serializer)
    {
        ByteBuffer[] result = new ByteBuffer[items.size()];
        for (int i=0,mi=items.size(); i<mi; i++)
            result[i] = serialize(items.get(i), serializer);
        return result;
    }

    public static <T> T deserialize(ByteBuffer bytes, IVersionedSerializer<T> serializer)
    {
        try (DataInputBuffer in = new DataInputBuffer(bytes, true))
        {
            int version = in.readInt();
            return serializer.deserialize(in, version);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static <T> List<T> deserialize(ByteBuffer[] buffers, IVersionedSerializer<T> serializer)
    {
        List<T> result = new ArrayList<>(buffers.length);
        for (ByteBuffer bytes : buffers)
            result.add(deserialize(bytes, serializer));
        return result;
    }

    public static <T> IVersionedSerializer<T> todoSerializer()
    {
        return new IVersionedSerializer<T>()
        {
            @Override
            public void serialize(T t, DataOutputPlus out, int version) throws IOException
            {
                throw new UnsupportedOperationException("TODO");
            }

            @Override
            public T deserialize(DataInputPlus in, int version) throws IOException
            {
                throw new UnsupportedOperationException("TODO");
            }

            @Override
            public long serializedSize(T t, int version)
            {
                throw new UnsupportedOperationException("TODO");
            }
        };
    }
}

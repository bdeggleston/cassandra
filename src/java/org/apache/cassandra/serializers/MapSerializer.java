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

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferHandle;
import org.apache.cassandra.db.marshal.DataHandle;
import org.apache.cassandra.db.marshal.ValueHandle;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.values.Value;
import org.apache.cassandra.utils.values.ValueUnderflowException;

public class MapSerializer<K, V> extends CollectionSerializer<Map<K, V>>
{
    // interning instances
    private static final ConcurrentMap<Pair<TypeSerializer<?>, TypeSerializer<?>>, MapSerializer> instances = new ConcurrentHashMap<Pair<TypeSerializer<?>, TypeSerializer<?>>, MapSerializer>();

    public final TypeSerializer<K> keys;
    public final TypeSerializer<V> values;
    private final Comparator<Pair<Value, Value>> valueComparator;
    private final Comparator<Pair<ByteBuffer, ByteBuffer>> bufferComparator;

    public static <K, V> MapSerializer<K, V> getInstance(TypeSerializer<K> keys, TypeSerializer<V> values, Comparator<ByteBuffer> bufferComparator, Comparator<Value> valueComparator)
    {
        Pair<TypeSerializer<?>, TypeSerializer<?>> p = Pair.<TypeSerializer<?>, TypeSerializer<?>>create(keys, values);
        MapSerializer<K, V> t = instances.get(p);
        if (t == null)
            t = instances.computeIfAbsent(p, k -> new MapSerializer<>(k.left, k.right, bufferComparator, valueComparator) );
        return t;
    }

    private MapSerializer(TypeSerializer<K> keys, TypeSerializer<V> values, Comparator<ByteBuffer> bufferComparator, Comparator<Value> valueComparator)
    {
        this.keys = keys;
        this.values = values;
        this.bufferComparator = (p1, p2) -> bufferComparator.compare(p1.left, p2.left);
        this.valueComparator = (p1, p2) -> valueComparator.compare(p1.left, p2.left);
    }

    private Comparator getComparatorForHandle(DataHandle handle)
    {
        if (handle == ValueHandle.instance)
            return valueComparator;
        if (handle == ByteBufferHandle.instance)
            return bufferComparator;

        throw new AssertionError("Unsupported value handle: " + handle);
    }

    protected <O> List<O> serializeValues(Map<K, V> map, DataHandle<O> handle)
    {
        List<Pair<O, O>> pairs = new ArrayList<>(map.size());
        for (Map.Entry<K, V> entry : map.entrySet())
            pairs.add(Pair.create(keys.serialize(entry.getKey(), handle), values.serialize(entry.getValue(), handle)));

        Collections.sort(pairs, getComparatorForHandle(handle));
        List<O> output = new ArrayList<>(pairs.size() * 2);
        for (Pair<O, O> p : pairs)
        {
            output.add(p.left);
            output.add(p.right);
        }
        return output;
    }

    public int getElementCount(Map<K, V> value)
    {
        return value.size();
    }

    public <V> void validateForNativeProtocol(V input, DataHandle<V> handle, ProtocolVersion version)
    {
        try
        {
            int n = readCollectionSize(input, handle, version);
            int offset = TypeSizes.sizeof(n);
            for (int i = 0; i < n; i++)
            {
                V key = readValue(input, handle, offset, version);
                offset += sizeOfValue(key, handle, version);
                keys.validate(key, handle);

                V value = readValue(input, handle, offset, version);
                offset += sizeOfValue(value, handle, version);
                values.validate(value, handle);
            }
            if (handle.sizeFromOffset(input, offset) != 0)
                throw new MarshalException("Unexpected extraneous bytes after map value");
        }
        catch (ValueUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a map");
        }
    }

    public <I> Map<K, V> deserializeForNativeProtocol(I input, DataHandle<I> handle, ProtocolVersion version)
    {
        try
        {
            int n = readCollectionSize(input, handle, version);

            if (n < 0)
                throw new MarshalException("The data cannot be deserialized as a map");

            int offset = TypeSizes.sizeof(n);

            // If the received bytes are not corresponding to a map, n might be a huge number.
            // In such a case we do not want to initialize the map with that initialCapacity as it can result
            // in an OOM when put is called (see CASSANDRA-12618). On the other hand we do not want to have to resize
            // the map if we can avoid it, so we put a reasonable limit on the initialCapacity.
            Map<K, V> m = new LinkedHashMap<K, V>(Math.min(n, 256));
            for (int i = 0; i < n; i++)
            {
                I key = readValue(input, handle, offset, version);
                offset += sizeOfValue(key, handle, version);
                keys.validate(key, handle);

                I value = readValue(input, handle, offset, version);
                offset += sizeOfValue(value, handle, version);
                values.validate(value, handle);

                m.put(keys.deserialize(key, handle), values.deserialize(value, handle));
            }
            if (handle.sizeFromOffset(input, offset) != 0)
                throw new MarshalException("Unexpected extraneous bytes after map value");
            return m;
        }
        catch (ValueUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a map");
        }
    }

    @Override
    public ByteBuffer getSerializedValue(ByteBuffer collection, ByteBuffer key, AbstractType<?> comparator)
    {
        try
        {
            ByteBuffer input = collection.duplicate();
            int n = readCollectionSize(input, ProtocolVersion.V3);
            int offset = TypeSizes.sizeof(n);
            for (int i = 0; i < n; i++)
            {
                ByteBuffer kbb = readValue(input, ByteBufferHandle.instance, offset, ProtocolVersion.V3);
                offset += sizeOfValue(kbb, ByteBufferHandle.instance, ProtocolVersion.V3);
                int comparison = comparator.compareForCQL(kbb, key);
                if (comparison == 0)
                    return readValue(input, ByteBufferHandle.instance, offset, ProtocolVersion.V3);
                else if (comparison > 0)
                    // since the map is in sorted order, we know we've gone too far and the element doesn't exist
                    return null;
                else // comparison < 0
                    offset += skipValue(input, ByteBufferHandle.instance, offset, ProtocolVersion.V3);
            }
            return null;
        }
        catch (ValueUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a map");
        }
    }

    @Override
    public ByteBuffer getSliceFromSerialized(ByteBuffer collection,
                                             ByteBuffer from,
                                             ByteBuffer to,
                                             AbstractType<?> comparator,
                                             boolean frozen)
    {
        if (from == ByteBufferUtil.UNSET_BYTE_BUFFER && to == ByteBufferUtil.UNSET_BYTE_BUFFER)
            return collection;

        try
        {
            ByteBuffer input = collection.duplicate();
            int n = readCollectionSize(input, ProtocolVersion.V3);
            int startPos = input.position();
            int count = 0;
            boolean inSlice = from == ByteBufferUtil.UNSET_BYTE_BUFFER;

            for (int i = 0; i < n; i++)
            {
                int pos = input.position();
                ByteBuffer kbb = readValue(input, ByteBufferHandle.instance, 0, ProtocolVersion.V3); // key
                input.position(input.position() + sizeOfValue(kbb, ByteBufferHandle.instance, ProtocolVersion.V3));

                // If we haven't passed the start already, check if we have now
                if (!inSlice)
                {
                    int comparison = comparator.compareForCQL(from, kbb);
                    if (comparison <= 0)
                    {
                        // We're now within the slice
                        inSlice = true;
                        startPos = pos;
                    }
                    else
                    {
                        // We're before the slice so we know we don't care about this element
                        skipValue(input, ProtocolVersion.V3); // value
                        continue;
                    }
                }

                // Now check if we're done
                int comparison = to == ByteBufferUtil.UNSET_BYTE_BUFFER ? -1 : comparator.compareForCQL(kbb, to);
                if (comparison > 0)
                {
                    // We're done and shouldn't include the key we just read
                    input.position(pos);
                    break;
                }

                // Otherwise, we'll include that element
                skipValue(input, ProtocolVersion.V3); // value
                ++count;

                // But if we know if was the last of the slice, we break early
                if (comparison == 0)
                    break;
            }

            if (count == 0 && !frozen)
                return null;

            return copyAsNewCollection(collection, count, startPos, input.position(), ProtocolVersion.V3);
        }
        catch (ValueUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a map");
        }
    }

    public String toString(Map<K, V> value)
    {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        boolean isFirst = true;
        for (Map.Entry<K, V> element : value.entrySet())
        {
            if (isFirst)
                isFirst = false;
            else
                sb.append(", ");
            sb.append(keys.toString(element.getKey()));
            sb.append(": ");
            sb.append(values.toString(element.getValue()));
        }
        sb.append('}');
        return sb.toString();
    }

    public Class<Map<K, V>> getType()
    {
        return (Class)Map.class;
    }
}

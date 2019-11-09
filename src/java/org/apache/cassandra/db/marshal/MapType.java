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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.MapSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.Pair;

public class MapType<K, V> extends CollectionType<Map<K, V>>
{
    // interning instances
    private static final ConcurrentHashMap<Pair<AbstractType<?>, AbstractType<?>>, MapType> instances = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Pair<AbstractType<?>, AbstractType<?>>, MapType> frozenInstances = new ConcurrentHashMap<>();

    private final AbstractType<K> keys;
    private final AbstractType<V> values;
    private final MapSerializer<K, V> serializer;
    private final boolean isMultiCell;

    public static MapType<?, ?> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        List<AbstractType<?>> l = parser.getTypeParameters();
        if (l.size() != 2)
            throw new ConfigurationException("MapType takes exactly 2 type parameters");

        return getInstance(l.get(0), l.get(1), true);
    }

    public static <K, V> MapType<K, V> getInstance(AbstractType<K> keys, AbstractType<V> values, boolean isMultiCell)
    {
        ConcurrentHashMap<Pair<AbstractType<?>, AbstractType<?>>, MapType> internMap = isMultiCell ? instances : frozenInstances;
        Pair<AbstractType<?>, AbstractType<?>> p = Pair.create(keys, values);
        MapType<K, V> t = internMap.get(p);
        return null == t
             ? internMap.computeIfAbsent(p, k -> new MapType<>(k.left, k.right, isMultiCell))
             : t;
    }

    private MapType(AbstractType<K> keys, AbstractType<V> values, boolean isMultiCell)
    {
        super(ComparisonType.CUSTOM, Kind.MAP);
        this.keys = keys;
        this.values = values;
        this.serializer = MapSerializer.getInstance(keys.getSerializer(),
                                                    values.getSerializer(),
                                                    (l, r) -> keys.compare(l, r, ByteBufferHandle.instance),
                                                    (l, r) -> keys.compare(l, r, ValueHandle.instance));
        this.isMultiCell = isMultiCell;
    }

    @Override
    public boolean referencesUserType(ByteBuffer name)
    {
        return keys.referencesUserType(name) || values.referencesUserType(name);
    }

    @Override
    public MapType<?,?> withUpdatedUserType(UserType udt)
    {
        if (!referencesUserType(udt.name))
            return this;

        (isMultiCell ? instances : frozenInstances).remove(Pair.create(keys, values));

        return getInstance(keys.withUpdatedUserType(udt), values.withUpdatedUserType(udt), isMultiCell);
    }

    @Override
    public AbstractType<?> expandUserTypes()
    {
        return getInstance(keys.expandUserTypes(), values.expandUserTypes(), isMultiCell);
    }

    @Override
    public boolean referencesDuration()
    {
        // Maps cannot be created with duration as keys
        return getValuesType().referencesDuration();
    }

    public AbstractType<K> getKeysType()
    {
        return keys;
    }

    public AbstractType<V> getValuesType()
    {
        return values;
    }

    public AbstractType<K> nameComparator()
    {
        return keys;
    }

    public AbstractType<V> valueComparator()
    {
        return values;
    }

    @Override
    public boolean isMultiCell()
    {
        return isMultiCell;
    }

    @Override
    public AbstractType<?> freeze()
    {
        if (isMultiCell)
            return getInstance(this.keys, this.values, false);
        else
            return this;
    }

    @Override
    public AbstractType<?> freezeNestedMulticellTypes()
    {
        if (!isMultiCell())
            return this;

        AbstractType<?> keyType = (keys.isFreezable() && keys.isMultiCell())
                                ? keys.freeze()
                                : keys.freezeNestedMulticellTypes();

        AbstractType<?> valueType = (values.isFreezable() && values.isMultiCell())
                                  ? values.freeze()
                                  : values.freezeNestedMulticellTypes();

        return getInstance(keyType, valueType, isMultiCell);
    }

    @Override
    public boolean isCompatibleWithFrozen(CollectionType<?> previous)
    {
        assert !isMultiCell;
        MapType tprev = (MapType) previous;
        return keys.isCompatibleWith(tprev.keys) && values.isCompatibleWith(tprev.values);
    }

    @Override
    public boolean isValueCompatibleWithFrozen(CollectionType<?> previous)
    {
        assert !isMultiCell;
        MapType tprev = (MapType) previous;
        return keys.isCompatibleWith(tprev.keys) && values.isValueCompatibleWith(tprev.values);
    }

    public <V> int compareCustom(V left, V right, DataHandle<V> handle)
    {
        return compareMaps(keys, values, left, right, handle);
    }

    public static <V> int compareMaps(AbstractType<?> keysComparator, AbstractType<?> valuesComparator, V left, V right, DataHandle<V> handle)
    {
        if (handle.isEmpty(left) || handle.isEmpty(right))
            return Boolean.compare(handle.isEmpty(right), handle.isEmpty(left));


        ProtocolVersion protocolVersion = ProtocolVersion.V3;
        int size1 = CollectionSerializer.readCollectionSize(left, handle, protocolVersion);
        int size2 = CollectionSerializer.readCollectionSize(right, handle, protocolVersion);

        int offset1 = TypeSizes.sizeof(size1);
        int offset2 = TypeSizes.sizeof(size2);

        for (int i = 0; i < Math.min(size1, size2); i++)
        {
            V k1 = CollectionSerializer.readValue(left, handle, offset1, protocolVersion);
            offset1 += CollectionSerializer.sizeOfValue(k1, handle, protocolVersion);
            V k2 = CollectionSerializer.readValue(right, handle, offset2, protocolVersion);
            offset2 += CollectionSerializer.sizeOfValue(k2, handle, protocolVersion);
            int cmp = keysComparator.compare(k1, k2, handle);
            if (cmp != 0)
                return cmp;

            V v1 = CollectionSerializer.readValue(left, handle, offset1, protocolVersion);
            offset1 += CollectionSerializer.sizeOfValue(v1, handle, protocolVersion);
            V v2 = CollectionSerializer.readValue(right, handle, offset2, protocolVersion);
            offset2 += CollectionSerializer.sizeOfValue(v2, handle, protocolVersion);
            cmp = valuesComparator.compare(v1, v2, handle);
            if (cmp != 0)
                return cmp;
        }

        return size1 == size2 ? 0 : (size1 < size2 ? -1 : 1);
    }

    @Override
    public MapSerializer<K, V> getSerializer()
    {
        return serializer;
    }

    @Override
    protected int collectionSize(List<ByteBuffer> values)
    {
        return values.size() / 2;
    }

    public String toString(boolean ignoreFreezing)
    {
        boolean includeFrozenType = !ignoreFreezing && !isMultiCell();

        StringBuilder sb = new StringBuilder();
        if (includeFrozenType)
            sb.append(FrozenType.class.getName()).append("(");
        sb.append(getClass().getName()).append(TypeParser.stringifyTypeParameters(Arrays.asList(keys, values), ignoreFreezing || !isMultiCell));
        if (includeFrozenType)
            sb.append(")");
        return sb.toString();
    }

    public List<ByteBuffer> serializedValues(Iterator<Cell> cells)
    {
        assert isMultiCell;
        List<ByteBuffer> bbs = new ArrayList<ByteBuffer>();
        while (cells.hasNext())
        {
            Cell c = cells.next();
            bbs.add(c.path().get(0));
            bbs.add(c.value().buffer());
        }
        return bbs;
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        if (parsed instanceof String)
            parsed = Json.decodeJson((String) parsed);

        if (!(parsed instanceof Map))
            throw new MarshalException(String.format(
                    "Expected a map, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));

        Map<Object, Object> map = (Map<Object, Object>) parsed;
        Map<Term, Term> terms = new HashMap<>(map.size());
        for (Map.Entry<Object, Object> entry : map.entrySet())
        {
            if (entry.getKey() == null)
                throw new MarshalException("Invalid null key in map");

            if (entry.getValue() == null)
                throw new MarshalException("Invalid null value in map");

            terms.put(keys.fromJSONObject(entry.getKey()), values.fromJSONObject(entry.getValue()));
        }
        return new Maps.DelayedValue(keys, terms);
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        ByteBuffer value = buffer.duplicate();
        StringBuilder sb = new StringBuilder("{");
        int size = CollectionSerializer.readCollectionSize(value, protocolVersion);
        int offset = TypeSizes.sizeof(size);
        for (int i = 0; i < size; i++)
        {
            if (i > 0)
                sb.append(", ");

            // map keys must be JSON strings, so convert non-string keys to strings
            ByteBuffer kv = CollectionSerializer.readValue(value, ByteBufferHandle.instance, offset, protocolVersion);
            offset += CollectionSerializer.sizeOfValue(kv, ByteBufferHandle.instance, protocolVersion);
            String key = keys.toJSONString(kv, protocolVersion);
            if (key.startsWith("\""))
                sb.append(key);
            else
                sb.append('"').append(Json.quoteAsJsonString(key)).append('"');

            sb.append(": ");
            ByteBuffer vv = CollectionSerializer.readValue(value, ByteBufferHandle.instance, offset, protocolVersion);
            offset += CollectionSerializer.sizeOfValue(vv, ByteBufferHandle.instance, protocolVersion);
            sb.append(values.toJSONString(vv, protocolVersion));
        }
        return sb.append("}").toString();
    }
}

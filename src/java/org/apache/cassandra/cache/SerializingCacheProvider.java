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
package org.apache.cassandra.cache;

import java.io.DataInput;
import java.io.IOException;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilySerializer;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.IAllocator;
import org.apache.cassandra.net.MessagingService;

public class SerializingCacheProvider
{

    private final ColumnFamilySerializer columnFamilySerializer;

    public SerializingCacheProvider(ColumnFamilySerializer columnFamilySerializer)
    {
        this.columnFamilySerializer = columnFamilySerializer;
    }

    public ICache<RowCacheKey, IRowCacheEntry> create(long capacity, IAllocator allocator)
    {
        return SerializingCache.create(capacity, new RowCacheSerializer(columnFamilySerializer), allocator);
    }

    // Package protected for tests
    static class RowCacheSerializer implements ISerializer<IRowCacheEntry>
    {

        private final ColumnFamilySerializer columnFamilySerializer;

        RowCacheSerializer(ColumnFamilySerializer columnFamilySerializer)
        {
            this.columnFamilySerializer = columnFamilySerializer;
        }

        public void serialize(IRowCacheEntry entry, DataOutputPlus out) throws IOException
        {
            assert entry != null; // unlike CFS we don't support nulls, since there is no need for that in the cache
            boolean isSentinel = entry instanceof RowCacheSentinel;
            out.writeBoolean(isSentinel);
            if (isSentinel)
                out.writeLong(((RowCacheSentinel) entry).sentinelId);
            else
                columnFamilySerializer.serialize((ColumnFamily) entry, out, MessagingService.current_version);
        }

        public IRowCacheEntry deserialize(DataInput in) throws IOException
        {
            boolean isSentinel = in.readBoolean();
            if (isSentinel)
                return new RowCacheSentinel(in.readLong());
            return columnFamilySerializer.deserialize(in, MessagingService.current_version);
        }

        public long serializedSize(IRowCacheEntry entry, TypeSizes typeSizes)
        {
            int size = typeSizes.sizeof(true);
            if (entry instanceof RowCacheSentinel)
                size += typeSizes.sizeof(((RowCacheSentinel) entry).sentinelId);
            else
                size += columnFamilySerializer.serializedSize((ColumnFamily) entry, typeSizes, MessagingService.current_version);
            return size;
        }
    }
}

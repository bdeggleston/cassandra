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

package org.apache.cassandra.service.accord.txn;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import accord.api.Key;
import accord.api.Store;
import accord.api.Write;
import accord.local.CommandStore;
import accord.txn.Timestamp;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.AccordCommandsForKey;
import org.apache.cassandra.service.accord.SerializationUtils;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import static org.apache.cassandra.service.accord.SerializationUtils.deserializeArray;
import static org.apache.cassandra.service.accord.SerializationUtils.partitionUpdateSerializer;
import static org.apache.cassandra.service.accord.SerializationUtils.serializeArray;

public class TxnWrite extends AbstractKeySorted<TxnWrite.Update> implements Write
{
    public static final TxnWrite EMPTY = new TxnWrite(Collections.emptyList());

    public static class Update extends AbstractSerialized<PartitionUpdate>
    {
        public final PartitionKey key;
        public final int index;

        public Update(PartitionKey key, int index, PartitionUpdate update)
        {
            super(update);
            this.key = key;
            this.index = index;
        }

        private Update(PartitionKey key, int index, ByteBuffer bytes)
        {
            super(bytes);
            this.key = key;
            this.index = index;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Update update = (Update) o;
            return index == update.index && key.equals(update.key);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(super.hashCode(), key, index);
        }

        @Override
        public String toString()
        {
            return "Complete{" +
                   "key=" + key +
                   ", index=" + index +
                   ", update=" + get() +
                   '}';
        }

        public Future<?> write(long timestamp, int nowInSeconds)
        {
            PartitionUpdate update = new PartitionUpdate.Builder(get(), 0).updateAllTimestampAndLocalDeletionTime(timestamp, nowInSeconds).build();
            Mutation mutation = new Mutation(update);
            return Stage.MUTATION.submit((Runnable) mutation::apply);
        }

        @Override
        protected IVersionedSerializer<PartitionUpdate> serializer()
        {
            return partitionUpdateSerializer;
        }

        public static final IVersionedSerializer<Update> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(Update write, DataOutputPlus out, int version) throws IOException
            {
                PartitionKey.serializer.serialize(write.key, out, version);
                out.writeInt(write.index);
                ByteBufferUtil.writeWithVIntLength(write.bytes(), out);

            }

            @Override
            public Update deserialize(DataInputPlus in, int version) throws IOException
            {
                PartitionKey key = PartitionKey.serializer.deserialize(in, version);
                int index = in.readInt();
                ByteBuffer bytes = ByteBufferUtil.readWithVIntLength(in);
                return new Update(key, index, bytes);
            }

            @Override
            public long serializedSize(Update write, int version)
            {
                long size = 0;
                size += PartitionKey.serializer.serializedSize(write.key, version);
                size += TypeSizes.INT_SIZE;
                size += ByteBufferUtil.serializedSizeWithVIntLength(write.bytes());
                return size;
            }
        };
    }


    /**
     * Partition update that can later be supplemented with data from the read phase
     */
    public static class Fragment
    {
        public final PartitionKey key;
        public final int index;
        public final PartitionUpdate baseUpdate;

        public Fragment(PartitionKey key, int index, PartitionUpdate baseUpdate)
        {
            this.key = key;
            this.index = index;
            this.baseUpdate = baseUpdate;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Fragment fragment = (Fragment) o;
            return index == fragment.index && key.equals(fragment.key) && baseUpdate.equals(fragment.baseUpdate);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(key, index, baseUpdate);
        }

        @Override
        public String toString()
        {
            return "Fragment{" +
                   "key=" + key +
                   ", index=" + index +
                   ", baseUpdate=" + baseUpdate +
                   '}';
        }

        public Update complete(TxnData data)
        {
            // TODO: perform column reference substitution
            return new Update(key, index, baseUpdate);
        }

        public static final IVersionedSerializer<Fragment> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(Fragment fragment, DataOutputPlus out, int version) throws IOException
            {
                PartitionKey.serializer.serialize(fragment.key, out, version);
                out.writeInt(fragment.index);
                partitionUpdateSerializer.serialize(fragment.baseUpdate, out, version);
            }

            @Override
            public Fragment deserialize(DataInputPlus in, int version) throws IOException
            {
                PartitionKey key = PartitionKey.serializer.deserialize(in, version);
                int idx = in.readInt();
                PartitionUpdate baseUpdate = partitionUpdateSerializer.deserialize(in, version);
                return new Fragment(key, idx, baseUpdate);
            }

            @Override
            public long serializedSize(Fragment fragment, int version)
            {
                long size = 0;
                size += PartitionKey.serializer.serializedSize(fragment.key, version);
                size += TypeSizes.INT_SIZE;
                size += partitionUpdateSerializer.serializedSize(fragment.baseUpdate, version);
                return size;
            }
        };
    }

    private TxnWrite(Update[] items)
    {
        super(items);
    }

    public TxnWrite(List<Update> items)
    {
        super(items);
    }

    @Override
    int compareNonKeyFields(Update left, Update right)
    {
        return Integer.compare(left.index, right.index);
    }

    @Override
    PartitionKey getKey(Update item)
    {
        return item.key;
    }

    @Override
    Update[] newArray(int size)
    {
        return new Update[size];
    }

    @Override
    public Future<?> apply(Key key, CommandStore commandStore, Timestamp executeAt, Store store)
    {
        AccordCommandsForKey cfk = (AccordCommandsForKey) commandStore.commandsForKey(key);
        long timestamp = cfk.timestampMicrosFor(executeAt);
        int nowInSeconds = cfk.nowInSecondsFor(executeAt);

        List<Future<?>> futures = new ArrayList<>();
        forEachForKey((PartitionKey) key, write -> futures.add(write.write(timestamp, nowInSeconds)));

        if (futures.isEmpty())
            return ImmediateFuture.success(new TxnData());

        if (futures.size() == 1)
            return futures.get(0);

        return FutureCombiner.allOf(futures);
    }

    public static final IVersionedSerializer<TxnWrite> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(TxnWrite write, DataOutputPlus out, int version) throws IOException
        {
            serializeArray(write.items, out, version, Update.serializer);
        }

        @Override
        public TxnWrite deserialize(DataInputPlus in, int version) throws IOException
        {
            return new TxnWrite(deserializeArray(in, version, Update.serializer, Update[]::new));
        }

        @Override
        public long serializedSize(TxnWrite write, int version)
        {
            return SerializationUtils.serializedArraySize(write.items, version, Update.serializer);
        }
    };
}

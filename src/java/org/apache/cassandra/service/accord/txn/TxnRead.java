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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import com.google.common.collect.Maps;

import accord.api.Data;
import accord.api.Key;
import accord.api.Read;
import accord.api.Store;
import accord.local.CommandStore;
import accord.txn.Timestamp;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import static org.apache.cassandra.service.accord.SerializationUtils.deserializeArray;
import static org.apache.cassandra.service.accord.SerializationUtils.serializeArray;
import static org.apache.cassandra.service.accord.SerializationUtils.serializedArraySize;

public class TxnRead extends AbstractKeySorted<TxnNamedRead> implements Read
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new TxnRead(new TxnNamedRead[0]));
    private final Map<String, TxnNamedRead> readsByName;

    private static Map<String, TxnNamedRead> indexReads(TxnNamedRead[] reads)
    {
        Map<String, TxnNamedRead> index = Maps.newHashMapWithExpectedSize(reads.length);
        for (TxnNamedRead read : reads)
            if (index.put(read.name(), read) != null)
                throw new IllegalArgumentException("More than one read is named " + read.name());
        return index;
    }

    public TxnRead(TxnNamedRead[] items)
    {
        super(items);
        this.readsByName = indexReads(this.items);
    }

    public TxnRead(List<TxnNamedRead> items)
    {
        super(items);
        this.readsByName = indexReads(this.items);
    }

    public long estimatedSizeOnHeap()
    {
        long size = EMPTY_SIZE;
        for (TxnNamedRead read : items)
            size += read.estimatedSizeOnHeap();
        return size;
    }

    @Override
    int compareNonKeyFields(TxnNamedRead left, TxnNamedRead right)
    {
        return left.name().compareTo(right.name());
    }

    @Override
    PartitionKey getKey(TxnNamedRead read)
    {
        return read.key();
    }

    @Override
    TxnNamedRead[] newArray(int size)
    {
        return new TxnNamedRead[size];
    }

    @Override
    public Future<Data> read(Key key, CommandStore commandStore, Timestamp executeAt, Store store)
    {
        List<Future<Data>> futures = new ArrayList<>();
        forEachForKey((PartitionKey) key, read -> futures.add(read.read(commandStore, executeAt)));

        if (futures.isEmpty())
            return ImmediateFuture.success(new TxnData());

        if (futures.size() == 1)
            return futures.get(0);

        return new MultiReadFuture(futures);
    }

    static class MultiReadFuture extends AsyncPromise<Data> implements BiConsumer<Data, Throwable>
    {
        private Data result = null;
        private int pending = 0;

        public MultiReadFuture(List<Future<Data>> futures)
        {
            pending = futures.size();
            listen(futures);
        }

        private synchronized void listen(List<Future<Data>> futures)
        {
            for (int i=0, mi=futures.size(); i<mi; i++)
                futures.get(i).addCallback(this);
        }

        @Override
        public synchronized void accept(Data data, Throwable throwable)
        {
            if (isDone())
                return;

            if (throwable != null)
                tryFailure(throwable);

            result = result != null ? result.merge(data) : data;
            if (--pending == 0)
                trySuccess(result);
        }
    }

    public static final IVersionedSerializer<TxnRead> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(TxnRead read, DataOutputPlus out, int version) throws IOException
        {
            serializeArray(read.items, out, version, TxnNamedRead.serializer);
        }

        @Override
        public TxnRead deserialize(DataInputPlus in, int version) throws IOException
        {
            return new TxnRead(deserializeArray(in, version, TxnNamedRead.serializer, TxnNamedRead[]::new));
        }

        @Override
        public long serializedSize(TxnRead read, int version)
        {
            return serializedArraySize(read.items, version, TxnNamedRead.serializer);
        }
    };
}

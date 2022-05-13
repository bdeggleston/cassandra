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
import java.util.Objects;

import accord.api.Data;
import accord.local.CommandStore;
import accord.primitives.Timestamp;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.AccordCommandsForKey;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.service.accord.SerializationUtils.singlePartitionReadCommandSerializer;
import static org.apache.cassandra.utils.ByteBufferUtil.readWithVIntLength;
import static org.apache.cassandra.utils.ByteBufferUtil.serializedSizeWithVIntLength;
import static org.apache.cassandra.utils.ByteBufferUtil.writeWithVIntLength;

public class TxnNamedRead extends AbstractSerialized<SinglePartitionReadCommand>
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new TxnNamedRead("", null, null));
    private final String name;
    private final PartitionKey key;

    private TxnNamedRead(String name, PartitionKey key, ByteBuffer bytes)
    {
        super(bytes);
        this.name = name;
        this.key = key;
    }

    public TxnNamedRead(String name, SinglePartitionReadCommand value)
    {
        super(value);
        this.name = name;
        this.key = new PartitionKey(value.metadata().id, value.partitionKey());
    }

    public long estimatedSizeOnHeap()
    {
        return EMPTY_SIZE + name.length() + key.estimatedSizeOnHeap() + ByteBufferUtil.estimatedSizeOnHeap(bytes());
    }

    @Override
    protected IVersionedSerializer<SinglePartitionReadCommand> serializer()
    {
        return singlePartitionReadCommandSerializer;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        TxnNamedRead namedRead = (TxnNamedRead) o;
        return name.equals(namedRead.name) && key.equals(namedRead.key);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), name, key);
    }

    @Override
    public String toString()
    {
        return "TxnNamedRead{" +
               "name='" + name + '\'' +
               ", key=" + key +
               ", update=" + get() +
               '}';
    }

    public String name()
    {
        return name;
    }

    public PartitionKey key()
    {
        return key;
    }

    public Future<Data> read(CommandStore commandStore, Timestamp executeAt)
    {
        SinglePartitionReadCommand command = get();
        AccordCommandsForKey cfk = (AccordCommandsForKey) commandStore.commandsForKey(key);
        int nowInSeconds = cfk.nowInSecondsFor(executeAt);
        AsyncPromise<Data> future = new AsyncPromise<>();
        Stage.READ.execute(() -> {
            SinglePartitionReadCommand read = command.withNowInSec(nowInSeconds);
            try (ReadExecutionController controller = read.executionController();
                 UnfilteredPartitionIterator partition = read.executeLocally(controller))
            {
                PartitionIterator iterator = UnfilteredPartitionIterators.filter(partition, read.nowInSec());
                FilteredPartition filtered = FilteredPartition.create(PartitionIterators.getOnlyElement(iterator, read));
                TxnData result = new TxnData();
                result.put(name, filtered);
                future.trySuccess(result);
            }
        });

        return future;
    }

    public static final IVersionedSerializer<TxnNamedRead> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(TxnNamedRead read, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(read.name);
            PartitionKey.serializer.serialize(read.key, out, version);
            writeWithVIntLength(read.bytes(), out);
        }

        @Override
        public TxnNamedRead deserialize(DataInputPlus in, int version) throws IOException
        {
            String name = in.readUTF();
            PartitionKey key = PartitionKey.serializer.deserialize(in, version);
            ByteBuffer bytes = readWithVIntLength(in);
            return new TxnNamedRead(name, key, bytes);
        }

        @Override
        public long serializedSize(TxnNamedRead read, int version)
        {
            long size = 0;
            size += TypeSizes.sizeof(read.name);
            size += PartitionKey.serializer.serializedSize(read.key, version);
            size += serializedSizeWithVIntLength(read.bytes());
            return size;
        }
    };
}

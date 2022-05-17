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

import accord.api.Data;
import accord.local.CommandStore;
import accord.txn.Timestamp;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
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
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

public class TxnNamedRead extends AbstractSerialized<SinglePartitionReadCommand>
{
    private final String name;
    private final PartitionKey key;

    public TxnNamedRead(String name, PartitionKey key, ByteBuffer bytes)
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

    @Override
    protected IVersionedSerializer<SinglePartitionReadCommand> serializer()
    {
        return serializer;
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

    private static final IVersionedSerializer<SinglePartitionReadCommand> serializer = new IVersionedSerializer<SinglePartitionReadCommand>()
    {
        @Override
        public void serialize(SinglePartitionReadCommand command, DataOutputPlus out, int version) throws IOException
        {
            SinglePartitionReadCommand.serializer.serialize(command, out, version);
        }

        @Override
        public SinglePartitionReadCommand deserialize(DataInputPlus in, int version) throws IOException
        {
            return (SinglePartitionReadCommand) SinglePartitionReadCommand.serializer.deserialize(in, version);
        }

        @Override
        public long serializedSize(SinglePartitionReadCommand command, int version)
        {
            return SinglePartitionReadCommand.serializer.serializedSize(command, version);
        }
    };
}

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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import accord.api.Data;
import accord.api.Update;
import accord.api.Write;
import accord.primitives.Keys;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.SerializationUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

import static org.apache.cassandra.service.accord.SerializationUtils.deserialize;
import static org.apache.cassandra.service.accord.SerializationUtils.serialize;
import static org.apache.cassandra.service.accord.SerializationUtils.serializeArray;
import static org.apache.cassandra.utils.ByteBufferUtil.readWithVIntLength;
import static org.apache.cassandra.utils.ByteBufferUtil.serializedSizeWithVIntLength;
import static org.apache.cassandra.utils.ByteBufferUtil.writeWithVIntLength;

public class TxnUpdate implements Update
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new TxnUpdate(new ByteBuffer[0], null));

    private final ByteBuffer[] serializedUpdates;
    private final ByteBuffer serializedCondition;

    public TxnUpdate(List<TxnWrite.Fragment> updates, TxnCondition condition)
    {
        this.serializedUpdates = serialize(updates, TxnWrite.Fragment.serializer);
        this.serializedCondition = serialize(condition, TxnCondition.serializer);
    }

    public TxnUpdate(ByteBuffer[] serializedUpdates, ByteBuffer serializedCondition)
    {
        this.serializedUpdates = serializedUpdates;
        this.serializedCondition = serializedCondition;
    }

    public ByteBuffer serializedCondition()
    {
        return serializedCondition.duplicate();
    }

    public long estimatedSizeOnHeap()
    {
        long size = EMPTY_SIZE + ByteBufferUtil.estimatedSizeOnHeap(serializedCondition);
        for (ByteBuffer update : serializedUpdates)
            size += ByteBufferUtil.estimatedSizeOnHeap(update);
        return size;
    }

    @Override
    public String toString()
    {
        return "TxnUpdate{" +
               "updates=" + deserialize(serializedUpdates, TxnWrite.Fragment.serializer) +
               ", condition=" + deserialize(serializedCondition, TxnCondition.serializer) +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TxnUpdate txnUpdate = (TxnUpdate) o;
        return Arrays.equals(serializedUpdates, txnUpdate.serializedUpdates) && Objects.equals(serializedCondition, txnUpdate.serializedCondition);
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash(serializedCondition);
        result = 31 * result + Arrays.hashCode(serializedUpdates);
        return result;
    }

    @Override
    public Keys keys()
    {
        // TODO: Does this actually work?
        throw new UnsupportedOperationException();
    }

    @Override
    public Write apply(Data data)
    {
        TxnCondition condition = deserialize(serializedCondition, TxnCondition.serializer);

        if (!condition.applies((TxnData) data))
            return TxnWrite.EMPTY;

        List<TxnWrite.Fragment> fragments = deserialize(serializedUpdates, TxnWrite.Fragment.serializer);
        List<TxnWrite.Update> updates = new ArrayList<>(fragments.size());
        for (TxnWrite.Fragment fragment : fragments)
            updates.add(fragment.complete((TxnData) data));

        return new TxnWrite(updates);
    }

    public static final IVersionedSerializer<TxnUpdate> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(TxnUpdate update, DataOutputPlus out, int version) throws IOException
        {
            writeWithVIntLength(update.serializedCondition, out);
            serializeArray(update.serializedUpdates, out, version, ByteBufferUtil.vintSerializer);
        }

        @Override
        public TxnUpdate deserialize(DataInputPlus in, int version) throws IOException
        {
            ByteBuffer serializedCondition = readWithVIntLength(in);
            ByteBuffer[] serializedUpdates = SerializationUtils.deserializeArray(in, version, ByteBufferUtil.vintSerializer, ByteBuffer[]::new);
            return new TxnUpdate(serializedUpdates, serializedCondition);
        }

        @Override
        public long serializedSize(TxnUpdate update, int version)
        {
            long size = 0;
            size += serializedSizeWithVIntLength(update.serializedCondition);
            size += SerializationUtils.serializedArraySize(update.serializedUpdates, version, ByteBufferUtil.vintSerializer);
            return size;
        }
    };
}

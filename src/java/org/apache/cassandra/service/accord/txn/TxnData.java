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
import java.util.HashMap;
import java.util.Map;

import accord.api.Data;
import accord.api.Result;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.service.accord.SerializationUtils.filteredPartitionSerializer;

public class TxnData implements Data, Result
{
    private final Map<String, FilteredPartition> data;

    public TxnData(Map<String, FilteredPartition> data)
    {
        this.data = data;
    }

    public TxnData()
    {
        this(new HashMap<>());
    }

    public void put(String name, FilteredPartition partition)
    {
        data.put(name, partition);
    }

    public FilteredPartition get(String name)
    {
        return data.get(name);
    }

    @Override
    public Data merge(Data data)
    {
        TxnData that = (TxnData) data;
        TxnData merged = new TxnData();
        this.data.forEach(merged::put);
        that.data.forEach(merged::put);
        return merged;
    }

    // TODO: implement this for AccordCommand
    public long estimatedSizeOnHeap()
    {
        return 0;
    }

    public static final IVersionedSerializer<TxnData> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(TxnData data, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(data.data.size());
            for (Map.Entry<String, FilteredPartition> entry : data.data.entrySet())
            {
                out.writeUTF(entry.getKey());
                filteredPartitionSerializer.serialize(entry.getValue(), out, version);
            }
        }

        @Override
        public TxnData deserialize(DataInputPlus in, int version) throws IOException
        {
            Map<String, FilteredPartition> data = new HashMap<>();
            int size = in.readInt();
            for (int i=0; i<size; i++)
            {
                String name = in.readUTF();
                FilteredPartition partition = filteredPartitionSerializer.deserialize(in, version);
                data.put(name, partition);
            }
            return new TxnData(data);
        }

        @Override
        public long serializedSize(TxnData data, int version)
        {
            long size = TypeSizes.INT_SIZE;
            for (Map.Entry<String, FilteredPartition> entry : data.data.entrySet())
            {
                size += TypeSizes.sizeof(entry.getKey());
                size += filteredPartitionSerializer.serializedSize(entry.getValue(), version);
            }
            return size;
        }
    };
}

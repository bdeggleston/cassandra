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
import java.util.function.IntFunction;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorSerializer;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

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

    public static <T> void serializeArray(T[] items, DataOutputPlus out, int version, IVersionedSerializer<T> serializer) throws IOException
    {
        out.writeInt(items.length);
        for (T item : items)
            serializer.serialize(item, out, version);
    }

    public static <T> T[] deserializeArray(DataInputPlus in, int version, IVersionedSerializer<T> serializer, IntFunction<T[]> arrayFactory) throws IOException
    {
        int size = in.readInt();
        T[] items = arrayFactory.apply(size);
        for (int i=0; i<size; i++)
            items[i] = serializer.deserialize(in, version);
        return items;
    }

    public static <T> long serializedArraySize(T[] array, int version, IVersionedSerializer<T> serializer)
    {
        long size = TypeSizes.INT_SIZE;
        for (T item : array)
            size += serializer.serializedSize(item, version);
        return size;
    }

    public static <T> void serializeList(List<T> items, DataOutputPlus out, int version, IVersionedSerializer<T> serializer) throws IOException
    {
        out.writeInt(items.size());
        for (int i=0, mi=items.size(); i<mi; i++)
            serializer.serialize(items.get(i), out, version);
    }

    public static <T> List<T> deserializeList(DataInputPlus in, int version, IVersionedSerializer<T> serializer) throws IOException
    {
        int size = in.readInt();
        List<T> items = new ArrayList<>(size);
        for (int i=0; i<size; i++)
            items.add(serializer.deserialize(in, version));
        return items;
    }

    public static <T> long serializedListSize(List<T> items, int version, IVersionedSerializer<T> serializer)
    {
        long size = TypeSizes.INT_SIZE;
        for (int i=0, mi=items.size(); i<mi; i++)
            size += serializer.serializedSize(items.get(i), version);
        return size;
    }

    public static final IVersionedSerializer<PartitionUpdate> partitionUpdateSerializer = new IVersionedSerializer<PartitionUpdate>()
    {
        @Override
        public void serialize(PartitionUpdate upd, DataOutputPlus out, int version) throws IOException
        {
            PartitionUpdate.serializer.serialize(upd, out, version);
        }

        @Override
        public PartitionUpdate deserialize(DataInputPlus in, int version) throws IOException
        {
            return PartitionUpdate.serializer.deserialize(in, version, DeserializationHelper.Flag.FROM_REMOTE);
        }

        @Override
        public long serializedSize(PartitionUpdate upd, int version)
        {
            return PartitionUpdate.serializer.serializedSize(upd, version);
        }
    };

    public static final IVersionedSerializer<SinglePartitionReadCommand> singlePartitionReadCommandSerializer = new IVersionedSerializer<>()
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

    public static final IVersionedSerializer<FilteredPartition> filteredPartitionSerializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(FilteredPartition partition, DataOutputPlus out, int version) throws IOException
        {
            partition.metadata().id.serialize(out);
            TableMetadata metadata = Schema.instance.getTableMetadata(partition.metadata().id);
            try (UnfilteredRowIterator iterator = partition.unfilteredIterator())
            {
                UnfilteredRowIteratorSerializer.serializer.serialize(iterator, ColumnFilter.all(metadata), out, version, partition.rowCount());
            }
        }

        @Override
        public FilteredPartition deserialize(DataInputPlus in, int version) throws IOException
        {
            TableMetadata metadata = Schema.instance.getTableMetadata(TableId.deserialize(in));
            Preconditions.checkState(metadata != null);
            try (UnfilteredRowIterator partition = UnfilteredRowIteratorSerializer.serializer.deserialize(in, version, metadata, ColumnFilter.all(metadata), DeserializationHelper.Flag.FROM_REMOTE))
            {
                return new FilteredPartition(UnfilteredRowIterators.filter(partition, 0));
            }
        }

        @Override
        public long serializedSize(FilteredPartition partition, int version)
        {
            long size = TableId.serializedSize();
            TableMetadata metadata = Schema.instance.getTableMetadata(partition.metadata().id);
            Preconditions.checkState(metadata != null);
            try (UnfilteredRowIterator iterator = partition.unfilteredIterator())
            {
                return size + UnfilteredRowIteratorSerializer.serializer.serializedSize(iterator, ColumnFilter.all(metadata), version, partition.rowCount());
            }
        }
    };

    public static final IVersionedSerializer<ColumnMetadata> columnMetadataSerializer = new IVersionedSerializer<ColumnMetadata>()
    {
        @Override
        public void serialize(ColumnMetadata column, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(column.ksName);
            out.writeUTF(column.cfName);
            ByteBufferUtil.writeWithShortLength(column.name.bytes, out);
        }

        @Override
        public ColumnMetadata deserialize(DataInputPlus in, int version) throws IOException
        {
            String keyspace = in.readUTF();
            String table = in.readUTF();
            ByteBuffer name = ByteBufferUtil.readWithShortLength(in);
            TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
            return metadata.getColumn(name);
        }

        @Override
        public long serializedSize(ColumnMetadata column, int version)
        {
            long size = 0;
            size += TypeSizes.sizeof(column.ksName);
            size += TypeSizes.sizeof(column.cfName);
            size += ByteBufferUtil.serializedSizeWithShortLength(column.name.bytes);
            return size;
        }
    };

    public static final IVersionedSerializer<TableMetadata> tableMetadataSerializer = new IVersionedSerializer<TableMetadata>()
    {
        @Override
        public void serialize(TableMetadata metadata, DataOutputPlus out, int version) throws IOException
        {
            metadata.id.serialize(out);
        }

        @Override
        public TableMetadata deserialize(DataInputPlus in, int version) throws IOException
        {
            return Schema.instance.getTableMetadata(TableId.deserialize(in));
        }

        @Override
        public long serializedSize(TableMetadata metadata, int version)
        {
            return TableId.serializedSize();
        }
    };
}

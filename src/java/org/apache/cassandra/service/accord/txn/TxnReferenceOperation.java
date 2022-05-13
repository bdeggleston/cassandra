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
import java.util.Objects;

import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;

import static org.apache.cassandra.service.accord.SerializationUtils.columnMetadataSerializer;

public class TxnReferenceOperation
{
    private final ColumnMetadata receiver;
    private final TxnReferenceValue value;

    public TxnReferenceOperation(ColumnMetadata receiver, TxnReferenceValue value)
    {
        this.receiver = receiver;
        this.value = value;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TxnReferenceOperation that = (TxnReferenceOperation) o;
        return receiver.equals(that.receiver) && value.equals(that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(receiver, value);
    }

    @Override
    public String toString()
    {
        return receiver + " = " + value;
    }

    public ColumnMetadata receiver()
    {
        return receiver;
    }

    public void apply(TxnData data, Row.Builder row, long timestamp, int nowInSeconds)
    {
        // FIXME: this will probably be insufficient when dealing with collection columns
        row.addCell(BufferCell.live(receiver, timestamp, value.compute(data, receiver.type)));
    }

    public static final IVersionedSerializer<TxnReferenceOperation> serializer = new IVersionedSerializer<TxnReferenceOperation>()
    {
        @Override
        public void serialize(TxnReferenceOperation operation, DataOutputPlus out, int version) throws IOException
        {
            columnMetadataSerializer.serialize(operation.receiver, out, version);
            TxnReferenceValue.serializer.serialize(operation.value, out, version);
        }

        @Override
        public TxnReferenceOperation deserialize(DataInputPlus in, int version) throws IOException
        {
            ColumnMetadata receiver = columnMetadataSerializer.deserialize(in, version);
            TxnReferenceValue value = TxnReferenceValue.serializer.deserialize(in, version);
            return new TxnReferenceOperation(receiver, value);
        }

        @Override
        public long serializedSize(TxnReferenceOperation operation, int version)
        {
            long size = 0;
            size += columnMetadataSerializer.serializedSize(operation.receiver, version);
            size += TxnReferenceValue.serializer.serializedSize(operation.value, version);
            return size;
        }
    };
}

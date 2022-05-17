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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.service.accord.SerializationUtils;

public abstract class TxnCondition
{
    public enum Kind
    {
        NONE("n/a"),
        AND("AND"),
        OR("OR"),

        EXISTS("EXISTS"),
        NOT_EXISTS("NOT EXISTS"),
        EQUAL("=="),
        NOT_EQUAL("!="),
        GREATER_THAN(">"),
        GREATER_THAN_OR_EQUAL(">="),
        LESS_THAN("<"),
        LESS_THAN_OR_EQUAL("<=");

        final String symbol;

        Kind(String symbol)
        {
            this.symbol = symbol;
        }
    }

    public final Kind kind;

    public TxnCondition(Kind kind)
    {
        this.kind = kind;
    }

    public Kind kind()
    {
        return kind;
    }

    public abstract boolean applies(TxnData data);

    public static final TxnCondition NONE = new TxnCondition(Kind.NONE)
    {
        @Override
        public boolean applies(TxnData data)
        {
            return true;
        }
    };

    public static class Exists extends TxnCondition
    {
        private static final Set<Kind> KINDS = ImmutableSet.of(Kind.EXISTS, Kind.NOT_EXISTS);

        public final ValueReference reference;
        public final boolean exists;

        public Exists(ValueReference reference, Kind kind, boolean exists)
        {
            super(kind);
            this.reference = reference;
            this.exists = exists;
            Preconditions.checkArgument(KINDS.contains(kind));
        }

        @Override
        public boolean applies(TxnData data)
        {
            FilteredPartition partition = reference.getPartition(data);
            boolean exists = partition != null && !partition.isEmpty();

            Row row = null;
            if (exists && reference.selectsRow())
            {
                row = reference.getRow(partition);
                exists = row != null && !row.isEmpty();
            }

            Cell<?> cell = null;
            if (exists && reference.selectsCell())
            {
                cell = reference.getCell(row);
                exists = cell != null && !cell.isTombstone();
            }

            switch (kind())
            {
                case EXISTS:
                    return exists;
                case NOT_EXISTS:
                    return !exists;
                default:
                    throw new IllegalStateException();
            }
        }
    }

    public static class Value extends TxnCondition
    {
        private static final Set<Kind> KINDS = ImmutableSet.of(Kind.EQUAL, Kind.NOT_EQUAL,
                                                               Kind.GREATER_THAN, Kind.GREATER_THAN_OR_EQUAL,
                                                               Kind.LESS_THAN, Kind.LESS_THAN_OR_EQUAL);

        public final ValueReference reference;
        public final ByteBuffer value;

        public Value(ValueReference reference, Kind kind, ByteBuffer value)
        {
            super(kind);
            this.reference = reference;
            this.value = value;
            Preconditions.checkArgument(KINDS.contains(kind));
            Preconditions.checkArgument(reference.selectsCell());
        }

        private <T> int compare(Cell<T> cell)
        {
            return reference.column().type.compare(cell.value(), cell.accessor(), value, ByteBufferAccessor.instance);
        }

        @Override
        public boolean applies(TxnData data)
        {
            Cell<?> cell = reference.getCell(data);
            if (cell == null)
                return false;

            int cmp = compare(cell);

            switch (kind())
            {
                case EQUAL:
                    return cmp == 0;
                case NOT_EQUAL:
                    return cmp != 0;
                case GREATER_THAN:
                    return cmp > 0;
                case GREATER_THAN_OR_EQUAL:
                    return cmp >= 0;
                case LESS_THAN:
                    return cmp < 0;
                case LESS_THAN_OR_EQUAL:
                    return cmp <= 0;
                default:
                    throw new IllegalStateException();
            }
        }
    }

    public static class BooleanGroup extends TxnCondition
    {
        private static final Set<Kind> KINDS = ImmutableSet.of(Kind.AND, Kind.OR);

        public final List<TxnCondition> conditions;

        public BooleanGroup(Kind kind, List<TxnCondition> conditions)
        {
            super(kind);
            this.conditions = conditions;
        }

        @Override
        public boolean applies(TxnData data)
        {
            switch (kind())
            {
                case AND:
                    return Iterables.all(conditions, c -> c.applies(data));
                case OR:
                    return Iterables.any(conditions, c -> c.applies(data));
                default:
                    throw new IllegalStateException();
            }
        }
    }

    public static final IVersionedSerializer<TxnCondition> serializer = SerializationUtils.todoSerializer();
}

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
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.service.accord.SerializationUtils.deserializeList;
import static org.apache.cassandra.service.accord.SerializationUtils.serializeList;
import static org.apache.cassandra.service.accord.SerializationUtils.serializedListSize;

public abstract class TxnCondition
{
    private interface ConditionSerializer<T extends TxnCondition>
    {
        void serialize(T condition, DataOutputPlus out, int version) throws IOException;
        T deserialize(DataInputPlus in, int version, Kind kind) throws IOException;
        long serializedSize(T condition, int version);
    }

    public enum Kind
    {
        NONE("n/a"),
        AND("AND"),
        OR("OR"),
        IS_NOT_NULL("IS NOT NULL"),
        IS_NULL("IS NULL"),
        EQUAL("=="),
        NOT_EQUAL("!="),
        GREATER_THAN(">"),
        GREATER_THAN_OR_EQUAL(">="),
        LESS_THAN("<"),
        LESS_THAN_OR_EQUAL("<=");

        private final String symbol;

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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TxnCondition condition = (TxnCondition) o;
        return kind == condition.kind;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(kind);
    }

    public Kind kind()
    {
        return kind;
    }

    public abstract boolean applies(TxnData data);

    private static class None extends TxnCondition
    {
        private static final None instance = new None();

        private None()
        {
            super(Kind.NONE);
        }

        @Override
        public String toString()
        {
            return kind.toString();
        }

        @Override
        public boolean applies(TxnData data)
        {
            return true;
        }

        private static final ConditionSerializer<None> serializer = new ConditionSerializer<>()
        {
            @Override
            public void serialize(None condition, DataOutputPlus out, int version) throws IOException {}
            @Override
            public None deserialize(DataInputPlus in, int version, Kind kind) throws IOException { return instance; }
            @Override
            public long serializedSize(None condition, int version) { return 0; }
        };
    }

    public static TxnCondition none()
    {
        return None.instance;
    }

    public static class Exists extends TxnCondition
    {
        private static final Set<Kind> KINDS = ImmutableSet.of(Kind.IS_NOT_NULL, Kind.IS_NULL);

        public final ValueReference reference;

        public Exists(ValueReference reference, Kind kind)
        {
            super(kind);
            this.reference = reference;
            Preconditions.checkArgument(KINDS.contains(kind));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Exists exists = (Exists) o;
            return reference.equals(exists.reference);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(super.hashCode(), reference);
        }

        @Override
        public String toString()
        {
            return reference.toString() + ' ' + kind.toString();
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

            if (exists && reference.selectsCell())
            {
                Cell<?> cell = reference.getCell(row);
                exists = cell != null && !cell.isTombstone();
            }

            switch (kind())
            {
                case IS_NOT_NULL:
                    return exists;
                case IS_NULL:
                    return !exists;
                default:
                    throw new IllegalStateException();
            }
        }

        private static final ConditionSerializer<Exists> serializer = new ConditionSerializer<>()
        {
            @Override
            public void serialize(Exists condition, DataOutputPlus out, int version) throws IOException
            {
                ValueReference.serializer.serialize(condition.reference, out, version);
            }

            @Override
            public Exists deserialize(DataInputPlus in, int version, Kind kind) throws IOException
            {
                return new Exists(ValueReference.serializer.deserialize(in, version), kind);
            }

            @Override
            public long serializedSize(Exists condition, int version)
            {
                return ValueReference.serializer.serializedSize(condition.reference, version);
            }
        };
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

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Value value1 = (Value) o;
            return reference.equals(value1.reference) && value.equals(value1.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(super.hashCode(), reference, value);
        }

        @Override
        public String toString()
        {
            return reference.toString() + ' ' + kind.symbol + " 0x" + ByteBufferUtil.bytesToHex(value);
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

        private static final ConditionSerializer<Value> serializer = new ConditionSerializer<>()
        {
            @Override
            public void serialize(Value condition, DataOutputPlus out, int version) throws IOException
            {
                ValueReference.serializer.serialize(condition.reference, out, version);
                ByteBufferUtil.writeWithVIntLength(condition.value, out);
            }

            @Override
            public Value deserialize(DataInputPlus in, int version, Kind kind) throws IOException
            {
                ValueReference reference = ValueReference.serializer.deserialize(in, version);
                ByteBuffer value = ByteBufferUtil.readWithVIntLength(in);
                return new Value(reference, kind, value);
            }

            @Override
            public long serializedSize(Value condition, int version)
            {
                long size = 0;
                size += ValueReference.serializer.serializedSize(condition.reference, version);
                size += ByteBufferUtil.serializedSizeWithVIntLength(condition.value);
                return size;
            }
        };
    }

    public static class BooleanGroup extends TxnCondition
    {
        private static final Set<Kind> KINDS = ImmutableSet.of(Kind.AND, Kind.OR);

        public final List<TxnCondition> conditions;

        public BooleanGroup(Kind kind, List<TxnCondition> conditions)
        {
            super(kind);
            Preconditions.checkArgument(KINDS.contains(kind));
            this.conditions = conditions;
        }

        @Override
        public String toString()
        {
            return '(' + conditions.stream().map(Objects::toString).reduce((a, b) -> a + ' ' + kind.symbol  + ' ' + b).orElse("") + ')';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            BooleanGroup that = (BooleanGroup) o;
            return Objects.equals(conditions, that.conditions);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(super.hashCode(), conditions);
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

        private static final ConditionSerializer<BooleanGroup> serializer = new ConditionSerializer<>()
        {
            @Override
            public void serialize(BooleanGroup condition, DataOutputPlus out, int version) throws IOException
            {
                serializeList(condition.conditions, out, version, TxnCondition.serializer);
            }

            @Override
            public BooleanGroup deserialize(DataInputPlus in, int version, Kind kind) throws IOException
            {
                return new BooleanGroup(kind, deserializeList(in, version, TxnCondition.serializer));
            }

            @Override
            public long serializedSize(BooleanGroup condition, int version)
            {
                return serializedListSize(condition.conditions, version, TxnCondition.serializer);
            }
        };
    }

    public static final IVersionedSerializer<TxnCondition> serializer = new IVersionedSerializer<>()
    {
        private ConditionSerializer serializerFor(Kind kind)
        {
            switch (kind)
            {
                case IS_NOT_NULL:
                case IS_NULL:
                    return Exists.serializer;
                case EQUAL:
                case NOT_EQUAL:
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                    return Value.serializer;
                case AND:
                case OR:
                    return BooleanGroup.serializer;
                case NONE:
                    return None.serializer;
                default:
                    throw new IllegalArgumentException();
            }
        }

        @Override
        public void serialize(TxnCondition condition, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(condition.kind.ordinal());
            serializerFor(condition.kind).serialize(condition, out, version);
        }

        @Override
        public TxnCondition deserialize(DataInputPlus in, int version) throws IOException
        {
            Kind kind = Kind.values()[in.readInt()];
            return serializerFor(kind).deserialize(in, version, kind);
        }

        @Override
        public long serializedSize(TxnCondition condition, int version)
        {
            return TypeSizes.INT_SIZE + serializerFor(condition.kind).serializedSize(condition, version);
        }
    };
}

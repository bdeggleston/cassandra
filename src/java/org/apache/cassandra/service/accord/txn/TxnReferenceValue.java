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
import java.util.function.BiFunction;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.NumberType;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class TxnReferenceValue
{
    public enum Kind
    {
        CONSTANT(Constant.serializer),
        SUBSTITUTION(Substitution.serializer),
        ADDITION(Addition.serializer),
        SUBTRACTION(Subtraction.serializer);

        final Serializer serializer;

        Kind(Serializer serializer)
        {
            this.serializer = serializer;
        }
    }

    public abstract Kind kind();
    public abstract ByteBuffer compute(TxnData data, AbstractType<?> receiver);

    /**
     * Serializer for everything except Kind
     * @param <T>
     */
    private interface Serializer<T extends TxnReferenceValue>
    {
        void serialize(T t, DataOutputPlus out, int version) throws IOException;
        T deserialize(DataInputPlus in, int version, Kind kind) throws IOException;
        long serializedSize(T t, int version);
    }

    public static class Constant extends TxnReferenceValue
    {
        private final ByteBuffer value;

        public Constant(ByteBuffer value)
        {
            this.value = value;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Constant constant = (Constant) o;
            return value.equals(constant.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(value);
        }

        @Override
        public String toString()
        {
            return ByteBufferUtil.bytesToHex(value);
        }

        @Override
        public Kind kind()
        {
            return Kind.CONSTANT;
        }

        @Override
        public ByteBuffer compute(TxnData data, AbstractType<?> receiver)
        {
            return value;
        }

        public static final Serializer<Constant> serializer = new Serializer<Constant>()
        {
            @Override
            public void serialize(Constant constant, DataOutputPlus out, int version) throws IOException
            {
                ByteBufferUtil.writeWithShortLength(constant.value, out);
            }

            @Override
            public Constant deserialize(DataInputPlus in, int version, Kind kind) throws IOException
            {
                return new Constant(ByteBufferUtil.readWithShortLength(in));
            }

            @Override
            public long serializedSize(Constant constant, int version)
            {
                return ByteBufferUtil.serializedSizeWithShortLength(constant.value);
            }
        };
    }

    public static class Substitution extends TxnReferenceValue
    {
        private final ValueReference reference;

        public Substitution(ValueReference reference)
        {
            this.reference = reference;
        }

        @Override
        public String toString()
        {
            return reference.toString();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Substitution that = (Substitution) o;
            return reference.equals(that.reference);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(reference);
        }

        @Override
        public Kind kind()
        {
            return Kind.SUBSTITUTION;
        }

        @Override
        public ByteBuffer compute(TxnData data, AbstractType<?> receiver)
        {
            // TODO: confirm all references can be satisfied as part of the txn condition
            // TODO: if the receiver type is not the same as the column type here, we need to do the neccesary conversion
            Preconditions.checkArgument(receiver == reference.column().type);
            return reference.getCell(data).buffer();
        }

        static final Serializer<Substitution> serializer = new Serializer<>()
        {
            @Override
            public void serialize(Substitution substitution, DataOutputPlus out, int version) throws IOException
            {
                ValueReference.serializer.serialize(substitution.reference, out, version);
            }

            @Override
            public Substitution deserialize(DataInputPlus in, int version, Kind kind) throws IOException
            {
                return new Substitution(ValueReference.serializer.deserialize(in, version));
            }

            @Override
            public long serializedSize(Substitution substitution, int version)
            {
                return ValueReference.serializer.serializedSize(substitution.reference, version);
            }
        };
    }

    private abstract static class BiValue extends TxnReferenceValue
    {
        final TxnReferenceValue left;
        final TxnReferenceValue right;

        public BiValue(TxnReferenceValue left, TxnReferenceValue right)
        {
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BiValue biValue = (BiValue) o;
            return left.equals(biValue.left) && right.equals(biValue.right);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(left, right);
        }

        static class BiValueSerializer<T extends BiValue> implements TxnReferenceValue.Serializer<T>
        {
            private final BiFunction<TxnReferenceValue, TxnReferenceValue, T> factory;

            public BiValueSerializer(BiFunction<TxnReferenceValue, TxnReferenceValue, T> factory)
            {
                this.factory = factory;
            }

            @Override
            public void serialize(T value, DataOutputPlus out, int version) throws IOException
            {
                TxnReferenceValue.serializer.serialize(value.left, out, version);
                TxnReferenceValue.serializer.serialize(value.right, out, version);
            }

            @Override
            public T deserialize(DataInputPlus in, int version, Kind kind) throws IOException
            {
                return factory.apply(TxnReferenceValue.serializer.deserialize(in, version),
                                     TxnReferenceValue.serializer.deserialize(in, version));
            }

            @Override
            public long serializedSize(T value, int version)
            {
                return TxnReferenceValue.serializer.serializedSize(value.left, version)
                       + TxnReferenceValue.serializer.serializedSize(value.left, version);
            }
        }
    }

    public static class Addition extends BiValue
    {
        public Addition(TxnReferenceValue left, TxnReferenceValue right)
        {
            super(left, right);
        }

        @Override
        public String toString()
        {
            return left + " + " + right;
        }

        @Override
        public Kind kind()
        {
            return Kind.ADDITION;
        }

        @Override
        public ByteBuffer compute(TxnData data, AbstractType<?> receiver)
        {
            if (receiver instanceof NumberType<?>)
            {
                NumberType<?> type = (NumberType<?>) receiver;
                return type.add(type, left.compute(data, receiver), type, right.compute(data, receiver));
            }
            else
            {
                throw new IllegalArgumentException("Unhandled type for addition: " + receiver);
            }
        }

        public static final Serializer<Addition> serializer = new BiValueSerializer<>(Addition::new);
    }

    public static class Subtraction extends BiValue
    {
        public Subtraction(TxnReferenceValue left, TxnReferenceValue right)
        {
            super(left, right);
        }

        @Override
        public String toString()
        {
            return left + " - " + right;
        }

        @Override
        public Kind kind()
        {
            return Kind.SUBTRACTION;
        }

        @Override
        public ByteBuffer compute(TxnData data, AbstractType<?> receiver)
        {
            if (receiver instanceof NumberType<?>)
            {
                NumberType<?> type = (NumberType<?>) receiver;
                return type.substract(type, left.compute(data, receiver), type, right.compute(data, receiver));
            }
            else
            {
                throw new IllegalArgumentException("Unhandled type for addition: " + receiver);
            }
        }

        public static final Serializer<Subtraction> serializer = new BiValueSerializer<>(Subtraction::new);
    }

    public static final IVersionedSerializer<TxnReferenceValue> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(TxnReferenceValue value, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(value.kind().ordinal());
            value.kind().serializer.serialize(value, out, version);
        }

        @Override
        public TxnReferenceValue deserialize(DataInputPlus in, int version) throws IOException
        {
            Kind kind = Kind.values()[in.readInt()];
            return kind.serializer.deserialize(in, version, kind);
        }

        @Override
        public long serializedSize(TxnReferenceValue value, int version)
        {
            return TypeSizes.INT_SIZE + value.kind().serializer.serializedSize(value, version);
        }
    };
}

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

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public abstract class TxnReferenceValue
{
    public enum Kind
    {
        SUBSTITUTION(Substitution.serializer);

        final Serializer serializer;

        Kind(Serializer serializer)
        {
            this.serializer = serializer;
        }
    }

    public abstract Kind kind();
    public abstract ByteBuffer dereference(TxnData data);

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
        public ByteBuffer dereference(TxnData data)
        {
            // TODO: confirm all references can be satisfied as part of the txn condition
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

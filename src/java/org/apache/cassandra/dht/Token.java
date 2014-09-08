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
package org.apache.cassandra.dht;

import java.io.DataInput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class Token<T> implements RingPosition<Token<T>>, Serializable
{
    private static final long serialVersionUID = 1L;

    public final T token;

    public final IPartitioner partitioner;

    protected Token(T token, IPartitioner partitioner)
    {
        this.token = token;
        this.partitioner = partitioner;
    }

    /**
     * This determines the comparison for node destination purposes.
     */
    abstract public int compareTo(Token<T> o);

    @Override
    public String toString()
    {
        return token.toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null || this.getClass() != obj.getClass())
            return false;

        return token.equals(((Token<T>)obj).token);
    }

    @Override
    public int hashCode()
    {
        return token.hashCode();
    }

    public static abstract class TokenFactory<T>
    {
        public abstract ByteBuffer toByteArray(Token<T> token);
        public abstract Token<T> fromByteArray(ByteBuffer bytes);
        public abstract String toString(Token<T> token); // serialize as string, not necessarily human-readable
        public abstract Token<T> fromString(String string); // deserialize

        public abstract void validate(String token) throws ConfigurationException;
    }

    public static class Serializer implements ISerializer<Token>
    {
        private final IPartitioner partitioner;

        public Serializer(IPartitioner partitioner)
        {
            this.partitioner = partitioner;
        }

        public void serialize(Token token, DataOutputPlus out) throws IOException
        {
            ByteBuffer b = partitioner.getTokenFactory().toByteArray(token);
            ByteBufferUtil.writeWithLength(b, out);
        }

        public Token deserialize(DataInput in) throws IOException
        {
            int size = in.readInt();
            byte[] bytes = new byte[size];
            in.readFully(bytes);
            return partitioner.getTokenFactory().fromByteArray(ByteBuffer.wrap(bytes));
        }

        public long serializedSize(Token object, TypeSizes typeSizes)
        {
            ByteBuffer b = partitioner.getTokenFactory().toByteArray(object);
            return TypeSizes.NATIVE.sizeof(b.remaining()) + b.remaining();
        }
    }

    public Token<T> getToken()
    {
        return this;
    }

    public boolean isMinimum(IPartitioner partitioner)
    {
        return this.equals(partitioner.getMinimumToken());
    }

    public boolean isMinimum()
    {
        return isMinimum(partitioner);
    }

    /*
     * A token corresponds to the range of all the keys having this token.
     * A token is thus no comparable directly to a key. But to be able to select
     * keys given tokens, we introduce two "fake" keys for each token T:
     *   - lowerBoundKey: a "fake" key representing the lower bound T represents.
     *                    In other words, lowerBoundKey is the smallest key that
     *                    have token T.
     *   - upperBoundKey: a "fake" key representing the upper bound T represents.
     *                    In other words, upperBoundKey is the largest key that
     *                    have token T.
     *
     * Note that those are "fake" keys and should only be used for comparison
     * of other keys, for selection of keys when only a token is known.
     */
    public KeyBound minKeyBound(IPartitioner partitioner)
    {
        return new KeyBound(this, true, partitioner);
    }

    public KeyBound minKeyBound()
    {
        return minKeyBound(partitioner);
    }

    public KeyBound maxKeyBound(IPartitioner partitioner)
    {
        /*
         * For each token, we needs both minKeyBound and maxKeyBound
         * because a token corresponds to a range of keys. But the minimun
         * token corresponds to no key, so it is valid and actually much
         * simpler to associate the same value for minKeyBound and
         * maxKeyBound for the minimun token.
         */
        if (isMinimum(partitioner))
            return minKeyBound();
        return new KeyBound(this, false, partitioner);
    }

    public KeyBound maxKeyBound()
    {
        return maxKeyBound(partitioner);
    }

    public <R extends RingPosition> R upperBound(Class<R> klass)
    {
        if (klass.equals(getClass()))
            return (R)this;
        else
            return (R)maxKeyBound();
    }

    public static class KeyBound implements RowPosition
    {
        private final Token token;
        public final boolean isMinimumBound;
        private final IPartitioner partitioner;

        private KeyBound(Token t, boolean isMinimumBound, IPartitioner partitioner)
        {
            this.token = t;
            this.isMinimumBound = isMinimumBound;
            this.partitioner = partitioner;
        }

        public Token getToken()
        {
            return token;
        }

        public int compareTo(RowPosition pos)
        {
            if (this == pos)
                return 0;

            int cmp = getToken().compareTo(pos.getToken());
            if (cmp != 0)
                return cmp;

            if (isMinimumBound)
                return ((pos instanceof KeyBound) && ((KeyBound)pos).isMinimumBound) ? 0 : -1;
            else
                return ((pos instanceof KeyBound) && !((KeyBound)pos).isMinimumBound) ? 0 : 1;
        }

        public boolean isMinimum(IPartitioner partitioner)
        {
            return getToken().isMinimum(partitioner);
        }

        public boolean isMinimum()
        {
            return isMinimum(partitioner);
        }

        public RowPosition.Kind kind()
        {
            return isMinimumBound ? RowPosition.Kind.MIN_BOUND : RowPosition.Kind.MAX_BOUND;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null || this.getClass() != obj.getClass())
                return false;

            KeyBound other = (KeyBound)obj;
            return token.equals(other.token) && isMinimumBound == other.isMinimumBound;
        }

        @Override
        public int hashCode()
        {
            return getToken().hashCode() + (isMinimumBound ? 0 : 1);
        }

        @Override
        public String toString()
        {
            return String.format("%s(%s)", isMinimumBound ? "min" : "max", getToken().toString());
        }
    }
}

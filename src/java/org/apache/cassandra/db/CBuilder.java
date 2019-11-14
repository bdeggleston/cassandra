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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.values.Value;

/**
 * Allows to build ClusteringPrefixes, either Clustering or ClusteringBound.
 */
public abstract class CBuilder
{
    public static CBuilder STATIC_BUILDER = new CBuilder()
    {
        public int count()
        {
            return 0;
        }

        public int remainingCount()
        {
            return 0;
        }

        public ClusteringComparator comparator()
        {
            throw new UnsupportedOperationException();
        }

        public CBuilder add(Value value)
        {
            throw new UnsupportedOperationException();
        }

        public CBuilder add(Object value)
        {
            throw new UnsupportedOperationException();
        }

        public Clustering build()
        {
            return Clustering.STATIC_CLUSTERING;
        }

        public ClusteringBound buildBound(boolean isStart, boolean isInclusive)
        {
            throw new UnsupportedOperationException();
        }

        public Slice buildSlice()
        {
            throw new UnsupportedOperationException();
        }

        public Clustering buildWith(Value value)
        {
            throw new UnsupportedOperationException();
        }

        public Clustering buildWith(List<Value> newValues)
        {
            throw new UnsupportedOperationException();
        }

        public ClusteringBound buildBoundWith(Value value, boolean isStart, boolean isInclusive)
        {
            throw new UnsupportedOperationException();
        }

        public ClusteringBound buildBoundWith(List<Value> newValues, boolean isStart, boolean isInclusive)
        {
            throw new UnsupportedOperationException();
        }
    };

    public static CBuilder create(ClusteringComparator comparator)
    {
        return new ArrayBackedBuilder(comparator);
    }

    public abstract int count();
    public abstract int remainingCount();
    public abstract ClusteringComparator comparator();
    public abstract CBuilder add(Value value);
    public abstract CBuilder add(Object value);
    public abstract Clustering build();
    public abstract ClusteringBound buildBound(boolean isStart, boolean isInclusive);
    public abstract Slice buildSlice();
    public abstract Clustering buildWith(Value value);
    public abstract Clustering buildWith(List<Value> newValues);
    public abstract ClusteringBound buildBoundWith(Value value, boolean isStart, boolean isInclusive);
    public abstract ClusteringBound buildBoundWith(List<Value> newValues, boolean isStart, boolean isInclusive);

    private static class ArrayBackedBuilder extends CBuilder
    {
        private final ClusteringComparator type;
        private final Value[] values;
        private int size;
        private boolean built;

        public ArrayBackedBuilder(ClusteringComparator type)
        {
            this.type = type;
            this.values = new Value[type.size()];
        }

        public int count()
        {
            return size;
        }

        public int remainingCount()
        {
            return values.length - size;
        }

        public ClusteringComparator comparator()
        {
            return type;
        }

        public CBuilder add(Value value)
        {
            if (isDone())
                throw new IllegalStateException();
            values[size++] = value;
            return this;
        }

        public CBuilder add(Object value)
        {
            return add(((AbstractType)type.subtype(size)).decomposeBuffer(value));
        }

        private boolean isDone()
        {
            return remainingCount() == 0 || built;
        }

        public Clustering build()
        {
            // We don't allow to add more element to a builder that has been built so
            // that we don't have to copy values.
            built = true;

            // Currently, only dense table can leave some clustering column out (see #7990)
            return size == 0 ? Clustering.EMPTY : Clustering.make(values);
        }

        public ClusteringBound buildBound(boolean isStart, boolean isInclusive)
        {
            // We don't allow to add more element to a builder that has been built so
            // that we don't have to copy values (even though we have to do it in most cases).
            built = true;

            if (size == 0)
                return isStart ? ClusteringBound.BOTTOM : ClusteringBound.TOP;

            return ClusteringBound.create(ClusteringBound.boundKind(isStart, isInclusive),
                                size == values.length ? values : Arrays.copyOfRange(values, 0, size));
        }

        public Slice buildSlice()
        {
            // We don't allow to add more element to a builder that has been built so
            // that we don't have to copy values.
            built = true;

            if (size == 0)
                return Slice.ALL;

            return Slice.make(buildBound(true, true), buildBound(false, true));
        }

        public Clustering buildWith(Value value)
        {
            assert size+1 <= type.size();

            Value[] newValues = Arrays.copyOf(values, type.size());
            newValues[size] = value;
            return Clustering.make(newValues);
        }

        public Clustering buildWith(List<Value> newValues)
        {
            assert size + newValues.size() <= type.size();
            Value[] buffers = Arrays.copyOf(values, type.size());
            int newSize = size;
            for (Value value : newValues)
                buffers[newSize++] = value;

            return Clustering.make(buffers);
        }

        public ClusteringBound buildBoundWith(Value value, boolean isStart, boolean isInclusive)
        {
            Value[] newValues = Arrays.copyOf(values, size+1);
            newValues[size] = value;
            return ClusteringBound.create(ClusteringBound.boundKind(isStart, isInclusive), newValues);
        }

        public ClusteringBound buildBoundWith(List<Value> newValues, boolean isStart, boolean isInclusive)
        {
            Value[] buffers = Arrays.copyOf(values, size + newValues.size());
            int newSize = size;
            for (Value value : newValues)
                buffers[newSize++] = value;

            return ClusteringBound.create(ClusteringBound.boundKind(isStart, isInclusive), buffers);
        }
    }
}

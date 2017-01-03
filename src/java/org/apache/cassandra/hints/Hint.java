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
package org.apache.cassandra.hints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.primitives.Ints;

import org.apache.cassandra.db.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt;

/**
 * Encapsulates the hinted mutation, its creation time, and the gc grace seconds param for each table involved.
 *
 * - Why do we need to track hint creation time?
 * - We must exclude updates for tables that have been truncated after hint's creation, otherwise the result is data corruption.
 *
 * - Why do we need to track gc grace seconds?
 * - Hints can stay in storage for a while before being applied, and without recording gc grace seconds (+ creation time),
 *   if we apply the mutation blindly, we risk resurrecting a deleted value, a tombstone for which had been already
 *   compacted away while the hint was in storage.
 *
 *   We also look at the smallest current value of the gcgs param for each affected table when applying the hint, and use
 *   creation time + min(recorded gc gs, current gcgs + current gc grace) as the overall hint expiration time.
 *   This allows now to safely reduce gc gs on tables without worrying that an applied old hint might resurrect any data.
 */
public final class Hint
{
    public static final Serializer serializer = new Serializer();
    static final int maxHintTTL = Integer.getInteger("cassandra.maxHintTTL", Integer.MAX_VALUE);

    final Mutation mutation;
    final long creationTime;  // time of hint creation (in milliseconds)
    final int gcgs; // the smallest gc gs of all involved tables

    private Hint(Mutation mutation, long creationTime, int gcgs)
    {
        this.mutation = mutation;
        this.creationTime = creationTime;
        this.gcgs = gcgs;
    }

    /**
     * @param mutation the hinted mutation
     * @param creationTime time of this hint's creation (in milliseconds since epoch)
     */
    public static Hint create(Mutation mutation, long creationTime)
    {
        return new Hint(mutation, creationTime, mutation.smallestGCGS());
    }

    /**
     * @param mutation the hinted mutation
     * @param creationTime time of this hint's creation (in milliseconds since epoch)
     * @param gcgs the smallest gcgs of all tables involved at the time of hint creation (in seconds)
     */
    public static Hint create(Mutation mutation, long creationTime, int gcgs)
    {
        return new Hint(mutation, creationTime, gcgs);
    }

    /**
     * Applies the contained mutation unless it's expired, filtering out any updates for truncated tables
     */
    void apply()
    {
        if (!isLive())
            return;

        // filter out partition update for table that have been truncated since hint's creation
        Mutation filtered = mutation;
        for (UUID id : mutation.getColumnFamilyIds())
            if (creationTime <= SystemKeyspace.getTruncatedAt(id))
                filtered = filtered.without(id);

        if (!filtered.isEmpty())
            filtered.apply();
    }

    /**
     * @return calculates whether or not it is safe to apply the hint without risking to resurrect any deleted data
     */
    boolean isLive()
    {
        int smallestGCGS = Math.min(gcgs, mutation.smallestGCGS());
        return isLive(System.currentTimeMillis(), creationTime, maxHintTTL, smallestGCGS);
    }

    static boolean isLive(long now, long creationTime, int maxTTL, int gcgs)
    {
        long expirationTime = creationTime + TimeUnit.SECONDS.toMillis(Math.min(gcgs, maxTTL));
        return expirationTime > now;
    }

    static final class Serializer implements IVersionedSerializer<Hint>
    {
        public long serializedSize(Hint hint, int version)
        {
            long size = sizeof(hint.creationTime);
            size += sizeofUnsignedVInt(hint.gcgs);
            size += Mutation.serializer.serializedSize(hint.mutation, version);
            return size;
        }

        public void serialize(Hint hint, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(hint.creationTime);
            out.writeUnsignedVInt(hint.gcgs);
            Mutation.serializer.serialize(hint.mutation, out, version);
        }

        public Hint deserialize(DataInputPlus in, int version) throws IOException
        {
            long creationTime = in.readLong();
            int gcgs = (int) in.readUnsignedVInt();
            return new Hint(Mutation.serializer.deserialize(in, version), creationTime, gcgs);
        }

        /**
         * Will short-circuit Mutation deserialization if the hint is definitely dead. If a Hint instance is
         * returned, there is a chance it's alive, if gcgs on one of the table involved got reduced between
         * hint creation and deserialization, but this does not impact correctness.
         *
         * @return null if the hint is definitely dead, a Hint instance if it's likely alive
         */
        @Nullable
        Hint deserializeIfLive(DataInputPlus in, int version, long timestamp, long size) throws IOException
        {
            long creationTime = in.readLong();
            int gcgs = (int) in.readUnsignedVInt();
            int bytesRead = sizeof(creationTime) + sizeofUnsignedVInt(gcgs);
            if (isLive(timestamp, creationTime, maxHintTTL, gcgs))
            {
                return new Hint(Mutation.serializer.deserialize(in, version), creationTime, gcgs);
            }
            else
            {
                in.skipBytesFully(Ints.checkedCast(size) - bytesRead);
                return null;
            }
        }

        /**
         * Will short-circuit ByteBuffer allocation if the hint is definitely dead. If a ByteBuffer instance is
         * returned, there is a chance it's alive, if gcgs on one of the table involved got reduced between
         * hint creation and deserialization, but this does not impact correctness.
         *
         * @return null if the hint is definitely dead, a ByteBuffer instance if it's likely alive
         */
        @Nullable
        ByteBuffer readBufferIfLive(DataInputPlus in, int version, long timestamp, int size) throws IOException
        {
            int maxHeaderSize = Math.min(sizeof(Long.MAX_VALUE) + VIntCoding.MAX_SIZE, size);
            byte[] header = new byte[maxHeaderSize];
            in.readFully(header);
            int remaining = size - header.length;

            try(DataInputBuffer input = new DataInputBuffer(ByteBuffer.wrap(header), false))
            {
                long creationTime = input.readLong();
                int gcgs = (int) input.readUnsignedVInt();
                if (!isLive(timestamp, creationTime, maxHintTTL, gcgs))
                {
                    if (remaining > 0)
                    {
                        in.skipBytesFully(size - maxHeaderSize);
                    }
                    return null;
                }
            }

            byte[] bytes = new byte[size];
            System.arraycopy(header, 0, bytes, 0, header.length);
            if (remaining > 0)
            {
                in.readFully(bytes, header.length, size - header.length);
            }

            return ByteBuffer.wrap(bytes);
        }
    }
}

package org.apache.cassandra.service.paxos;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDSerializer;

public class PrepareResponse
{
    public final boolean promised;

    /*
     * To maintain backward compatibility (see #6023), the meaning of inProgressCommit is a bit tricky.
     * If promised is true, then that's the last accepted commit. If promise is false, that's just
     * the previously promised ballot that made us refuse this one.
     */
    public final Commit inProgressCommit;
    public final Commit mostRecentCommit;

    public PrepareResponse(boolean promised, Commit inProgressCommit, Commit mostRecentCommit)
    {
        assert inProgressCommit.key == mostRecentCommit.key;
        assert inProgressCommit.update.metadata() == mostRecentCommit.update.metadata();

        this.promised = promised;
        this.mostRecentCommit = mostRecentCommit;
        this.inProgressCommit = inProgressCommit;
    }

    @Override
    public String toString()
    {
        return String.format("PrepareResponse(%s, %s, %s)", promised, mostRecentCommit, inProgressCommit);
    }

    public static class Serializer implements IVersionedSerializer<PrepareResponse>
    {

        private final ColumnFamilySerializer columnFamilySerializer;
        private final MutationFactory mutationFactory;

        public Serializer(ColumnFamilySerializer columnFamilySerializer, MutationFactory mutationFactory)
        {
            this.columnFamilySerializer = columnFamilySerializer;
            this.mutationFactory = mutationFactory;
        }

        public void serialize(PrepareResponse response, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(response.promised);
            ByteBufferUtil.writeWithShortLength(response.inProgressCommit.key, out);
            UUIDSerializer.serializer.serialize(response.inProgressCommit.ballot, out, version);
            columnFamilySerializer.serialize(response.inProgressCommit.update, out, version);
            UUIDSerializer.serializer.serialize(response.mostRecentCommit.ballot, out, version);
            columnFamilySerializer.serialize(response.mostRecentCommit.update, out, version);
        }

        public PrepareResponse deserialize(DataInput in, int version) throws IOException
        {
            boolean success = in.readBoolean();
            ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
            return new PrepareResponse(success,
                                       new Commit(key,
                                                  UUIDSerializer.serializer.deserialize(in, version),
                                                  columnFamilySerializer.deserialize(in,
                                                                                      ArrayBackedSortedColumns.factory,
                                                                                      ColumnSerializer.Flag.LOCAL, version),
                                                  mutationFactory),
                                       new Commit(key,
                                                  UUIDSerializer.serializer.deserialize(in, version),
                                                  columnFamilySerializer.deserialize(in,
                                                                                      ArrayBackedSortedColumns.factory,
                                                                                      ColumnSerializer.Flag.LOCAL, version),
                                                  mutationFactory));
        }

        public long serializedSize(PrepareResponse response, int version)
        {
            return 1
                   + 2 + response.inProgressCommit.key.remaining()
                   + UUIDSerializer.serializer.serializedSize(response.inProgressCommit.ballot, version)
                   + columnFamilySerializer.serializedSize(response.inProgressCommit.update, version)
                   + UUIDSerializer.serializer.serializedSize(response.mostRecentCommit.ballot, version)
                   + columnFamilySerializer.serializedSize(response.mostRecentCommit.update, version);
        }
    }
}

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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class RangeSliceReply
{
    public final List<Row> rows;
    private final Serializer serializer;

    public RangeSliceReply(List<Row> rows, Serializer serializer)
    {
        this.rows = rows;
        this.serializer = serializer;
    }

    public MessageOut<RangeSliceReply> createMessage()
    {
        return new MessageOut<RangeSliceReply>(MessagingService.Verb.REQUEST_RESPONSE, this, serializer);
    }

    @Override
    public String toString()
    {
        return "RangeSliceReply{" +
               "rows=" + StringUtils.join(rows, ",") +
               '}';
    }

    public static RangeSliceReply read(byte[] body, int version, Serializer serializer) throws IOException
    {
        return serializer.deserialize(new DataInputStream(new FastByteArrayInputStream(body)), version);
    }

    public static class Serializer implements IVersionedSerializer<RangeSliceReply>
    {
        private final Row.RowSerializer rowSerializer;

        public Serializer(Row.RowSerializer rowSerializer)
        {
            this.rowSerializer = rowSerializer;
        }

        public void serialize(RangeSliceReply rsr, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(rsr.rows.size());
            for (Row row : rsr.rows)
                rowSerializer.serialize(row, out, version);
        }

        public RangeSliceReply deserialize(DataInput in, int version) throws IOException
        {
            int rowCount = in.readInt();
            List<Row> rows = new ArrayList<Row>(rowCount);
            for (int i = 0; i < rowCount; i++)
                rows.add(rowSerializer.deserialize(in, version));
            return new RangeSliceReply(rows, this);
        }

        public long serializedSize(RangeSliceReply rsr, int version)
        {
            int size = TypeSizes.NATIVE.sizeof(rsr.rows.size());
            for (Row row : rsr.rows)
                size += rowSerializer.serializedSize(row, version);
            return size;
        }
    }
}

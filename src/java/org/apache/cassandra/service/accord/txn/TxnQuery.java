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

import com.google.common.base.Preconditions;

import accord.api.Data;
import accord.api.Query;
import accord.api.Result;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.db.AccordData;
import org.apache.cassandra.utils.ObjectSizes;

public abstract class TxnQuery implements Query
{
    public static final TxnQuery ALL = new TxnQuery()
    {
        @Override
        public Result compute(Data data)
        {
            return (AccordData) data;
        }
    };

    public static final TxnQuery NONE = new TxnQuery()
    {
        @Override
        public Result compute(Data data)
        {
            return new AccordData();
        }
    };

    private static final long SIZE = ObjectSizes.measure(ALL);

    private TxnQuery() {}

    public long estimatedSizeOnHeap()
    {
        return SIZE;
    }

    public static final IVersionedSerializer<TxnQuery> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(TxnQuery query, DataOutputPlus out, int version) throws IOException
        {
            Preconditions.checkArgument(query == ALL || query == NONE);
            out.writeBoolean(query == ALL);
        }

        @Override
        public TxnQuery deserialize(DataInputPlus in, int version) throws IOException
        {
            return in.readBoolean() ? ALL : NONE;
        }

        @Override
        public long serializedSize(TxnQuery query, int version)
        {
            Preconditions.checkArgument(query == ALL || query == NONE);
            return TypeSizes.sizeof(query == ALL);
        }
    };
}

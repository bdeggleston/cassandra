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

package org.apache.cassandra.service.epaxos;

import java.io.DataInput;
import java.io.IOException;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
* TODO: explain why this is neccesary
*/
public enum Scope
{
    GLOBAL(ConsistencyLevel.SERIAL),
    LOCAL(ConsistencyLevel.LOCAL_SERIAL);

    final ConsistencyLevel cl;

    Scope(ConsistencyLevel cl)
    {
        this.cl = cl;
    }

    public static Scope get(ConsistencyLevel cl)
    {
        switch (cl)
        {
            case SERIAL:
                return GLOBAL;
            case LOCAL_SERIAL:
                return LOCAL;
            default:
                throw new IllegalArgumentException("Invalid serial consistency level: " + cl);
        }
    }

    public static Scope get(Instance instance)
    {
        return get(instance.getConsistencyLevel());
    }

    public static final IVersionedSerializer<Scope> serializer = new IVersionedSerializer<Scope>()
    {
        @Override
        public void serialize(Scope scope, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(scope.ordinal());
        }

        @Override
        public Scope deserialize(DataInput in, int version) throws IOException
        {
            return Scope.values()[in.readInt()];
        }

        @Override
        public long serializedSize(Scope scope, int version)
        {
            return 4;
        }
    };
}

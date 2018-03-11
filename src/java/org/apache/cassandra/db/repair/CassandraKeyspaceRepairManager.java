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

package org.apache.cassandra.db.repair;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.KeyspaceRepairManager;

public class CassandraKeyspaceRepairManager implements KeyspaceRepairManager
{
    private final Keyspace keyspace;

    public CassandraKeyspaceRepairManager(Keyspace keyspace)
    {
        this.keyspace = keyspace;
    }

    @Override
    public ListenableFuture prepareIncrementalRepair(UUID sessionID, Collection<Range<Token>> ranges, ExecutorService executor)
    {
        PendingAntiCompaction pac = new PendingAntiCompaction(sessionID, ranges, executor);
        return pac.run();
    }
}

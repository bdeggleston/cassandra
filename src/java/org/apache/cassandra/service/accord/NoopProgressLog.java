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

package org.apache.cassandra.service.accord;

import java.util.Set;
import javax.annotation.Nullable;

import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Status;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;

public enum NoopProgressLog implements ProgressLog, ProgressLog.Factory
{
    INSTANCE;

    @Override
    public ProgressLog create(CommandStore store)
    {
        return this;
    }

    @Override
    public void unwitnessed(TxnId txnId, RoutingKey homeKey, ProgressShard shard)
    {

    }

    @Override
    public void preaccepted(Command command, ProgressShard shard)
    {

    }

    @Override
    public void accepted(Command command, ProgressShard shard)
    {

    }

    @Override
    public void committed(Command command, ProgressShard shard)
    {

    }

    @Override
    public void readyToExecute(Command command, ProgressShard shard)
    {

    }

    @Override
    public void executed(Command command, ProgressShard shard)
    {

    }

    @Override
    public void invalidated(Command command, ProgressShard shard)
    {

    }

    @Override
    public void durableLocal(TxnId txnId)
    {

    }

    @Override
    public void durable(Command command, @Nullable Set<Node.Id> persistedOn)
    {

    }

    @Override
    public void durable(TxnId txnId, @Nullable Unseekables<?, ?> unseekables, ProgressShard shard)
    {

    }

    @Override
    public void waiting(TxnId blockedBy, Status.Known blockedUntil, Unseekables<?, ?> blockedOn)
    {

    }
}

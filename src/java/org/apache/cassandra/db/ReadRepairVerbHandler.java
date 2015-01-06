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

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.epaxos.EpaxosState;
import org.apache.cassandra.service.epaxos.ExecutionInfo;

import java.net.InetAddress;
import java.util.UUID;

public abstract class ReadRepairVerbHandler <T> implements IVerbHandler<T>
{
    protected void sendResponse(int id, InetAddress from)
    {
        WriteResponse response = new WriteResponse();
        MessagingService.instance().sendReply(response.createMessage(), id, from);
    }

    protected void doRepair(Mutation mutation, int id, InetAddress from)
    {
        mutation.apply();
        sendResponse(id, from);
    }

    public static class Normal extends ReadRepairVerbHandler<Mutation>
    {
        @Override
        public void doVerb(MessageIn<Mutation> message, int id)
        {
            doRepair(message.payload, id, message.from);
        }
    }

    /**
     * Applies read repairs to epaxos managed tables, as long as the local epaxos state
     * is at or past the remote state
     */
    public static class Epaxos extends ReadRepairVerbHandler<ExecutionInfo.Tuple<Mutation>>
    {
        private final EpaxosState state;

        public Epaxos(EpaxosState state)
        {
            this.state = state;
        }

        @Override
        public void doVerb(MessageIn<ExecutionInfo.Tuple<Mutation>> message, int id)
        {
            ExecutionInfo info = message.payload.executionInfo;
            Mutation mutation = message.payload.payload;
            assert mutation.getColumnFamilyIds().size() == 1;
            UUID cfId = mutation.getColumnFamilyIds().iterator().next();

            if (state.canApplyRepair(mutation.key(), cfId, info.epoch, info.executed))
            {
                doRepair(mutation, id, message.from);
            }
            else
            {
                sendResponse(id, message.from);
            }
        }

        public static final IVersionedSerializer<ExecutionInfo.Tuple<Mutation>> serializer
                = ExecutionInfo.Tuple.createSerializer(Mutation.serializer);
    }

}

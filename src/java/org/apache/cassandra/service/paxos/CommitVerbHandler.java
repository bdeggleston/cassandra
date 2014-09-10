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


import org.apache.cassandra.db.KeyspaceManager;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.WriteResponse;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.Tracing;

public class CommitVerbHandler implements IVerbHandler<Commit>
{
    private final Tracing tracing;
    private final SystemKeyspace systemKeyspace;
    private final KeyspaceManager keyspaceManager;
    private final MessagingService messagingService;
    private final PaxosManager paxosManager;

    public CommitVerbHandler(Tracing tracing, SystemKeyspace systemKeyspace, KeyspaceManager keyspaceManager, MessagingService messagingService, PaxosManager paxosManager)
    {
        this.tracing = tracing;
        this.systemKeyspace = systemKeyspace;
        this.keyspaceManager = keyspaceManager;
        this.messagingService = messagingService;
        this.paxosManager = paxosManager;
    }

    public void doVerb(MessageIn<Commit> message, int id)
    {
        paxosManager.commit(message.payload, tracing, systemKeyspace, keyspaceManager);

        WriteResponse response = new WriteResponse();
        tracing.trace("Enqueuing acknowledge to {}", message.from);
        messagingService.sendReply(response.createMessage(), id, message.from);
    }
}

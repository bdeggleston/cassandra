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
package org.apache.cassandra.repair;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.DBConfig;
import org.apache.cassandra.db.KeyspaceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.SyncComplete;
import org.apache.cassandra.repair.messages.SyncRequest;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.*;

/**
 * Task that make two nodes exchange (stream) some ranges (for a given table/cf).
 * This handle the case where the local node is neither of the two nodes that
 * must stream their range, and allow to register a callback to be called on
 * completion.
 */
public class StreamingRepairTask implements Runnable, StreamEventHandler
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingRepairTask.class);

    /** Repair session ID that this streaming task belongs */
    public final RepairJobDesc desc;
    public final SyncRequest request;

    private final DatabaseDescriptor databaseDescriptor;
    private final Schema schema;
    private final ActiveRepairService activeRepairService;
    private final KeyspaceManager keyspaceManager;
    private final StreamManager streamManager;
    private final MessagingService messagingService;
    private final DBConfig dbConfig;

    public StreamingRepairTask(RepairJobDesc desc, SyncRequest request, DatabaseDescriptor databaseDescriptor, Schema schema,  ActiveRepairService activeRepairService,
                               KeyspaceManager keyspaceManager, StreamManager streamManager, MessagingService messagingService, DBConfig dbConfig)
    {
        this.desc = desc;
        this.request = request;
        this.databaseDescriptor = databaseDescriptor;
        this.schema = schema;
        this.activeRepairService = activeRepairService;
        this.keyspaceManager = keyspaceManager;
        this.streamManager = streamManager;
        this.messagingService = messagingService;
        this.dbConfig = dbConfig;
    }

    public void run()
    {
        if (request.src.equals(databaseDescriptor.getBroadcastAddress()))
            initiateStreaming();
        else
            forwardToSource();
    }

    private void initiateStreaming()
    {
        long repairedAt = ActiveRepairService.UNREPAIRED_SSTABLE;
        if (desc.parentSessionId != null && activeRepairService.getParentRepairSession(desc.parentSessionId) != null)
            repairedAt = activeRepairService.getParentRepairSession(desc.parentSessionId).repairedAt;

        logger.info(String.format("[streaming task #%s] Performing streaming repair of %d ranges with %s", desc.sessionId, request.ranges.size(), request.dst));
        StreamResultFuture op = new StreamPlan("Repair", repairedAt, 1, databaseDescriptor, schema, keyspaceManager, streamManager, dbConfig)
                                    .flushBeforeTransfer(true)
                                    // request ranges from the remote node
                                    .requestRanges(request.dst, desc.keyspace, request.ranges, desc.columnFamily)
                                    // send ranges to the remote node
                                    .transferRanges(request.dst, desc.keyspace, request.ranges, desc.columnFamily)
                                    .execute();
        op.addEventListener(this);
    }

    private void forwardToSource()
    {
        logger.info(String.format("[repair #%s] Forwarding streaming repair of %d ranges to %s (to be streamed with %s)", desc.sessionId, request.ranges.size(), request.src, request.dst));
        messagingService.sendOneWay(request.createMessage(), request.src);
    }

    public void handleStreamEvent(StreamEvent event)
    {
        // Nothing to do here, all we care about is the final success or failure and that's handled by
        // onSuccess and onFailure
    }

    /**
     * If we succeeded on both stream in and out, reply back to the initiator.
     */
    public void onSuccess(StreamState state)
    {
        logger.info(String.format("[repair #%s] streaming task succeed, returning response to %s", desc.sessionId, request.initiator));
        messagingService.sendOneWay(new SyncComplete(desc, request.src, request.dst, true, MessagingService.instance.repairMessageSerializer).createMessage(), request.initiator);
    }

    /**
     * If we failed on either stream in or out, reply fail to the initiator.
     */
    public void onFailure(Throwable t)
    {
        messagingService.sendOneWay(new SyncComplete(desc, request.src, request.dst, false, MessagingService.instance.repairMessageSerializer).createMessage(), request.initiator);
    }
}

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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Result;
import accord.local.Node;
import accord.messages.Reply;
import accord.messages.Request;
import accord.txn.Txn;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.api.AccordScheduler;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

public class AccordService
{
    public static final AccordService instance = new AccordService();

    public final Node node;
    private final AccordMessageSink messageSink;
    public final AccordConfigurationService configService;
    private final AccordScheduler scheduler;
    private final AccordVerbHandler verbHandler;

    public static long uniqueNow()
    {
        return TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
    }

    private AccordService()
    {
        Node.Id localId = EndpointMapping.endpointToId(FBUtilities.getBroadcastAddressAndPort());
        this.messageSink = new AccordMessageSink();
        this.configService = new AccordConfigurationService(localId);
        this.scheduler = new AccordScheduler();
        this.node = new Node(localId,
                             messageSink,
                             configService,
                             AccordService::uniqueNow,
                             () -> null,
                             new AccordAgent(),
                             scheduler,
                             AccordCommandStores::new);
        this.verbHandler = new AccordVerbHandler(this.node);
    }

    public <T extends Request> IVerbHandler<T> verbHandler()
    {
        return verbHandler;
    }

    @VisibleForTesting
    public void createEpochFromConfigUnsafe()
    {
        configService.createEpochFromConfig();
    }

    public static boolean isFinalReply(Object reply)
    {
        return ((Reply) reply).isFinal();
    }

    public static long nowInMicros()
    {
        return TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
    }

    public TxnData coordinate(Txn txn)
    {
        try
        {
            Future<Result> future = node.coordinate(txn);
            Result result = future.get(2, TimeUnit.SECONDS);
            return (TxnData) result;
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (TimeoutException e)
        {
            throw new ReadTimeoutException(ConsistencyLevel.ANY, 0, 0, false);
        }
    };

    @VisibleForTesting
    AccordMessageSink messageSink()
    {
        return messageSink;
    }

    public void setCacheSize(long kb)
    {
        long bytes = kb << 10;
        AccordCommandStores commandStores = (AccordCommandStores) node.commandStores();
        commandStores.setCacheSize(bytes);
    }
}

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
package org.apache.cassandra.sink;

import java.net.InetAddress;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;

public class SinkManager
{
    public static SinkManager instance = new SinkManager();

    private final Set<IMessageSink> messageSinks = new CopyOnWriteArraySet<>();
    private final Set<IRequestSink> requestSinks = new CopyOnWriteArraySet<>();

    public void add(IMessageSink ms)
    {
        messageSinks.add(ms);
    }

    public void add(IRequestSink rs)
    {
        requestSinks.add(rs);
    }

    public void remove(IMessageSink ms)
    {
        messageSinks.remove(ms);
    }

    public void remove(IRequestSink rs)
    {
        requestSinks.remove(rs);
    }

    public void clear()
    {
        messageSinks.clear();
        requestSinks.clear();
    }

    public MessageOut processOutboundMessage(MessageOut message, int id, InetAddress to)
    {
        if (messageSinks.isEmpty())
            return message;

        for (IMessageSink ms : messageSinks)
        {
            message = ms.handleMessage(message, id, to);
            if (message == null)
                return null;
        }
        return message;
    }

    public MessageIn processInboundMessage(MessageIn message, int id)
    {
        if (messageSinks.isEmpty())
            return message;

        for (IMessageSink ms : messageSinks)
        {
            message = ms.handleMessage(message, id, null);
            if (message == null)
                return null;
        }
        return message;
    }

    public IMutation processWriteRequest(IMutation mutation)
    {
        if (requestSinks.isEmpty())
            return mutation;

        for (IRequestSink rs : requestSinks)
        {
            mutation = rs.handleWriteRequest(mutation);
            if (mutation == null)
                return null;
        }
        return mutation;
    }
}

package org.apache.cassandra.db;
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


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.LocatorConfig;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SystemKeyspaceTest
{
    public static final DatabaseDescriptor databaseDescriptor = DatabaseDescriptor.instance;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.init();
    }

    @Test
    public void testLocalTokens()
    {
        // Remove all existing tokens
        Collection<Token> current = databaseDescriptor.getSystemKeyspace().loadTokens().asMap().get(DatabaseDescriptor.instance.getLocalAddress());
        if (current != null && !current.isEmpty())
            databaseDescriptor.getSystemKeyspace().updateTokens(current);

        List<Token> tokens = new ArrayList<Token>()
        {{
            for (int i = 0; i < 9; i++)
                add(new BytesToken(ByteBufferUtil.bytes(String.format("token%d", i)), databaseDescriptor.getLocatorConfig().getPartitioner()));
        }};

        databaseDescriptor.getSystemKeyspace().updateTokens(tokens);
        int count = 0;

        for (Token tok : databaseDescriptor.getSystemKeyspace().getSavedTokens())
            assert tokens.get(count++).equals(tok);
    }

    @Test
    public void testNonLocalToken() throws UnknownHostException
    {
        BytesToken token = new BytesToken(ByteBufferUtil.bytes("token3"), databaseDescriptor.getLocatorConfig().getPartitioner());
        InetAddress address = InetAddress.getByName("127.0.0.2");
        databaseDescriptor.getSystemKeyspace().updateTokens(address, Collections.<Token>singletonList(token));
        assert databaseDescriptor.getSystemKeyspace().loadTokens().get(address).contains(token);
        databaseDescriptor.getSystemKeyspace().removeEndpoint(address);
        assert !databaseDescriptor.getSystemKeyspace().loadTokens().containsValue(token);
    }

    @Test
    public void testLocalHostID()
    {
        UUID firstId = databaseDescriptor.getSystemKeyspace().getLocalHostId();
        UUID secondId = databaseDescriptor.getSystemKeyspace().getLocalHostId();
        assert firstId.equals(secondId) : String.format("%s != %s%n", firstId.toString(), secondId.toString());
    }
}

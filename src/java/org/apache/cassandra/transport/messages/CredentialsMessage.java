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
package org.apache.cassandra.transport.messages;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.transport.ProtocolException;
import io.netty.buffer.ByteBuf;

import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;

/**
 * Message to indicate that the server is ready to receive requests.
 */
public class CredentialsMessage extends Message.Request
{
    public static class Codec implements Message.Codec<CredentialsMessage>
    {
        private final IAuthenticator authenticator;

        public Codec(IAuthenticator authenticator)
        {
            this.authenticator = authenticator;
        }

        @Override
        public CredentialsMessage decode(ByteBuf body, int version)
        {
            if (version > 1)
                throw new ProtocolException("Legacy credentials authentication is not supported in " +
                        "protocol versions > 1. Please use SASL authentication via a SaslResponse message");

            Map<String, String> credentials = CBUtil.readStringMap(body);
            return new CredentialsMessage(credentials, authenticator);
        }

        @Override
        public void encode(CredentialsMessage msg, ByteBuf dest, int version)
        {
            CBUtil.writeStringMap(msg.credentials, dest);
        }

        @Override
        public int encodedSize(CredentialsMessage msg, int version)
        {
            return CBUtil.sizeOfStringMap(msg.credentials);
        }
    };

    public final Map<String, String> credentials;

    private final IAuthenticator authenticator;

    public CredentialsMessage(IAuthenticator authenticator)
    {
        this(new HashMap<String, String>(), authenticator);
    }

    private CredentialsMessage(Map<String, String> credentials, IAuthenticator authenticator)
    {
        super(Message.Type.CREDENTIALS);
        this.credentials = credentials;
        this.authenticator = authenticator;
    }

    public Message.Response execute(QueryState state)
    {
        try
        {
            AuthenticatedUser user = authenticator.authenticate(credentials);
            state.getClientState().login(user);
            return new ReadyMessage();
        }
        catch (AuthenticationException e)
        {
            return ErrorMessage.fromException(e);
        }
    }

    @Override
    public String toString()
    {
        return "CREDENTIALS " + credentials;
    }
}

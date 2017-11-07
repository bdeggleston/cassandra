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
package org.apache.cassandra.cql3.statements;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.dht.Datacenters;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class GrantPermissionsStatement extends PermissionsManagementStatement
{
    public final ImmutableSet<String> datacenters;

    public GrantPermissionsStatement(Set<Permission> permissions, IResource resource, RoleName grantee, List<String> datacenters)
    {
        super(permissions, resource, grantee);
        this.datacenters = ImmutableSet.copyOf(datacenters);
    }

    private void validateDatacenters()
    {
        if (datacenters.isEmpty())
            return;

        Set<String> validDcs = Datacenters.getValidDatacenters();
        Set<String> invalidDcs = new HashSet<>();
        for (String dc: Iterables.filter(datacenters, dc -> !validDcs.contains(dc)))
        {
            invalidDcs.add(dc);
        }

        if (!invalidDcs.isEmpty())
        {
            throw new ConfigurationException(invalidDcs.toString() + " are not valid datacenters");
        }
    }

    public ResultMessage execute(ClientState state) throws RequestValidationException, RequestExecutionException
    {
        validateDatacenters();
        DatabaseDescriptor.getAuthorizer().grant(state.getUser(), permissions, resource, grantee, datacenters);
        return null;
    }
}

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

package org.apache.cassandra.dht;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class Datacenters
{
    /*
     * @return a set of valid DC names
     */
    public static Set<String> getValidDatacenters()
    {
        final Set<String> validDataCenters = new HashSet<>();
        final IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();

        // Add data center of localhost.
        validDataCenters.add(snitch.getDatacenter(FBUtilities.getBroadcastAddress()));
        // Fetch and add DCs of all peers.
        for (final InetAddress peer : StorageService.instance.getTokenMetadata().getAllEndpoints())
        {
            validDataCenters.add(snitch.getDatacenter(peer));
        }

        return validDataCenters;
    }
}

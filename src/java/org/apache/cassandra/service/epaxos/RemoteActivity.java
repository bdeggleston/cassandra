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

package org.apache.cassandra.service.epaxos;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * stub class used for tracking mutations LOCAL_SERIAL
 * mutations from other datacenters
 *
 * notes:
 *  - table structure: ((key), cfId, dc), epoch, executionCount
 *  - because of KeyState's timestamp tracking, we should be able to blindly insert with the same ts as recorded mutations
 *  - maybe just send messages of current epochs for token ranges for GC
 */
// TODO: all of this, the whole thing
public class RemoteActivity
{
    public void recordMutation(String dc, ByteBuffer key, UUID cfId, ExecutionInfo executionInfo)
    {

    }

    public Map<Scope.DC, ExecutionInfo> getExecutionInfo(ByteBuffer key, UUID cfId)
    {
        return Collections.emptyMap();
    }

    public boolean managesCfId(UUID cfId)
    {
        // TODO: this will be difficult without a lightweight token state, given the proposed datamodel
        return false;
    }
}

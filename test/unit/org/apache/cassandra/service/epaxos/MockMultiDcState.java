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

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

class MockMultiDcState extends EpaxosState
{
    final Map<InetAddress, String> dcs = new HashMap<>();

    @Override
    protected TokenStateManager createTokenStateManager(Scope scope)
    {
        return new MockTokenStateManager(scope);
    }

    @Override
    protected String getDc()
    {
        return AbstractEpaxosTest.DC1;
    }

    @Override
    protected String getDc(InetAddress endpoint)
    {
        String dc = dcs.get(endpoint);
        assert dc != null : endpoint + " not in dc map";
        return dc;
    }
}

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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.commitlog.ReplayPosition;

public class EpaxosExecutionInfoTest extends AbstractEpaxosTest
{
    private static String DC2 = "DC2";

    private static class MockDcState extends EpaxosState
    {
        final Map<InetAddress, String> dcs = new HashMap<>();
        {{
            dcs.put(LOCALHOST, DC1);
        }}

        @Override
        protected TokenStateManager createTokenStateManager(Scope scope)
        {
            return new MockTokenStateManager(scope);
        }

        @Override
        protected String getDc(InetAddress endpoint)
        {
            String dc = dcs.get(endpoint);
            assert dc != null : endpoint + " not in dc map";
            return dc;
        }
    }

    private static boolean ksManagerContains(EpaxosState state, Scope scope, QueryInstance instance)
    {
        return state.getKeyStateManager(scope).loadKeyState(instance.getQuery().getCfKey()).contains(instance.getId());
    }


    /**
     * Test that LOCAL and GLOBAL scopes are included for local DCs
     * and GLOBAL only for remote
     */
    @Test
    public void getEpochExecutionInfo() throws Exception
    {
        MockDcState state = new MockDcState();
        InetAddress localAddr = InetAddress.getByName("127.0.0.2");
        InetAddress remoteAddr = InetAddress.getByName("127.0.0.3");
        state.dcs.put(localAddr, DC1);
        state.dcs.put(remoteAddr, DC2);

        QueryInstance globalInstance = state.createQueryInstance(getSerializedCQLRequest(0, 0, ConsistencyLevel.SERIAL));
        globalInstance.preaccept(state.getCurrentDependencies(globalInstance).left);

        QueryInstance localInstance = state.createQueryInstance(getSerializedCQLRequest(0, 0, ConsistencyLevel.LOCAL_SERIAL));
        localInstance.preaccept(state.getCurrentDependencies(localInstance).left);

        CfKey cfKey = globalInstance.getQuery().getCfKey();
        Assert.assertEquals(cfKey, localInstance.getQuery().getCfKey());

        state.recordExecuted(globalInstance, new ReplayPosition(0, 0), 0l);
        state.recordExecuted(localInstance, new ReplayPosition(0, 0), 0l);

        // check that the instances are recorded in the correct key state managers
        Assert.assertTrue(ksManagerContains(state, Scope.GLOBAL, globalInstance));
        Assert.assertFalse(ksManagerContains(state, Scope.GLOBAL, localInstance));

        Assert.assertTrue(ksManagerContains(state, Scope.LOCAL, localInstance));
        Assert.assertFalse(ksManagerContains(state, Scope.LOCAL, globalInstance));

        Map<Scope.DC, ExecutionInfo> infos;
        // dc local nodes should receive info for both scopes
        infos = state.getEpochExecutionInfo(cfKey.key, cfKey.cfId, localAddr);
        Assert.assertEquals(2, infos.size());
        Assert.assertEquals(new ExecutionInfo(0l, 1l), infos.get(Scope.DC.global()));
        Assert.assertEquals(new ExecutionInfo(0l, 1l), infos.get(Scope.DC.local(DC1)));

        // check that the local execution info is only added for dc-local nodes
        infos = state.getEpochExecutionInfo(cfKey.key, cfKey.cfId, remoteAddr);
        Assert.assertEquals(1, infos.size());
        Assert.assertEquals(new ExecutionInfo(0l, 1l), infos.get(Scope.DC.global()));
    }
}

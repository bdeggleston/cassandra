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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class EpaxosExecutionInfoTest extends AbstractEpaxosTest
{
    private static String DC2 = "DC2";
    private static int VERSION = MessagingService.current_version;
    private static MessagingService.Verb VERB = MessagingService.Verb.READ_REPAIR;

    private static InetAddress LOCAL_ADDRESS;
    private static InetAddress REMOTE_ADDRESS;

    static
    {
        try
        {
            LOCAL_ADDRESS = InetAddress.getByName("127.0.0.2");
            REMOTE_ADDRESS = InetAddress.getByName("127.0.0.3");
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    private static class MockDcState extends EpaxosState
    {
        final Map<InetAddress, String> dcs = new HashMap<>();
        {{
            dcs.put(LOCALHOST, DC1);
            dcs.put(LOCAL_ADDRESS, DC1);
            dcs.put(REMOTE_ADDRESS, DC2);
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

    private MockDcState state = null;
    private QueryInstance globalInstance = null;
    private QueryInstance localInstance = null;
    private CfKey cfKey = null;
    private Mutation mutation = null;

    private static final ExecutionInfo PRESENT = new ExecutionInfo(0l, 1l);
    private static final ExecutionInfo FUTURE = new ExecutionInfo(0l, 2l);

    @Before
    public void setUp() throws Exception
    {
        clearAll();

        state = new MockDcState();

        globalInstance = state.createQueryInstance(getSerializedCQLRequest(0, 0, ConsistencyLevel.SERIAL));
        globalInstance.preaccept(state.getCurrentDependencies(globalInstance).left);

        localInstance = state.createQueryInstance(getSerializedCQLRequest(0, 0, ConsistencyLevel.LOCAL_SERIAL));
        localInstance.preaccept(state.getCurrentDependencies(localInstance).left);

        cfKey = globalInstance.getQuery().getCfKey();
        Assert.assertEquals(cfKey, localInstance.getQuery().getCfKey());

        state.recordExecuted(globalInstance, new ReplayPosition(0, 0), 0l);
        state.recordExecuted(localInstance, new ReplayPosition(0, 0), 0l);
        mutation = new Mutation(ksm.name, cfKey.key);
    }

    /**
     * Test that LOCAL and GLOBAL scopes are included for local DCs
     * and GLOBAL only for remote
     */
    @Test
    public void getEpochExecutionInfo() throws Exception
    {
        // check that the instances are recorded in the correct key state managers
        Assert.assertTrue(ksManagerContains(state, Scope.GLOBAL, globalInstance));
        Assert.assertFalse(ksManagerContains(state, Scope.GLOBAL, localInstance));

        Assert.assertTrue(ksManagerContains(state, Scope.LOCAL, localInstance));
        Assert.assertFalse(ksManagerContains(state, Scope.LOCAL, globalInstance));

        Map<Scope, ExecutionInfo> infos;
        // dc local nodes should receive info for both scopes
        infos = state.getEpochExecutionInfo(cfKey.key, cfKey.cfId, LOCAL_ADDRESS);
        Assert.assertEquals(2, infos.size());
        Assert.assertEquals(new ExecutionInfo(0l, 1l), infos.get(Scope.GLOBAL));
        Assert.assertEquals(new ExecutionInfo(0l, 1l), infos.get(Scope.LOCAL));

        // check that the local execution info is only added for dc-local nodes
        infos = state.getEpochExecutionInfo(cfKey.key, cfKey.cfId, REMOTE_ADDRESS);
        Assert.assertEquals(1, infos.size());
        Assert.assertEquals(new ExecutionInfo(0l, 1l), infos.get(Scope.GLOBAL));
    }

    private MessageOut<Mutation> makeMessageOut()
    {
        return new MessageOut<>(VERB, mutation, Mutation.serializer);
    }

    private MessageIn<Mutation> makeParamMessageIn(InetAddress from, Map<Scope, ExecutionInfo> infos) throws IOException
    {
        Map<String, byte[]> params = new HashMap<>();
        params.put(EpaxosState.EXECUTION_INFO_PARAMETER, EpaxosState.serializeMessageExecutionParameters(infos, VERSION));
        return MessageIn.create(from, mutation, params, VERB, VERSION);
    }

    private MessageIn<Mutation> makeMessageIn(InetAddress from, Map<String, byte[]> params)
    {
       return MessageIn.create(from, mutation, params, VERB, VERSION);
    }

    /**
     * Tests the message parameters that are sent with read repair messages
     */
    @Test
    public void dcLocalMessageParameterSend() throws Exception
    {
        MessageOut<Mutation> msg = makeMessageOut();

        // check that message parameters aren't added for keys we don't have info for
        msg = state.maybeAddExecutionInfo(key(10), cfKey.cfId, msg, VERSION, LOCAL_ADDRESS);
        Assert.assertFalse(msg.parameters.containsKey(EpaxosState.EXECUTION_INFO_PARAMETER));

        // ... but are if we do
        msg = state.maybeAddExecutionInfo(cfKey.key, cfKey.cfId, msg, MessagingService.current_version, LOCAL_ADDRESS);
        Assert.assertTrue(msg.parameters.containsKey(EpaxosState.EXECUTION_INFO_PARAMETER));

        Map<Scope, ExecutionInfo> infos;
        // dc local nodes should receive info for both scopes
        infos = EpaxosState.getMessageExecutionInfo(msg.parameters, VERSION);
        Assert.assertEquals(2, infos.size());
        Assert.assertEquals(new ExecutionInfo(0l, 1l), infos.get(Scope.GLOBAL));
        Assert.assertEquals(new ExecutionInfo(0l, 1l), infos.get(Scope.LOCAL));

        // should be ok to apply, since execution info will be identical
        MessageIn<Mutation> msgIn = makeMessageIn(LOCAL_ADDRESS, msg.parameters);
        Assert.assertTrue(state.shouldApplyRepair(cfKey.key, cfKey.cfId, msgIn));
    }

    /**
     * Tests the message parameters that are sent with read repair messages
     */
    @Test
    public void dcRemoteMessageParameterSend() throws Exception
    {
        MessageOut<Mutation> msg = makeMessageOut();

        msg = state.maybeAddExecutionInfo(cfKey.key, cfKey.cfId, msg, MessagingService.current_version, REMOTE_ADDRESS);
        Assert.assertTrue(msg.parameters.containsKey(EpaxosState.EXECUTION_INFO_PARAMETER));

        Map<Scope, ExecutionInfo> infos;
        // dc local nodes should receive info for both scopes
        infos = EpaxosState.getMessageExecutionInfo(msg.parameters, VERSION);
        Assert.assertEquals(1, infos.size());
        Assert.assertEquals(new ExecutionInfo(0l, 1l), infos.get(Scope.GLOBAL));
    }

    @Test
    public void dcLocalMessageParameterReceive() throws Exception
    {
        Map<Scope, ExecutionInfo> infos = new HashMap<>();

        // no-op case
        Assert.assertTrue(state.shouldApplyRepair(cfKey.key, cfKey.cfId, makeMessageIn(LOCAL_ADDRESS, null)));

        Assert.assertTrue(state.shouldApplyRepair(cfKey.key, cfKey.cfId, makeMessageIn(LOCAL_ADDRESS, new HashMap<String, byte[]>())));

        // success cases
        infos.clear();
        infos.put(Scope.GLOBAL, PRESENT);
        infos.put(Scope.LOCAL, PRESENT);
        Assert.assertTrue(state.shouldApplyRepair(cfKey.key, cfKey.cfId, makeParamMessageIn(LOCAL_ADDRESS, infos)));

        infos.clear();
        infos.put(Scope.LOCAL, PRESENT);
        Assert.assertTrue(state.shouldApplyRepair(cfKey.key, cfKey.cfId, makeParamMessageIn(LOCAL_ADDRESS, infos)));

        infos.clear();
        infos.put(Scope.GLOBAL, PRESENT);
        Assert.assertTrue(state.shouldApplyRepair(cfKey.key, cfKey.cfId, makeParamMessageIn(LOCAL_ADDRESS, infos)));

        // global failure
        infos.clear();
        infos.put(Scope.GLOBAL, FUTURE);
        Assert.assertFalse(state.shouldApplyRepair(cfKey.key, cfKey.cfId, makeParamMessageIn(LOCAL_ADDRESS, infos)));
        infos.put(Scope.LOCAL, PRESENT);
        Assert.assertFalse(state.shouldApplyRepair(cfKey.key, cfKey.cfId, makeParamMessageIn(LOCAL_ADDRESS, infos)));

        // local failure
        infos.clear();
        infos.put(Scope.LOCAL, FUTURE);
        Assert.assertFalse(state.shouldApplyRepair(cfKey.key, cfKey.cfId, makeParamMessageIn(LOCAL_ADDRESS, infos)));
        infos.put(Scope.GLOBAL, PRESENT);
        Assert.assertFalse(state.shouldApplyRepair(cfKey.key, cfKey.cfId, makeParamMessageIn(LOCAL_ADDRESS, infos)));
    }

    @Test
    public void dcRemoteMessageParameterReceive() throws Exception
    {
        Map<Scope, ExecutionInfo> infos = new HashMap<>();

        // success cases
        infos.clear();
        infos.put(Scope.GLOBAL, PRESENT);
        Assert.assertTrue(state.shouldApplyRepair(cfKey.key, cfKey.cfId, makeParamMessageIn(REMOTE_ADDRESS, infos)));

        // local info sent from remote dcs should be ignored
        infos.put(Scope.LOCAL, FUTURE);
        Assert.assertTrue(state.shouldApplyRepair(cfKey.key, cfKey.cfId, makeParamMessageIn(REMOTE_ADDRESS, infos)));

        // global failure
        infos.clear();
        infos.put(Scope.GLOBAL, FUTURE);
        Assert.assertFalse(state.shouldApplyRepair(cfKey.key, cfKey.cfId, makeParamMessageIn(REMOTE_ADDRESS, infos)));
        infos.put(Scope.LOCAL, PRESENT);
        Assert.assertFalse(state.shouldApplyRepair(cfKey.key, cfKey.cfId, makeParamMessageIn(REMOTE_ADDRESS, infos)));
    }
}

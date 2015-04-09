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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.dht.Token;

public class EpaxosTokenStateMaintenanceTest extends AbstractEpaxosTest
{

    private static class AlwaysReplicatingMaintenanceTask extends TokenStateMaintenanceTask
    {
        private AlwaysReplicatingMaintenanceTask(EpaxosState state, TokenStateManager tokenStateManager)
        {
            super(state, tokenStateManager);
        }

        @Override
        protected boolean replicatesTokenForKeyspace(Token token, UUID cfId)
        {
            return true;
        }
    }
    /**
     * Epoch should be incremented for a token state if it's executions
     * for it's currect epoch exceed the state's threshold
     */
    @Test
    public void epochIsIncremented()
    {

        final AtomicReference<EpochInstance> preaccepted = new AtomicReference<>();
        MockCallbackState state = new MockCallbackState(3, 0) {
            @Override
            public void preaccept(Instance instance)
            {
                assert preaccepted.get() == null;
                preaccepted.set((EpochInstance) instance);
            }
        };

        TokenState ts = state.tokenStateManager.get(TOKEN, CFID);
        ts.setEpoch(5);
        Assert.assertNotNull(ts);

        int threshold = state.getEpochIncrementThreshold(CFID);
        while (ts.getExecutions() < threshold)
        {
            new AlwaysReplicatingMaintenanceTask(state, state.tokenStateManager).run();
            Assert.assertNull(preaccepted.get());
            ts.recordExecution();
        }
        Assert.assertEquals(threshold, ts.getExecutions());
        new AlwaysReplicatingMaintenanceTask(state, state.tokenStateManager).run();

        EpochInstance instance = preaccepted.get();
        Assert.assertNotNull(instance);
        Assert.assertEquals(ts.getToken(), instance.getToken());
        Assert.assertEquals(ts.getEpoch() + 1, instance.getEpoch());
        Assert.assertEquals(ts.getCfId(), instance.getCfId());
    }

    /**
     * If the maintenance task encounters a token state with
     * recovery-required, it should start a local recovery
     */
    @Test
    public void recoveryRequiredStartsRecovery()
    {
        class FRCall
        {
            public final Token token;
            public final UUID cfId;
            public final long epoch;

            FRCall(Token token, UUID cfId, long epoch)
            {
                this.token = token;
                this.cfId = cfId;
                this.epoch = epoch;
            }
        }

        final AtomicReference<FRCall> call = new AtomicReference<>();
        MockCallbackState state = new MockCallbackState(3, 0) {
            public void startLocalFailureRecovery(Token token, UUID cfId, long epoch)
            {
                call.set(new FRCall(token, cfId, epoch));
            }
        };

        TokenState ts = state.tokenStateManager.get(TOKEN, CFID);
        Assert.assertNotNull(ts);
        ts.setState(TokenState.State.RECOVERY_REQUIRED);

        Assert.assertNull(call.get());
        new AlwaysReplicatingMaintenanceTask(state, state.tokenStateManager).run();
        FRCall frCall = call.get();
        Assert.assertEquals(ts.getToken(), frCall.token);
        Assert.assertEquals(ts.getCfId(), frCall.cfId);
        Assert.assertEquals(0, frCall.epoch);
    }
}

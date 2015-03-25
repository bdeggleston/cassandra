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

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.utils.ByteBufferUtil;

public class EpaxosTokenStateMaintenanceTest extends AbstractEpaxosTest
{

    @Before
    public void setUp()
    {
        clearTokenStates();
        clearKeyStates();
    }

    private static class EpochMaintenanceTask extends TokenStateMaintenanceTask
    {
        private EpochMaintenanceTask(EpaxosState state, TokenStateManager tokenStateManager)
        {
            super(state, tokenStateManager);
        }

        @Override
        protected boolean replicatesTokenForKeyspace(Token token, UUID cfId)
        {
            return true;
        }

        @Override
        protected void checkTokenCoverage()
        {
            // no-op
        }

        @Override
        protected boolean shouldRun()
        {
            return true;
        }
    }

    private static class TokenCoverageMaintenanceTask extends TokenStateMaintenanceTask
    {
        private TokenCoverageMaintenanceTask(EpaxosState state, TokenStateManager tokenStateManager)
        {
            super(state, tokenStateManager);
        }

        @Override
        protected void updateEpochs()
        {
            // no-op
        }

        Set<Token> normalTokens = Sets.newHashSet();

        @Override
        protected Set<Token> getReplicatedTokens(String ksName)
        {
            return normalTokens;
        }

        Set<Token> pendingTokens = Sets.newHashSet();

        @Override
        protected Set<Token> getPendingReplicatedTokens(String ksName)
        {
            return pendingTokens;
        }

        @Override
        protected String getKsName(UUID cfId)
        {
            return "ks";
        }

        @Override
        protected boolean shouldRun()
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
            new EpochMaintenanceTask(state, state.tokenStateManager).run();
            Assert.assertNull(preaccepted.get());
            ts.recordExecution();
        }
        Assert.assertEquals(threshold, ts.getExecutions());
        new EpochMaintenanceTask(state, state.tokenStateManager).run();

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
            @Override
            public void startLocalFailureRecovery(Token token, UUID cfId, long epoch)
            {
                call.set(new FRCall(token, cfId, epoch));
            }
        };

        TokenState ts = state.tokenStateManager.get(TOKEN, CFID);
        Assert.assertNotNull(ts);
        ts.setState(TokenState.State.RECOVERY_REQUIRED);

        Assert.assertNull(call.get());
        new EpochMaintenanceTask(state, state.tokenStateManager).run();
        FRCall frCall = call.get();
        Assert.assertNotNull(frCall);
        Assert.assertEquals(ts.getToken(), frCall.token);
        Assert.assertEquals(ts.getCfId(), frCall.cfId);
        Assert.assertEquals(0, frCall.epoch);
    }

    @Test
    public void tokenCoverageNewToken()
    {
        final AtomicReference<TokenInstance> preaccepted = new AtomicReference<>();
        MockCallbackState state = new MockCallbackState(3, 0) {
            @Override
            public Object process(Instance instance, ConsistencyLevel cl) throws WriteTimeoutException
            {
                preaccepted.set((TokenInstance) instance);
                return null;
            }
        };

        state.tokenStateManager.get(MockTokenStateManager.TOKEN, CFID);
        Token newToken = DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(5));

        TokenCoverageMaintenanceTask task = new TokenCoverageMaintenanceTask(state, state.tokenStateManager);
        task.normalTokens.add(MockTokenStateManager.TOKEN);
        task.normalTokens.add(newToken);

        Assert.assertNull(preaccepted.get());
        task.run();
        TokenInstance instance = preaccepted.get();
        Assert.assertNotNull(instance);
        Assert.assertEquals(CFID, instance.getCfId());
        Assert.assertEquals(newToken, instance.getToken());

    }
}

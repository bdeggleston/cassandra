package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class EpaxosFailureRecoveryTest extends AbstractEpaxosTest
{
    @Before
    public void setUp()
    {
        clearKeyStates();
        clearTokenStates();
    }

    private static class InstrumentedFailureRecoveryTask extends FailureRecoveryTask
    {
        private InstrumentedFailureRecoveryTask(EpaxosState state, Token token, UUID cfId, long epoch, Scope scope)
        {
            super(state, token, cfId, epoch, scope);
        }

        @Override
        protected Collection<InetAddress> getEndpoints(Range<Token> range)
        {
            return Lists.newArrayList(LOCALHOST, LOCAL_ADDRESS, REMOTE_ADDRESS);
        }

        static class InstanceStreamRequest
        {
            private final InetAddress endpoint;
            private final UUID cfId;
            private final Range<Token> range;
            private final Scope[] scopes;

            private InstanceStreamRequest(InetAddress endpoint, UUID cfId, Range<Token> range, Scope... scopes)
            {
                this.endpoint = endpoint;
                this.cfId = cfId;
                this.range = range;
                this.scopes = scopes;
            }
        }

        List<InstanceStreamRequest> rangeRequests = new ArrayList<>();

        @Override
        protected StreamPlan createStreamPlan(String name)
        {
            return new StreamPlan(name) {

                @Override
                public StreamPlan requestEpaxosRange(InetAddress from, UUID cfId, Range<Token> range, Scope... scopes)
                {
                    rangeRequests.add(new InstanceStreamRequest(from, cfId, range, scopes));
                    return super.requestEpaxosRange(from, cfId, range, scopes);
                }
            };
        }

        StreamPlan streamPlan = null;

        @Override
        protected void runStreamPlan(StreamPlan streamPlan)
        {
            this.streamPlan = streamPlan;
        }

        static class RepairRequest
        {
            private final Range<Token> range;
            private final boolean isLocal;

            RepairRequest(Range<Token> range, boolean isLocal)
            {
                this.range = range;
                this.isLocal = isLocal;
            }
        }

        List<RepairRequest> repairRequests = new ArrayList<>();

        @Override
        protected void runRepair(Range<Token> range, boolean isLocal)
        {
            repairRequests.add(new RepairRequest(range, isLocal));
        }
    }

    @Test
    public void preRecover() throws Exception
    {
        final Set<UUID> deleted = Sets.newHashSet();

        EpaxosState state = new MockVerbHandlerState(){

            @Override
            void deleteInstance(UUID id)
            {
                deleted.add(id);
            }
        };

        final TokenState tokenState = new TokenState(range(TOKEN0, TOKEN0), CFID, 2, 5);
        Assert.assertEquals(TokenState.State.NORMAL, tokenState.getState());
        Assert.assertFalse(state.managesCfId(CFID));

        // make a bunch of keys & instance ids
        final Set<ByteBuffer> keys = Sets.newHashSet();
        Set<UUID> expectedIds = Sets.newHashSet();

        for (int i=0; i<10; i++)
        {
            ByteBuffer key = ByteBufferUtil.bytes(i);
            keys.add(key);

            KeyState ks = state.getKeyStateManager(DEFAULT_SCOPE).loadKeyState(key, CFID);
            UUID prev = null;
            for (int j=0; j<9; j++)
            {
                UUID id = UUIDGen.getTimeUUID();
                expectedIds.add(id);
                ks.getDepsAndAdd(id);
                switch (j%3)
                {
                    case 2:
                        ks.markExecuted(id, null, new ReplayPosition(0, 0), 0);
                    case 1:
                        ks.markAcknowledged(Sets.newHashSet(id), prev);
                }
                prev = id;
            }

            Assert.assertTrue(ks.getActiveInstanceIds().size() > 0);
            Assert.assertTrue(ks.getEpochExecutions().size() > 0);
        }

        FailureRecoveryTask task = new FailureRecoveryTask(state, TOKEN0, CFID, 3, DEFAULT_SCOPE)
        {
            @Override
            protected TokenState getTokenState()
            {
                return tokenState;
            }
        };

        Assert.assertTrue(state.managesCfId(CFID));
        Assert.assertTrue(deleted.isEmpty());
        for (ByteBuffer key: keys)
        {
            Assert.assertTrue(state.getKeyStateManager(DEFAULT_SCOPE).managesKey(key, CFID));
        }

        task.preRecover();

        // all of the instances and key states should have been deleted
        Assert.assertEquals(TokenState.State.PRE_RECOVERY, tokenState.getState());
        Assert.assertEquals(expectedIds, deleted);
        for (ByteBuffer key: keys)
        {
            Assert.assertFalse(state.getKeyStateManager(DEFAULT_SCOPE).managesKey(key, CFID));
        }
    }

    @Test
    public void preRecoverBailsIfNotBehindRemoteEpoch()
    {
        EpaxosState state = new MockVerbHandlerState();
        TokenState tokenState = state.getTokenStateManager(DEFAULT_SCOPE).get(TOKEN0, CFID);
        tokenState.setEpoch(2);
        state.getTokenStateManager(DEFAULT_SCOPE).save(tokenState);
        Assert.assertEquals(TokenState.State.NORMAL, tokenState.getState());
        FailureRecoveryTask task = new FailureRecoveryTask(state, TOKEN0, CFID, 2, DEFAULT_SCOPE);

        task.preRecover();
        Assert.assertEquals(TokenState.State.NORMAL, tokenState.getState());
    }


    @Test
    public void preRecoverAlwaysContinuesIfTokenStateIsNotNormal()
    {

        EpaxosState state = new MockVerbHandlerState();
        TokenState tokenState = state.getTokenStateManager(DEFAULT_SCOPE).get(TOKEN0, CFID);
        tokenState.setEpoch(2);
        tokenState.setState(TokenState.State.RECOVERY_REQUIRED);
        FailureRecoveryTask task = new FailureRecoveryTask(state, TOKEN0, CFID, 0, DEFAULT_SCOPE);

        task.preRecover();
        Assert.assertEquals(TokenState.State.PRE_RECOVERY, tokenState.getState());
    }

    @Test
    public void recoverInstancesTestGlobalScope()
    {
        EpaxosState state = new MockVerbHandlerState();
        TokenState tokenState = state.getTokenStateManager(Scope.GLOBAL).get(TOKEN0, CFID);
        tokenState.setEpoch(2);
        tokenState.setState(TokenState.State.PRE_RECOVERY);

        InstrumentedFailureRecoveryTask task = new InstrumentedFailureRecoveryTask(state, TOKEN0, CFID, 0, Scope.GLOBAL);

        task.recoverInstances();
        Assert.assertEquals(TokenState.State.RECOVERING_INSTANCES, tokenState.getState());

        Assert.assertNotNull(task.streamPlan);

        Assert.assertEquals(2, task.rangeRequests.size());
        Assert.assertEquals(LOCAL_ADDRESS, task.rangeRequests.get(0).endpoint);
        Assert.assertEquals(REMOTE_ADDRESS, task.rangeRequests.get(1).endpoint);

        for (InstrumentedFailureRecoveryTask.InstanceStreamRequest request: task.rangeRequests)
        {
            Assert.assertEquals(CFID, request.cfId);
            Assert.assertEquals(tokenState.getRange(), request.range);
            Assert.assertArrayEquals(Scope.GLOBAL_ONLY, request.scopes);
        }
    }

    @Test
    public void recoverInstancesTestLocalScope()
    {
        EpaxosState state = new MockVerbHandlerState();
        TokenState tokenState = state.getTokenStateManager(Scope.LOCAL).get(TOKEN0, CFID);
        tokenState.setEpoch(2);
        tokenState.setState(TokenState.State.PRE_RECOVERY);

        InstrumentedFailureRecoveryTask task = new InstrumentedFailureRecoveryTask(state, TOKEN0, CFID, 0, Scope.LOCAL);

        task.recoverInstances();
        Assert.assertEquals(TokenState.State.RECOVERING_INSTANCES, tokenState.getState());

        Assert.assertNotNull(task.streamPlan);

        Assert.assertEquals(1, task.rangeRequests.size());

        InstrumentedFailureRecoveryTask.InstanceStreamRequest request = task.rangeRequests.get(0);
        Assert.assertEquals(LOCAL_ADDRESS, request.endpoint);
        Assert.assertEquals(CFID, request.cfId);
        Assert.assertEquals(tokenState.getRange(), request.range);
        Assert.assertArrayEquals(Scope.LOCAL_ONLY, request.scopes);
    }

    @Test
    public void recoverDataGlobalScope()
    {
        EpaxosState state = new MockVerbHandlerState();
        TokenState tokenState = state.getTokenStateManager(Scope.GLOBAL).get(TOKEN0, CFID);
        tokenState.setEpoch(2);
        tokenState.setState(TokenState.State.RECOVERING_INSTANCES);

        InstrumentedFailureRecoveryTask task = new InstrumentedFailureRecoveryTask(state, TOKEN0, CFID, 0, Scope.GLOBAL);

        task.recoverData();
        Assert.assertEquals(TokenState.State.RECOVERING_DATA, tokenState.getState());
        Assert.assertEquals(1, task.repairRequests.size());
        Assert.assertFalse(task.repairRequests.get(0).isLocal);
        Assert.assertEquals(tokenState.getRange(), task.repairRequests.get(0).range);
    }

    @Test
    public void recoverDataLocalcope()
    {
        EpaxosState state = new MockVerbHandlerState();
        TokenState tokenState = state.getTokenStateManager(Scope.LOCAL).get(TOKEN0, CFID);
        tokenState.setEpoch(2);
        tokenState.setState(TokenState.State.RECOVERING_INSTANCES);

        InstrumentedFailureRecoveryTask task = new InstrumentedFailureRecoveryTask(state, TOKEN0, CFID, 0, Scope.LOCAL);

        task.recoverData();
        Assert.assertEquals(TokenState.State.RECOVERING_DATA, tokenState.getState());
        Assert.assertEquals(1, task.repairRequests.size());
        Assert.assertTrue(task.repairRequests.get(0).isLocal);
        Assert.assertEquals(tokenState.getRange(), task.repairRequests.get(0).range);
    }

    @Test
    public void postRecoveryTask() throws Exception
    {
        final Set<UUID> executed = new HashSet<>();
        EpaxosState state = new MockMultiDcState() {
            @Override
            public void execute(UUID instanceId)
            {
                executed.add(instanceId);
            }
        };
        ((MockTokenStateManager) state.getTokenStateManager(Scope.GLOBAL)).setTokens(TOKEN0, token(100));

        TokenState tokenState = state.getTokenStateManager(Scope.GLOBAL).get(token(50), cfm.cfId);
        tokenState.setEpoch(2);
        tokenState.setState(TokenState.State.RECOVERING_DATA);

        UUID lastId;
        QueryInstance instance;
        Set<UUID> expected = new HashSet<>();

        // key1, nothing committed
        instance = state.createQueryInstance(getSerializedCQLRequest(10, 10));
        instance.preaccept(state.getCurrentDependencies(instance).left);
        state.saveInstance(instance);

        // key2, 2 instances committed
        instance = state.createQueryInstance(getSerializedCQLRequest(20, 10));
        instance.commit(state.getCurrentDependencies(instance).left);
        state.saveInstance(instance);

        lastId = instance.getId();
        instance = state.createQueryInstance(getSerializedCQLRequest(20, 10));
        instance.commit(state.getCurrentDependencies(instance).left);
        state.saveInstance(instance);
        expected.add(instance.getId());

        Assert.assertEquals(Sets.newHashSet(lastId), instance.getDependencies());

        // key3, 1 committed, head executed
        // executed head will prevent 1st instance from being executed
        instance = state.createQueryInstance(getSerializedCQLRequest(30, 10));
        instance.commit(state.getCurrentDependencies(instance).left);
        state.saveInstance(instance);

        lastId = instance.getId();
        instance = state.createQueryInstance(getSerializedCQLRequest(30, 10));
        instance.commit(state.getCurrentDependencies(instance).left);
        instance.setExecuted(2);
        state.saveInstance(instance);

        Assert.assertEquals(Sets.newHashSet(lastId), instance.getDependencies());

        InstrumentedFailureRecoveryTask task = new InstrumentedFailureRecoveryTask(state, TOKEN0, cfm.cfId, 0, Scope.GLOBAL) {
            @Override
            protected void runPostCompleteTask(TokenState tokenState)
            {
                new PostStreamTask(state, tokenState.getRange(), cfId, scope).run();
            }
        };

        Assert.assertEquals(TokenState.State.RECOVERING_DATA, tokenState.getState());
        task.complete();

        Assert.assertEquals(TokenState.State.NORMAL, tokenState.getState());
        Assert.assertEquals(expected, executed);
    }
}

package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.SettableFuture;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class EpaxosExecuteTaskTest extends AbstractEpaxosTest
{
    static final InetAddress LEADER = FBUtilities.getBroadcastAddress();
    static final String LEADER_DC = "DC1";

    @Before
    public void clearTables()
    {
        clearAll();
    }

    static class MockExecutionState extends EpaxosState
    {
        List<UUID> executedIds = Lists.newArrayList();

        @Override
        protected Pair<ReplayPosition, Long> executeInstance(Instance instance) throws InvalidRequestException, ReadTimeoutException, WriteTimeoutException
        {
            executedIds.add(instance.getId());
            return super.executeInstance(instance);
        }

        @Override
        protected void scheduleTokenStateMaintenanceTask()
        {
            // no-op
        }

        @Override
        protected TokenStateManager createTokenStateManager(Scope scope)
        {
            return new MockTokenStateManager(scope);
        }

        @Override
        public boolean replicates(Instance instance)
        {
            return true;
        }

        Collection<InetAddress> disseminatedEndpoints = null;
        @Override
        protected void sendToHintedEndpoints(Mutation mutation, Collection<InetAddress> endpoints)
        {
            disseminatedEndpoints = endpoints;
        }
    }

    @Test
    public void correctExecutionOrder() throws Exception
    {
        MockExecutionState state = new MockExecutionState();

        QueryInstance instance1 = new QueryInstance(getSerializedCQLRequest(0, 1), LEADER);
        instance1.commit(Collections.<UUID>emptySet());
        state.saveInstance(instance1);

        QueryInstance instance2 = new QueryInstance(getSerializedCQLRequest(0, 1), LEADER);
        instance2.commit(Sets.newHashSet(instance1.getId()));
        state.saveInstance(instance2);

        ExecuteTask task = new ExecuteTask(state, instance2.getId());
        task.run();

        Assert.assertEquals(Lists.newArrayList(instance1.getId(), instance2.getId()), state.executedIds);
    }

    /**
     * A noop instance should not be executed
     * @throws Exception
     */
    @Test
    public void skipExecution() throws Exception
    {
        MockExecutionState state = new MockExecutionState();
        QueryInstance instance = new QueryInstance(getSerializedCQLRequest(0, 1), LEADER);
        instance.commit(Collections.<UUID>emptySet());
        instance.setNoop(true);
        state.saveInstance(instance);

        ExecuteTask task = new ExecuteTask(state, instance.getId());
        task.run();

        Assert.assertEquals(0, state.executedIds.size());
    }

    /**
     * tests that execution is skipped if the keystate prevents it
     * because it has repair data from unexecuted instances
     */
    @Test
    public void keyStateCantExecute() throws Exception
    {
        MockExecutionState state = new MockExecutionState();
        QueryInstance instance = new QueryInstance(getSerializedCQLRequest(0, 1), LEADER);
        instance.commit(Collections.<UUID>emptySet());
        state.saveInstance(instance);

        TokenState ts = state.getTokenStateManager(DEFAULT_SCOPE).get(instance);
        KeyState ks = state.getKeyStateManager(DEFAULT_SCOPE).loadKeyState(instance.getQuery().getCfKey());

        Assert.assertEquals(0, ts.getEpoch());
        Assert.assertEquals(0, ks.getEpoch());

        ks.setFutureExecution(new ExecutionInfo(1, 0));
        ExecuteTask task = new ExecuteTask(state, instance.getId());
        task.run();

        ts = state.getTokenStateManager(DEFAULT_SCOPE).get(instance);
        Assert.assertEquals(0, state.executedIds.size());
    }

    private SerializedRequest adHocCqlRequest(String query, ByteBuffer key)
    {
        return newSerializedRequest(getCqlCasRequest(query, Collections.<ByteBuffer>emptyList(), ConsistencyLevel.SERIAL), key);
    }

    @Test
    public void conflictingTimestamps() throws Exception
    {
        int k = new Random(System.currentTimeMillis()).nextInt();
        MockExecutionState state = new MockExecutionState();

        SerializedRequest r3 = adHocCqlRequest("UPDATE ks.tbl SET v=2 WHERE k=" + k + " IF v=1", key(k));
        QueryInstance instance3 = state.createQueryInstance(r3);
        Thread.sleep(5);
        SerializedRequest r2 = adHocCqlRequest("UPDATE ks.tbl SET v=1 WHERE k=" + k + " IF v=0", key(k));
        QueryInstance instance2 = state.createQueryInstance(r2);
        Thread.sleep(5);
        SerializedRequest r1 = adHocCqlRequest("INSERT INTO ks.tbl (k, v) VALUES (" + k + ", 0) IF NOT EXISTS", key(k));
        QueryInstance instance1 = state.createQueryInstance(r1);


        SettableFuture future;
        instance1.commit(Sets.<UUID>newHashSet());
        state.saveInstance(instance1);
        future = state.setFuture(instance1);
        new ExecuteTask(state, instance1.getId()).run();
        Assert.assertTrue(future.isDone());
        Assert.assertNull(future.get());
        Thread.sleep(5);

        CfKey cfKey = instance1.getQuery().getCfKey();
        long firstTs = state.getKeyStateManager(DEFAULT_SCOPE).getMaxTimestamp(cfKey);
        Assert.assertNotSame(KeyState.MIN_TIMESTAMP, firstTs);

        instance2.commit(Sets.newHashSet(instance1.getId()));
        state.saveInstance(instance2);
        future = state.setFuture(instance2);
        new ExecuteTask(state, instance2.getId()).run();
        Assert.assertTrue(future.isDone());
        Assert.assertNull(future.get());
        Thread.sleep(5);

        // since the timestamp we supplied was less than the previously executed
        // one the timestamp actually executed should be lastTs + 1
        Assert.assertEquals(firstTs + 1, state.getKeyStateManager(DEFAULT_SCOPE).getMaxTimestamp(cfKey));

        instance3.commit(Sets.newHashSet(instance2.getId()));
        state.saveInstance(instance3);
        future = state.setFuture(instance3);
        new ExecuteTask(state, instance3.getId()).run();
        Assert.assertTrue(future.isDone());
        Assert.assertNull(future.get());

        Assert.assertEquals(firstTs + 2, state.getKeyStateManager(DEFAULT_SCOPE).getMaxTimestamp(cfKey));
    }

    @Test
    public void localSerialMutationDissemination() throws Exception
    {

        final List<InetAddress> local = Lists.newArrayList(InetAddress.getByName("127.0.0.2"), InetAddress.getByName("127.0.0.3"));
        final List<InetAddress> remote = Lists.newArrayList(InetAddress.getByName("126.0.0.2"), InetAddress.getByName("126.0.0.3"));

        MockExecutionState state = new MockExecutionState() {
            @Override
            protected ParticipantInfo getParticipants(Instance instance)
            {
                return new ParticipantInfo(local, remote, instance.getConsistencyLevel());
            }
        };

        QueryInstance instance = state.createQueryInstance(getSerializedCQLRequest(100, 100, ConsistencyLevel.LOCAL_SERIAL));
        EpaxosState.ParticipantInfo pi = state.getParticipants(instance);

        // sanity check
        Assert.assertEquals(local, pi.endpoints);
        Assert.assertEquals(remote, pi.remoteEndpoints);
        Assert.assertNull(state.disseminatedEndpoints);

        state.executeQueryInstance(instance);
        Assert.assertEquals(remote, state.disseminatedEndpoints);
    }

    @Test
    public void noSerialMutationDissemination() throws Exception
    {

        final List<InetAddress> local = Lists.newArrayList(InetAddress.getByName("127.0.0.2"), InetAddress.getByName("127.0.0.3"));
        final List<InetAddress> remote = Lists.newArrayList();

        MockExecutionState state = new MockExecutionState() {
            @Override
            protected ParticipantInfo getParticipants(Instance instance)
            {
                return new ParticipantInfo(local, remote, instance.getConsistencyLevel());
            }
        };

        QueryInstance instance = state.createQueryInstance(getSerializedCQLRequest(200, 200, ConsistencyLevel.SERIAL));
        EpaxosState.ParticipantInfo pi = state.getParticipants(instance);

        // sanity check
        Assert.assertEquals(local, pi.endpoints);
        Assert.assertEquals(remote, pi.remoteEndpoints);
        Assert.assertNull(state.disseminatedEndpoints);

        state.executeQueryInstance(instance);
        // should still be null
        Assert.assertNull(state.disseminatedEndpoints);
    }
}

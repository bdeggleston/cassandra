package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.SettableFuture;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
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
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class EpaxosExecuteTaskTest extends AbstractEpaxosTest
{

    @Before
    public void clearTables()
    {
        for (UntypedResultSet.Row row: QueryProcessor.executeInternal(String.format("SELECT id from %s.%s", Keyspace.SYSTEM_KS, SystemKeyspace.EPAXOS_INSTANCE)))
        {
            QueryProcessor.executeInternal(String.format("DELETE FROM %s.%s WHERE id=?", Keyspace.SYSTEM_KS, SystemKeyspace.EPAXOS_INSTANCE),
                                           row.getUUID("id"));
        }
        for (UntypedResultSet.Row row: QueryProcessor.executeInternal(String.format("SELECT row_key from %s.%s", Keyspace.SYSTEM_KS, SystemKeyspace.EPAXOS_KEY_STATE)))
        {
            QueryProcessor.executeInternal(String.format("DELETE FROM %s.%s WHERE row_key=?", Keyspace.SYSTEM_KS, SystemKeyspace.EPAXOS_KEY_STATE),
                                           row.getBlob("row_key"));
        }
        for (UntypedResultSet.Row row: QueryProcessor.executeInternal(String.format("SELECT cf_id from %s.%s", Keyspace.SYSTEM_KS, SystemKeyspace.EPAXOS_TOKEN_STATE)))
        {
            QueryProcessor.executeInternal(String.format("DELETE FROM %s.%s WHERE cf_id=?", Keyspace.SYSTEM_KS, SystemKeyspace.EPAXOS_TOKEN_STATE),
                                           row.getUUID("cf_id"));
        }
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
        protected TokenStateManager createTokenStateManager()
        {
            return new MockTokenStateManager();
        }

        @Override
        public boolean replicates(Instance instance)
        {
            return true;
        }
    }

    @Test
    public void correctExecutionOrder() throws Exception
    {
        MockExecutionState state = new MockExecutionState();

        QueryInstance instance1 = new QueryInstance(getSerializedCQLRequest(0, 1),
                                                   FBUtilities.getBroadcastAddress());
        instance1.commit(Collections.<UUID>emptySet());
        state.saveInstance(instance1);

        QueryInstance instance2 = new QueryInstance(getSerializedCQLRequest(0, 1),
                                                   FBUtilities.getBroadcastAddress());
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
        QueryInstance instance = new QueryInstance(getSerializedCQLRequest(0, 1),
                                                   FBUtilities.getBroadcastAddress());
        instance.commit(Collections.<UUID>emptySet());
        instance.setNoop(true);
        state.saveInstance(instance);

        ExecuteTask task = new ExecuteTask(state, instance.getId());
        task.run();

        Assert.assertEquals(0, state.executedIds.size());
    }

    /**
     * tests that the execution was reported
     * to the token state
     */
    @Test
    public void tokenStateReport() throws Exception
    {
        MockExecutionState state = new MockExecutionState();
        QueryInstance instance = new QueryInstance(getSerializedCQLRequest(0, 1),
                                                   FBUtilities.getBroadcastAddress());
        instance.commit(Collections.<UUID>emptySet());
        state.saveInstance(instance);

        TokenState ts = state.tokenStateManager.get(instance);
        Assert.assertEquals(0, ts.getExecutions());

        ExecuteTask task = new ExecuteTask(state, instance.getId());
        task.run();

        ts = state.tokenStateManager.get(instance);
        Assert.assertEquals(1, ts.getExecutions());
    }

    /**
     * Tests that a QueryInstance's token state
     * is notified if a SERIAL query is executed.
     */
    @Test
    public void serialQueryReport() throws Exception
    {
        MockExecutionState state = new MockExecutionState();
        QueryInstance instance = new QueryInstance(getSerializedCQLRequest(0, 1, ConsistencyLevel.SERIAL),
                                                   FBUtilities.getBroadcastAddress());
        instance.commit(Collections.<UUID>emptySet());
        state.saveInstance(instance);

        TokenState ts = state.tokenStateManager.get(instance);
        ts.recordExecution();
        Assert.assertTrue(ts.localOnly());

        ExecuteTask task = new ExecuteTask(state, instance.getId());
        task.run();

        ts = state.tokenStateManager.get(instance);
        Assert.assertFalse(ts.localOnly());
    }

    /**
     * tests that execution is skipped if the keystate prevents it
     * because it has repair data from unexecuted instances
     */
    @Test
    public void keyStateCantExecute() throws Exception
    {
        MockExecutionState state = new MockExecutionState();
        QueryInstance instance = new QueryInstance(getSerializedCQLRequest(0, 1),
                                                   FBUtilities.getBroadcastAddress());
        instance.commit(Collections.<UUID>emptySet());
        state.saveInstance(instance);

        TokenState ts = state.tokenStateManager.get(instance);
        KeyState ks = state.keyStateManager.loadKeyState(instance.getQuery().getCfKey());

        Assert.assertEquals(0, ts.getEpoch());
        Assert.assertEquals(0, ks.getEpoch());

        ks.setFutureExecution(new ExecutionInfo(1, 0));
        ExecuteTask task = new ExecuteTask(state, instance.getId());
        task.run();

        ts = state.tokenStateManager.get(instance);
        Assert.assertEquals(0, state.executedIds.size());
    }

    private SerializedRequest adHocCqlRequest(String query)
    {
        return newSerializedRequest(getCqlCasRequest(query, Collections.<ByteBuffer>emptyList(), ConsistencyLevel.SERIAL));
    }

    @Test
    public void conflictingTimestamps() throws Exception
    {
        int k = new Random(System.currentTimeMillis()).nextInt();
        MockExecutionState state = new MockExecutionState();

        SerializedRequest r3 = adHocCqlRequest("UPDATE ks.tbl SET v=2 WHERE k=" + k + " IF v=1");
        QueryInstance instance3 = state.createQueryInstance(r3);
        Thread.sleep(5);
        SerializedRequest r2 = adHocCqlRequest("UPDATE ks.tbl SET v=1 WHERE k=" + k + " IF v=0");
        QueryInstance instance2 = state.createQueryInstance(r2);
        Thread.sleep(5);
        SerializedRequest r1 = adHocCqlRequest("INSERT INTO ks.tbl (k, v) VALUES (" + k + ", 0) IF NOT EXISTS");
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
        long firstTs = state.keyStateManager.getMaxTimestamp(cfKey);
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
        Assert.assertEquals(firstTs + 1, state.keyStateManager.getMaxTimestamp(cfKey));

        instance3.commit(Sets.newHashSet(instance2.getId()));
        state.saveInstance(instance3);
        future = state.setFuture(instance3);
        new ExecuteTask(state, instance3.getId()).run();
        Assert.assertTrue(future.isDone());
        Assert.assertNull(future.get());

        Assert.assertEquals(firstTs + 2, state.keyStateManager.getMaxTimestamp(cfKey));
    }

}

package org.apache.cassandra.service.epaxos;

import com.google.common.collect.*;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.service.epaxos.integration.AbstractEpaxosIntegrationTest;
import org.apache.cassandra.service.epaxos.integration.Messenger;
import org.apache.cassandra.service.epaxos.integration.Node;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;

public class EpaxosTokenIntegrationTest extends AbstractEpaxosIntegrationTest.SingleThread
{
    private static BytesToken tokenFor(int i)
    {
        return tokenFor(ByteBufferUtil.bytes(i));
    }

    private static BytesToken tokenFor(ByteBuffer k)
    {
        return new BytesToken(k);
    }

    private static final Token TOKEN1 = tokenFor(100);
    private static final Token TOKEN2 = tokenFor(200);
    private static final UUID CFID = UUIDGen.getTimeUUID();

    static class IntegrationTokenStateManager extends TokenStateManager
    {
        IntegrationTokenStateManager(String keyspace, String table)
        {
            super(keyspace, table);
            start();
        }

        private volatile Set<Token> replicatedTokens = null;

        public void setReplicatedTokens(Set<Token> replicatedTokens)
        {
            this.replicatedTokens = replicatedTokens;
        }

        @Override
        protected Set<Token> getReplicatedTokensForCf(UUID cfId)
        {
            return replicatedTokens != null ? replicatedTokens : Collections.<Token>emptySet();
        }
    }

    public Node createNode(final int nodeNumber, final String ksName, Messenger messenger)
    {
        return new Node.SingleThreaded(nodeNumber, messenger)
        {

            @Override
            protected String keyspace()
            {
                return ksName;
            }

            @Override
            protected String instanceTable()
            {
                return String.format("%s_%s", SystemKeyspace.EPAXOS_INSTANCE, nodeNumber);
            }

            @Override
            protected String keyStateTable()
            {
                return String.format("%s_%s", SystemKeyspace.EPAXOS_KEY_STATE, nodeNumber);
            }

            @Override
            protected String tokenStateTable()
            {
                return String.format("%s_%s", SystemKeyspace.EPAXOS_TOKEN_STATE, nodeNumber);
            }

            @Override
            protected void scheduleTokenStateMaintenanceTask()
            {
                // no-op
            }

            @Override
            protected TokenStateManager createTokenStateManager()
            {
                return new IntegrationTokenStateManager(keyspace(), tokenStateTable());
            }
        };
    }

    @Test
    public void successCase()
    {
        // check baseline
        for (Node node: nodes)
        {
            IntegrationTokenStateManager tsm = (IntegrationTokenStateManager) node.tokenStateManager;
            tsm.setReplicatedTokens(Sets.newHashSet(TOKEN2));

            // token states for the currently replicated tokens should be implicitly initialized
            tsm.getOrInitManagedCf(CFID);
            Assert.assertEquals(Lists.newArrayList(TOKEN2), node.tokenStateManager.getManagedTokensForCf(CFID));
            Assert.assertEquals(0, node.getCurrentEpoch(TOKEN2, CFID));

            // add some key states
            KeyStateManager ksm = node.keyStateManager;
            for (int i=0; i<4; i++)
            {
                ByteBuffer key = ByteBufferUtil.bytes(i);
                Token token = new LongToken((long) (i * 50) + 50);
                ksm.loadKeyState(key, CFID);
            }
        }

        nodes.get(0).addToken(CFID, TOKEN1);

        // check new token exists, and epochs have been incremented
        for (Node node: nodes)
        {
            List<Token> expectedTokens = Lists.newArrayList(TOKEN1, TOKEN2);
            List<Token> actualTokens = node.tokenStateManager.allTokenStatesForCf(CFID);
            Assert.assertEquals(expectedTokens, actualTokens);

            TokenState ts1 = node.tokenStateManager.get(TOKEN1, CFID);
            Assert.assertNotNull(ts1);
            Assert.assertEquals(1, ts1.getEpoch());

            TokenState ts2 = node.tokenStateManager.get(TOKEN2, CFID);
            Assert.assertNotNull(ts2);
            Assert.assertEquals(1, ts2.getEpoch());
        }

        // check that duplicate token inserts are ignored
        nodes.get(1).addToken(CFID, TOKEN1);

        for (Node node: nodes)
        {
            List<Token> expectedTokens = Lists.newArrayList(TOKEN1, TOKEN2);
            List<Token> actualTokens = node.tokenStateManager.allTokenStatesForCf(CFID);
            Assert.assertEquals(expectedTokens, actualTokens);

            TokenState ts1 = node.tokenStateManager.get(TOKEN1, CFID);
            Assert.assertNotNull(ts1);
            Assert.assertEquals(1, ts1.getEpoch());

            TokenState ts2 = node.tokenStateManager.get(TOKEN2, CFID);
            Assert.assertNotNull(ts2);
            Assert.assertEquals(1, ts2.getEpoch());
        }
    }

    @Test
    public void executionSuccessCase() throws Exception
    {
        Node node = nodes.get(0);

        IntegrationTokenStateManager tsm = (IntegrationTokenStateManager) node.tokenStateManager;
        tsm.setReplicatedTokens(Sets.newHashSet(TOKEN2));
        tsm.getOrInitManagedCf(CFID);

        TokenState ts = tsm.getExact(TOKEN2, CFID);
        Assert.assertNotNull(ts);

        Map<Token, UUID> fakeIds = Maps.newHashMap();
        Map<Token, ByteBuffer> fakeKeys = Maps.newHashMap();

        // add some key states and pending token instances
        // this will create a key state for tokens 50, 100, 150, & 200, as well as token state
        // dependencies at 50 & 150
        KeyStateManager ksm = node.keyStateManager;
        for (int i=0; i<4; i++)
        {
            int iKey = (i * 50) + 50;
            ByteBuffer key = ByteBufferUtil.bytes(iKey);
            Token token = tokenFor(iKey);
            ksm.loadKeyState(key, CFID);
            fakeKeys.put(token, key);
            if (i%2 == 0)
            {
                UUID id = UUIDGen.getTimeUUID();
                fakeIds.put(token, id);
                ts.recordTokenInstance(token, id);
            }
        }

        TokenInstance instance = new TokenInstance(node.getEndpoint(), CFID, TOKEN1, false);
        node.getCurrentDependencies(instance);
        instance.setDependencies(Collections.<UUID>emptySet());
        instance.setState(Instance.State.COMMITTED);
        node.saveInstance(instance);
        Assert.assertTrue(ts.getCurrentTokenInstances(new Range<>(TOKEN2, TOKEN1)).contains(instance.getId()));

        new ExecuteTask(node, instance.getId()).run();

        TokenState ts2 = tsm.getExact(TOKEN1, CFID);
        Assert.assertNotNull(ts2);

        // check instance is marked executed
        Assert.assertEquals(Instance.State.EXECUTED, instance.getState());
        Assert.assertEquals(TokenState.State.NORMAL, ts2.getState());

        // check first token state
        Assert.assertEquals(1, ts.getEpoch());
        Set<UUID> deps = ts.getCurrentTokenInstances(new Range<Token>(TOKEN1, TOKEN2));
        Assert.assertEquals(1, deps.size());
        Assert.assertEquals(fakeIds.get(tokenFor(150)), deps.iterator().next());

        // check second token state
        Assert.assertEquals(1, ts2.getEpoch());
        deps = ts2.getCurrentTokenInstances(new Range<>(TOKEN2, TOKEN1));
        Assert.assertEquals(1, deps.size());
        Assert.assertEquals(fakeIds.get(tokenFor(50)), deps.iterator().next());
        Assert.assertEquals(Sets.newHashSet(instance.getId()), ts2.getCurrentEpochInstances());

        Map<Token, Long> expectedEpochs = Maps.newHashMap();
        expectedEpochs.put(TOKEN1, 1l);
        expectedEpochs.put(TOKEN2, 1l);
        // check key states
        for (Map.Entry<Token, ByteBuffer> entry: fakeKeys.entrySet())
        {
            ByteBuffer key = entry.getValue();

            KeyState ks = ksm.loadKeyState(key, CFID);
            Assert.assertEquals(1l, ks.getEpoch());
        }

        // increment the epoch for just the first token state
        EpochInstance epochInstance = node.createEpochInstance(TOKEN2, CFID, 2);
        epochInstance.setDependencies(node.getCurrentDependencies(epochInstance));
        epochInstance.setState(Instance.State.COMMITTED);
        node.saveInstance(epochInstance);
        node.executeEpochInstance(epochInstance);

        Assert.assertEquals(2, ts.getEpoch());
        Assert.assertEquals(1, ts2.getEpoch());

        // keys with tokens TOKEN1 < t <= TOKEN2 should be at epoch 2
        for (Map.Entry<Token, ByteBuffer> entry: fakeKeys.entrySet())
        {
            Token token = entry.getKey();
            ByteBuffer key = entry.getValue();

            KeyState ks = ksm.loadKeyState(key, CFID);
            long expectedEpoch = token.compareTo(TOKEN1) >= 1 ? 2 : 1;
            Assert.assertEquals(String.format("Token: " + token.toString()), expectedEpoch, ks.getEpoch());
        }
    }
}

package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.service.epaxos.integration.AbstractEpaxosIntegrationTest;
import org.apache.cassandra.service.epaxos.integration.Messenger;
import org.apache.cassandra.service.epaxos.integration.Node;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public class EpaxosTokenIntegrationTest extends AbstractEpaxosIntegrationTest.SingleThread
{
    static
    {
        DatabaseDescriptor.setPartitioner(new Murmur3Partitioner());
    }
    private static final Token TOKEN1 = new LongToken(100l);
    private static final Token TOKEN2 = new LongToken(200l);
    private static final UUID CFID = UUIDGen.getTimeUUID();

    static class IntegrationTokenStateManager extends TokenStateManager
    {
        IntegrationTokenStateManager(String keyspace, String table)
        {
            super(keyspace, table);
            start();
        }

        private volatile Token closestToken = null;

        private volatile Set<Token> replicatedTokens = null;

        public void setClosestToken(Token closestToken)
        {
            this.closestToken = closestToken;
        }

        @Override
        protected Token getClosestToken(Token token)
        {
            return closestToken;
        }

        public void setReplicatedTokens(Set<Token> replicatedTokens)
        {
            this.replicatedTokens = replicatedTokens;
        }

        @Override
        protected Set<Token> getReplicatedTokensForCf(UUID cfId)
        {
            return replicatedTokens;
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
            ((IntegrationTokenStateManager) node.tokenStateManager).setClosestToken(TOKEN2);
            ((IntegrationTokenStateManager) node.tokenStateManager).setReplicatedTokens(Sets.newHashSet(TOKEN2));
            node.tokenStateManager.getOrInitManagedCf(CFID);
            Assert.assertEquals(Lists.newArrayList(TOKEN2), node.tokenStateManager.getManagedTokensForCf(CFID));
            Assert.assertEquals(0, node.getCurrentEpoch(TOKEN2, CFID));
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
    public void executionSuccessCase()
    {
        // TODO: check instance is marked executed
        // TODO: check instance is moved from token instances to epoch instances
        // TODO: check key states for both are incremented
        // TODO: check new token state is set to 'NORMAL' state
    }
}

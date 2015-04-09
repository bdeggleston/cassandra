package org.apache.cassandra.service.epaxos;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.LongToken;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class EpaxosTokenStateManagerTest
{
    static
    {
        DatabaseDescriptor.setPartitioner(new Murmur3Partitioner());
    }

    private static final Token TOKEN100 = new LongToken(100l);
    private static final Token TOKEN200 = new LongToken(200l);
    private static final List<Token> TOKENS = ImmutableList.of(TOKEN100, TOKEN200);
    private static final Set<Token> TOKEN_SET = ImmutableSet.copyOf(TOKENS);
    private static final UUID CFID = UUIDGen.getTimeUUID();

    private static TokenStateManager getTokenStateManager()
    {
        return new TokenStateManager() {
            @Override
            protected Set<Token> getReplicatedTokensForCf(UUID cfId)
            {
                return TOKEN_SET;
            }
        };
    }

    @Test
    public void maybeInit()
    {
        TokenStateManager tsm = getTokenStateManager();

        Assert.assertFalse(tsm.managesCfId(CFID));

        TokenStateManager.ManagedCf cf = tsm.getOrInitManagedCf(CFID);
        List<Token> tokens = cf.allTokens();
        Assert.assertEquals(TOKENS, tokens);

        for (Token token: tokens)
        {
            TokenState ts = cf.get(token);
            Assert.assertEquals(token, ts.getToken());
            Assert.assertEquals(0, ts.getEpoch());
            Assert.assertEquals(0, ts.getExecutions());
        }
    }

    @Test
    public void addToken()
    {
        TokenStateManager tsm = getTokenStateManager();
        tsm.start();

        TokenState ts100 = tsm.get(TOKEN100, CFID);
        Assert.assertEquals(TOKEN100, ts100.getToken());
        ts100.setEpoch(5);

        TokenState ts200 = tsm.get(TOKEN200, CFID);
        Assert.assertEquals(TOKEN200, ts200.getToken());
        ts200.setEpoch(6);

        Token token150 = new LongToken(150l);

        Assert.assertEquals(ts200, tsm.get(token150, CFID));

        TokenState ts150 = new TokenState(token150, CFID, ts200.getEpoch(), 0);
        tsm.putState(ts150);

        Assert.assertEquals(token150, ts150.getToken());
        Assert.assertEquals(ts200.getEpoch(), ts150.getEpoch());
    }

    @Test
    public void unsavedExecutionThreshold()
    {
        final AtomicBoolean wasSaved = new AtomicBoolean(false);
        TokenStateManager tsm = new TokenStateManager() {
            @Override
            protected Set<Token> getReplicatedTokensForCf(UUID cfId)
            {
                return TOKEN_SET;
            }

            @Override
            public void save(TokenState state)
            {
                wasSaved.set(true);
                super.save(state);
            }
        };
    }

    /**
     * When the token state manager starts up, if it encounters any token states
     * in a non-normal state, it means that C* was shut down while they were in
     * the process of recovering. In this case, the tsm should set their state to
     * recovery required.
     */
    @Test
    public void nonNormalStartupState()
    {
        // create token state and save it in with a non-normal state
        TokenStateManager tsm = getTokenStateManager();
        tsm.start();

        TokenState ts = tsm.get(TOKEN100, CFID);
        ts.setState(TokenState.State.PRE_RECOVERY);
        tsm.save(ts);

        // start another, and check that it changed the state on startup
        tsm = getTokenStateManager();
        tsm.start();

        ts = tsm.get(TOKEN100, CFID);
        Assert.assertEquals(TokenState.State.RECOVERY_REQUIRED, ts.getState());
    }
}

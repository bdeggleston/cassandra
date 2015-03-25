package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.util.Set;
import java.util.UUID;


public class MockTokenStateManager extends TokenStateManager
{
    public static final Token TOKEN = DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(0));

    private volatile Token token = TOKEN;

    private volatile boolean persist = true;

    public MockTokenStateManager()
    {
        start();
    }

    public MockTokenStateManager(String keyspace, String table)
    {
        super(keyspace, table);
        start();
    }

    public void dontPersist()
    {
        persist = false;
    }

    public void setToken(Token token)
    {
        this.token = token;
    }

    @Override
    public void save(TokenState state)
    {
        if (persist)
        {
            super.save(state);
        }
    }

    @Override
    protected Set<Token> getReplicatedTokensForCf(UUID cfId)
    {
        return Sets.newHashSet(token);
    }

    public int epochIncrementThreshold = 100;

    @Override
    public int getEpochIncrementThreshold(UUID cfId)
    {
        return epochIncrementThreshold;
    }
}

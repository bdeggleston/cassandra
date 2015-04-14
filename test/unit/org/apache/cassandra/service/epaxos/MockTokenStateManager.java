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

    private volatile Set<Token> tokens = Sets.newHashSet(TOKEN);

    public MockTokenStateManager()
    {
        start();
    }

    public MockTokenStateManager(String keyspace, String table)
    {
        super(keyspace, table);
        start();
    }

    public void setToken(Token token)
    {
        tokens = Sets.newHashSet(token);
    }

    public void setTokens(Token... tokens)
    {
        this.tokens = Sets.newHashSet(tokens);
    }

    public void addToken(Token token)
    {
        tokens.add(token);
    }

    @Override
    protected Set<Token> getReplicatedTokensForCf(UUID cfId)
    {
        return Sets.newHashSet(tokens);
    }

    public int epochIncrementThreshold = 100;

    @Override
    public int getEpochIncrementThreshold(UUID cfId)
    {
        return epochIncrementThreshold;
    }
}

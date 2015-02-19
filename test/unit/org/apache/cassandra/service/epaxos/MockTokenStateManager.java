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
        this.token = token;
    }

    @Override
    protected Token getClosestToken(Token token)
    {
        return token;
    }

    @Override
    protected Set<Token> getReplicatedTokensForCf(UUID cfId)
    {
        return Sets.newHashSet(token);
    }


}

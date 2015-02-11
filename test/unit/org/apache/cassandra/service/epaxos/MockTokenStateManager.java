package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.util.Set;
import java.util.UUID;


public class MockTokenStateManager extends TokenStateManager
{
    public static final Token TOKEN = DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(1234));

    public MockTokenStateManager()
    {
        start();
    }

    public MockTokenStateManager(String keyspace, String table)
    {
        super(keyspace, table);
        start();
    }

    @Override
    protected Token getClosestToken(Token token)
    {
        return TOKEN;
    }

    @Override
    protected Set<Token> getReplicatedTokensForCf(UUID cfId)
    {
        return Sets.newHashSet(TOKEN);
    }


}

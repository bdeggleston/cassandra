package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;

public class MockTokenStateManager extends TokenStateManager
{
    public static final Token TOKEN = DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(1234));

    public MockTokenStateManager()
    {

    }

    public MockTokenStateManager(String keyspace, String table)
    {
        super(keyspace, table);
    }

    @Override
    protected Token getClosestToken(Token token)
    {
        return TOKEN;
    }
}

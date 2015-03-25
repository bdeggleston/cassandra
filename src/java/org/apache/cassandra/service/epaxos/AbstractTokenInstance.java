package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Token;

import java.net.InetAddress;
import java.util.UUID;

/**
 * Base instance used for managing token states
 */
public abstract class AbstractTokenInstance extends Instance
{
    protected final UUID cfId;
    protected final Token token;
    protected final boolean local;

    protected AbstractTokenInstance(InetAddress leader, UUID cfId, Token token, boolean local)
    {
        super(leader);
        this.cfId = cfId;
        this.token = token;
        this.local = local;
    }

    protected AbstractTokenInstance(UUID id, InetAddress leader, UUID cfId, Token token, boolean local)
    {
        super(id, leader);
        this.cfId = cfId;
        this.token = token;
        this.local = local;
    }

    public AbstractTokenInstance(AbstractTokenInstance i)
    {
        super(i);
        this.cfId = i.cfId;
        this.token = i.token;
        this.local = i.local;
    }

    @Override
    public Token getToken()
    {
        return token;
    }

    @Override
    public UUID getCfId()
    {
        return cfId;
    }

    @Override
    public ConsistencyLevel getConsistencyLevel()
    {
        return local ? ConsistencyLevel.LOCAL_SERIAL : ConsistencyLevel.SERIAL;
    }
}

package org.apache.cassandra.service.epaxos;

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

    public AbstractTokenInstance(InetAddress leader, UUID cfId, Token token)
    {
        super(leader);
        this.cfId = cfId;
        this.token = token;
    }

    public AbstractTokenInstance(UUID id, InetAddress leader, UUID cfId, Token token)
    {
        super(id, leader);
        this.cfId = cfId;
        this.token = token;
    }

    public AbstractTokenInstance(AbstractTokenInstance i)
    {
        super(i);
        this.cfId = i.cfId;
        this.token = i.token;
    }

    @Override
    protected String toStringExtra()
    {
        return ", token=" + token;
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
}

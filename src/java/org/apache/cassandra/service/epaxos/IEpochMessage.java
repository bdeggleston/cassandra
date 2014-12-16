package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.dht.Token;

public interface IEpochMessage
{
    public Token getToken();
    public long getEpoch();
}

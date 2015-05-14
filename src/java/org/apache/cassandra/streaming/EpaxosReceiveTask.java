package org.apache.cassandra.streaming;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.epaxos.EpaxosState;
import org.apache.cassandra.service.epaxos.Scope;

import java.util.UUID;

public class EpaxosReceiveTask extends EpaxosTask
{
    public EpaxosReceiveTask(StreamSession session, UUID taskId, UUID cfId, Range<Token> range, Scope scope)
    {
        this(session, taskId, EpaxosState.getInstance(), cfId, range, scope);
    }

    public EpaxosReceiveTask(StreamSession session, UUID taskId, EpaxosState state, UUID cfId, Range<Token> range, Scope scope)
    {
        super(session, taskId, state, cfId, range, scope);
    }
}

package org.apache.cassandra.streaming;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.epaxos.EpaxosState;

import java.util.UUID;

public class EpaxosReceiveTask extends EpaxosTask
{
    public EpaxosReceiveTask(StreamSession session, UUID taskId, UUID cfId, Range<Token> range)
    {
        this(session, taskId, EpaxosState.instance, cfId, range);
    }

    public EpaxosReceiveTask(StreamSession session, UUID taskId, EpaxosState state, UUID cfId, Range<Token> range)
    {
        super(session, taskId, state, cfId, range);
    }
}

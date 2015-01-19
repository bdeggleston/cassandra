package org.apache.cassandra.streaming;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.epaxos.EpaxosState;

import java.util.UUID;

public class EpaxosTask
{
    public final StreamSession session;
    public final UUID taskId;
    public final EpaxosState state;
    public final UUID cfId;
    public final Range<Token> range;

    public EpaxosTask(StreamSession session, UUID taskId, EpaxosState state, UUID cfId, Range<Token> range)
    {
        this.session = session;
        this.taskId = taskId;
        this.state = state;
        this.cfId = cfId;
        this.range = range;
    }
}

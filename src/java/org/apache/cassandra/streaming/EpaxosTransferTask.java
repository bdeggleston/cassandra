package org.apache.cassandra.streaming;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.epaxos.EpaxosState;
import org.apache.cassandra.streaming.messages.EpaxosMessage;

import java.util.UUID;

public class EpaxosTransferTask extends EpaxosTask
{
    public EpaxosTransferTask(StreamSession session, UUID taskId, UUID cfId, Range<Token> range)
    {
        this(session, taskId, EpaxosState.getInstance(), cfId, range);
    }

    public EpaxosTransferTask(StreamSession session, UUID taskId, EpaxosState state, UUID cfId, Range<Token> range)
    {
        super(session, taskId, state, cfId, range);
    }

    public EpaxosSummary getSummary()
    {
        return new EpaxosSummary(taskId, cfId, range);
    }

    public EpaxosMessage getMessage()
    {
        return new EpaxosMessage(taskId, cfId, range);
    }
}

package org.apache.cassandra.service.epaxos;

import java.util.List;
import java.util.UUID;

public class TryPreacceptTask implements Runnable
{
    private final EpaxosState state;
    private final UUID id;
    private final TryPreacceptAttempt attempt;
    private final List<TryPreacceptAttempt> nextAttempts;

    public TryPreacceptTask(EpaxosState state, UUID id, TryPreacceptAttempt attempt, List<TryPreacceptAttempt> nextAttempts)
    {
        this.state = state;
        this.id = id;
        this.attempt = attempt;
        this.nextAttempts = nextAttempts;
    }

    @Override
    public void run()
    {

    }
}

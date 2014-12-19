package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.dht.Token;

public class FailureRecoveryTask implements Runnable
{

    private final EpaxosState state;
    private final Token token;
    private final long epoch;

    public FailureRecoveryTask(EpaxosState state, Token token, long epoch)
    {
        this.state = state;
        this.token = token;
        this.epoch = epoch;
    }

    @Override
    public void run()
    {
        // TODO: set token state status to recovering
        // TODO: erase data for all keys owned by current epochs
        // TODO: stream in raw data for affected partition range.
        // META-TODO: maybe dovetail with incremental repair for this
        // TODO: get all instances executed in epoch -1, execute them in the order they were executed in remotely
        // we trust the remote ordering because the instances may rely on GC'd instances, so building a dependency graph will find dangling pointers
        // TODO: get all instances executed in current epoch, execute via dependency graph
        // TODO: set token state status to normal
    }
}

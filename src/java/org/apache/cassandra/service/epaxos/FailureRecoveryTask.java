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
        // TODO: stop actively participating in any epaxos queries
        // TODO: still receive accepts and commits, no execution though
        // TODO: erase data for all keys owned by current epochs
        // TODO: stream in instances for the affected token range from other nodes
        // TODO: begin participating in epaxos instances
        // TODO: stream in raw data for affected partition range.
        // TODO: get all instances executed in epoch -1, execute them in the order they were executed in remotely
        // we trust the remote ordering because the instances may rely on GC'd instances, so building a dependency graph will find dangling pointers
        // META-TODO: maybe dovetail with incremental repair for this
        // TODO: get all instances executed in current epoch, execute via dependency graph
        // TODO: set token state status to normal
    }
}

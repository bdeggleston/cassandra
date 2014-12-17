package org.apache.cassandra.service.epaxos;

public class FailureRecoveryTask implements Runnable
{
    @Override
    public void run()
    {
        // TODO: set token state status to recovering
        // TODO: erase out of date instance data
        // TODO: stream in raw data for affected partition range.
        // META-TODO: maybe dovetail with incremental repair for this
        // TODO: get all instances executed in epoch -1, execute them in the order they were executed in remotely
        // we trust the remote ordering because the instances may rely on GC'd instances, so the usual algorithm wouldn't work
        // TODO: get all instances executed in current epoch, execute via dependency graph
        // TODO: set token state status to normal
    }
}

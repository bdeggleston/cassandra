package org.apache.cassandra.service.epaxos;

import org.junit.Test;

public class EpaxosFailureRecoveryTest
{
    @Test
    public void preRecover() throws Exception
    {
        // TODO: set token state status to recovering
        // TODO: stop participating in any epaxos instances
        // TODO: erase data for all keys owned by current epochs
    }
}

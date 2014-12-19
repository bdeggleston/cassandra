package org.apache.cassandra.service.epaxos;

import org.junit.Test;

public class EpaxosPrepareTaskTest
{
    @Test
    public void normalCase() throws Exception
    {
        // TODO: no wait time
        // TODO: not committed
        // TODO: ballot incremented
        // TODO: messages sent to replicas
    }

    /**
     * Tests that waiting prepare phase will abort if another
     * thread commits it first
     */
    @Test
    public void commitNotification() throws Exception
    {
        // TODO: group notified of commit
    }

    @Test
    public void prepareIsDelayed() throws Exception
    {

    }

    @Test
    public void deferredPrepareAbortsOnCommit() throws Exception
    {

    }

    @Test
    public void missingInstanceIsFetched() throws Exception
    {

    }
}

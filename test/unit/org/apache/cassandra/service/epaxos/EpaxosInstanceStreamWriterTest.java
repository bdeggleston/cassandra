package org.apache.cassandra.service.epaxos;

import org.junit.Test;

public class EpaxosInstanceStreamWriterTest
{
    /**
     * Tests that keys from other column families aren't transmitted
     */
    @Test
    public void nonTargetCfIdKeysAreSkipped() throws Exception
    {

    }

    /**
     * Test that instances are transmitted in order of epoch, then active
     */
    @Test
    public void instanceOrdering() throws Exception
    {

    }

    /**
     * Instances from epochs older than current - 1 should be skipped
     */
    @Test
    public void oldEpochsAreSkipped() throws Exception
    {

    }
}

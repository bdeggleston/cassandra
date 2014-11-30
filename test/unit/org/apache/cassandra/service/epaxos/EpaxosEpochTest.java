package org.apache.cassandra.service.epaxos;

import org.junit.Test;

public class EpaxosEpochTest
{
    /**
     * When an instance is the last/only instance executed against a partition,
     * it should be GC'd after 2 epochs pass.
     * @throws Exception
     */
    @Test
    public void minimalActivityGC() throws Exception
    {

    }

    /**
     * When a token's epoch is incremented, the epochs for all
     * of it's dependency managers should be incremented as well
     */
    @Test
    public void depsManagerEpochIsIncremented() throws Exception
    {

    }

    @Test
    public void epochInstanceIncludedInQueryDeps() throws Exception
    {

    }

}

package org.apache.cassandra.service.epaxos;

import org.junit.Test;

public class EpaxosTryPreacceptCallbackTest
{
    // TODO: all of this

    /**
     * tests that a successful try preaccept
     * round causes an accept phase
     */
    @Test
    public void acceptCase()
    {

    }

    /**
     * tests that an unsuccessful try preaccept
     * round moves onto the next attempt, if one exists
     */
    @Test
    public void nextAttemptCase()
    {

    }

    /**
     * tests that an unsuccessful try preaccept  round falls back to a normal
     * prepare preaccept phase, if there are no other attempts to be made
     */
    @Test
    public void regularPreacceptCase()
    {

    }

    @Test
    public void epochVeto()
    {

    }
}

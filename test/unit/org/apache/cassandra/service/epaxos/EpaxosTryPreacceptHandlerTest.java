package org.apache.cassandra.service.epaxos;

import org.junit.Assert;
import org.junit.Test;

public class EpaxosTryPreacceptHandlerTest extends AbstractEpaxosTest
{
    // TODO: all of this

    @Test
    public void gettingCurrentDepsDoesntMutateKeyState()
    {

    }

    @Test
    public void successCase()
    {

    }

    @Test
    public void conflictedCase()
    {

    }

    @Test
    public void contendedCase()
    {

    }

    @Test
    public void ballotFailure()
    {

    }

    @Test
    public void epochVeto()
    {

    }

    @Test
    public void passiveRecord()
    {
        MockVerbHandlerState state = new MockVerbHandlerState();
        TryPreacceptVerbHandler handler = new TryPreacceptVerbHandler(state);
        Assert.assertFalse(handler.canPassiveRecord());
    }

    @Test
    public void placeholderInstancesAreIgnored() throws Exception
    {

    }
}

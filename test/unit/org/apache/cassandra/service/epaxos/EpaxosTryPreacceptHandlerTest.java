package org.apache.cassandra.service.epaxos;

import org.junit.Assert;
import org.junit.Test;

public class EpaxosTryPreacceptHandlerTest extends AbstractEpaxosTest
{
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
}

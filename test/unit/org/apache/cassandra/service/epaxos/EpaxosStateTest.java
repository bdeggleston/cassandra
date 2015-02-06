package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;

public class EpaxosStateTest extends AbstractEpaxosTest
{
    @Test
    public void getParticipantsRemoteDCExcludedFromLocalSerial()
    {

    }

    @Test
    public void getParticipantsRemoteDCInSerial()
    {

    }

    /**
     * test that noop instances are recorded
     */
    @Test
    public void checkNoopMessage()
    {

    }

    @Test
    public void deleteInstance() throws Exception
    {
        // TODO: check table delete
        // TODO: check cache removal
    }

    /**
     * tests that, when we receive missing instances from other nodes
     * that have the executed status, they are saved with the committed
     * status
     */
    @Test
    public void executedMissingInstances() throws Exception
    {
        EpaxosState state = new EpaxosState() {
            protected TokenStateManager createTokenStateManager()
            {
                return new MockTokenStateManager();
            }
        };
        QueryInstance extInstance = new QueryInstance(getSerializedCQLRequest(0, 1), InetAddress.getByAddress(new byte[] {127, 0, 0, 127}));
        extInstance.setExecuted(0);
        extInstance.setSuccessors(Lists.newArrayList(InetAddress.getLocalHost()));
        extInstance.setDependencies(Sets.newHashSet(UUIDGen.getTimeUUID()));

        Assert.assertEquals(Instance.State.EXECUTED, extInstance.getState());
        state.addMissingInstance(extInstance);

        Instance localInstance = state.getInstanceCopy(extInstance.getId());
        Assert.assertNotNull(localInstance);
        Assert.assertEquals(Instance.State.COMMITTED, localInstance.getState());
    }
}

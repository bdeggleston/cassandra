package org.apache.cassandra.service.epaxos;

import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.utils.UUIDGen;

public class EpaxosTryPreacceptTest
{
    /**
     * Tests that we jump right to the accept phase if an attempt doesn't require
     * any replicas be convinced, and they all agree with the leader
     */
    @Test
    public void zeroRequireConvinced() throws Exception
    {
        final AtomicReference<UUID> acceptedId = new AtomicReference<>(null);
        final AtomicReference<AcceptDecision> acceptedDecision = new AtomicReference<>(null);
        MockMessengerState state = new MockMessengerState(3, 0) {
            @Override
            public void accept(UUID iid, AcceptDecision decision, Runnable failureCallback)
            {
                acceptedId.set(iid);
                acceptedDecision.set(decision);
            }
        };

        TryPreacceptAttempt attempt = new TryPreacceptAttempt(Sets.newHashSet(UUIDGen.getTimeUUID()),
                                                              Sets.<InetAddress>newHashSet(),
                                                              0,
                                                              Sets.newHashSet(InetAddress.getAllByName("127.0.0.1")),
                                                              true,
                                                              true);

        Assert.assertNull(acceptedId.get());
        Assert.assertNull(acceptedDecision.get());

        UUID id = UUIDGen.getTimeUUID();
        state.tryPreaccept(id, Lists.newArrayList(attempt), null, null);

        Assert.assertEquals(id, acceptedId.get());
        AcceptDecision acceptDecision = acceptedDecision.get();
        Assert.assertNotNull(acceptDecision);
        Assert.assertEquals(attempt.dependencies, acceptDecision.acceptDeps);
        Assert.assertEquals(attempt.vetoed, acceptDecision.vetoed);
    }

    // TODO: below

    @Test
    public void nextAttemptsArePassedAlong()
    {

    }

    @Test
    public void messagesSentToProperReplicas()
    {

    }
}

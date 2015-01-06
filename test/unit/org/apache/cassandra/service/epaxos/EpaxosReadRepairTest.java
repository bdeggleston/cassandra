package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadRepairVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class EpaxosReadRepairTest extends AbstractEpaxosTest
{
    /**
     * Tests that a read repair that doesn't interfere
     * with  a future instance is allowed through
     */
    @Test
    public void allowedRepair()
    {
        final UUID cfId = UUIDGen.getTimeUUID();
        ByteBuffer key = ByteBufferUtil.bytes(1234);

        EpaxosState state = new MockVerbHandlerState();

        final AtomicBoolean repaired = new AtomicBoolean(false);
        ReadRepairVerbHandler.Epaxos verbHandler = new ReadRepairVerbHandler.Epaxos(state) {
            @Override
            protected void doRepair(Mutation mutation, int id, InetAddress from)
            {
                repaired.set(true);
            }
        };

        KeyState keyState = state.keyStateManager.loadKeyState(key, cfId);
        keyState.setEpoch(5);
        keyState.setExecutionCount(3);
        state.keyStateManager.saveKeyState(key, cfId, keyState);

        Mutation mutation = new Mutation("como estas", key) {
            @Override
            public Collection<UUID> getColumnFamilyIds()
            {
                return Lists.newArrayList(cfId);
            }
        };


        // check epoch validation
        repaired.set(false);
        ExecutionInfo.Tuple<Mutation> okEpoch = new ExecutionInfo.Tuple<>(new ExecutionInfo(4, 0), mutation);
        verbHandler.doVerb(MessageIn.create(null, okEpoch, null, MessagingService.Verb.EPAXOS_READ_REPAIR, 0), 0);
        Assert.assertTrue(repaired.get());

        // check execution validation
        repaired.set(false);
        ExecutionInfo.Tuple<Mutation> okExecution = new ExecutionInfo.Tuple<>(new ExecutionInfo(5, 3), mutation);
        verbHandler.doVerb(MessageIn.create(null, okExecution, null, MessagingService.Verb.EPAXOS_READ_REPAIR, 0), 0);
        Assert.assertTrue(repaired.get());
    }

    /**
     * Tests that a read repair that interferes with
     * a future instance is filtered out.
     */
    @Test
    public void restrictedRepair()
    {
        final UUID cfId = UUIDGen.getTimeUUID();
        ByteBuffer key = ByteBufferUtil.bytes(1234);

        EpaxosState state = new MockVerbHandlerState();

        final AtomicBoolean repaired = new AtomicBoolean(false);
        final AtomicBoolean responseSent = new AtomicBoolean(false);

        ReadRepairVerbHandler.Epaxos verbHandler = new ReadRepairVerbHandler.Epaxos(state) {
            @Override
            protected void doRepair(Mutation mutation, int id, InetAddress from)
            {
                repaired.set(true);
            }

            @Override
            protected void sendResponse(int id, InetAddress from)
            {
                responseSent.set(true);
            }
        };

        KeyState keyState = state.keyStateManager.loadKeyState(key, cfId);
        keyState.setEpoch(5);
        keyState.setExecutionCount(3);
        state.keyStateManager.saveKeyState(key, cfId, keyState);

        Mutation mutation = new Mutation("muy bien", key) {
            @Override
            public Collection<UUID> getColumnFamilyIds()
            {
                return Lists.newArrayList(cfId);
            }
        };


        // check epoch validation
        repaired.set(false);
        responseSent.set(false);
        ExecutionInfo.Tuple<Mutation> okEpoch = new ExecutionInfo.Tuple<>(new ExecutionInfo(6, 0), mutation);
        verbHandler.doVerb(MessageIn.create(null, okEpoch, null, MessagingService.Verb.EPAXOS_READ_REPAIR, 0), 0);
        Assert.assertFalse(repaired.get());
        Assert.assertTrue(responseSent.get());

        // check execution validation
        repaired.set(false);
        responseSent.set(false);
        ExecutionInfo.Tuple<Mutation> okExecution = new ExecutionInfo.Tuple<>(new ExecutionInfo(5, 4), mutation);
        verbHandler.doVerb(MessageIn.create(null, okExecution, null, MessagingService.Verb.EPAXOS_READ_REPAIR, 0), 0);
        Assert.assertFalse(repaired.get());
        Assert.assertTrue(responseSent.get());
    }
}

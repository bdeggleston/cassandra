package org.apache.cassandra.service.epaxos;

import com.google.common.cache.Cache;
import com.google.common.collect.Sets;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public class EpaxosStateTest extends AbstractEpaxosTest
{
    @Test
    public void deleteInstance() throws Exception
    {
        clearInstances();
        clearKeyStates();
        clearTokenStates();
        final AtomicReference<Cache<UUID, Instance>> cacheRef = new AtomicReference<>();

        EpaxosState state = new EpaxosState() {
            @Override
            protected void scheduleTokenStateMaintenanceTask()
            {
                // no-op
            }
        };

        QueryInstance instance = state.createQueryInstance(getSerializedCQLRequest(0, 1));
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));
        state.saveInstance(instance);
        UUID id = instance.getId();

        Assert.assertTrue(state.cacheContains(instance.getId()));
        Assert.assertNotNull(state.loadInstance(id));

        state.clearInstanceCache();
        Assert.assertFalse(state.cacheContains(instance.getId()));
        Assert.assertNotNull(state.loadInstance(id));
        Assert.assertTrue(state.cacheContains(instance.getId()));

        state.deleteInstance(id);
        Assert.assertFalse(state.cacheContains(instance.getId()));
        Assert.assertNull(state.loadInstance(id));
    }

    /**
     * tests that, when we receive missing instances from other nodes
     * that have the executed status, they are saved with the committed
     * status, then submitted for execution
     */
    @Test
    public void executedMissingInstances() throws Exception
    {
        final AtomicReference<UUID> executed = new AtomicReference<>();
        final Set<UUID> commitNotifications = new HashSet<>();
        EpaxosState state = new EpaxosState() {
            @Override
            protected TokenStateManager createTokenStateManager()
            {
                return new MockTokenStateManager();
            }

            @Override
            public void execute(UUID instanceId)
            {
                executed.set(instanceId);
            }

            @Override
            public void notifyCommit(UUID id)
            {
                commitNotifications.add(id);
            }

            @Override
            protected void scheduleTokenStateMaintenanceTask()
            {
                // no-op
            }
        };
        QueryInstance extInstance = new QueryInstance(getSerializedCQLRequest(0, 1), InetAddress.getByAddress(new byte[] {127, 0, 0, 127}));
        extInstance.setExecuted(0);
        extInstance.setDependencies(Sets.newHashSet(UUIDGen.getTimeUUID()));

        Assert.assertEquals(Instance.State.EXECUTED, extInstance.getState());
        Assert.assertNull(executed.get());
        Assert.assertTrue(commitNotifications.isEmpty());
        state.addMissingInstance(extInstance);

        Instance localInstance = state.getInstanceCopy(extInstance.getId());
        Assert.assertNotNull(localInstance);
        Assert.assertEquals(Instance.State.COMMITTED, localInstance.getState());
        Assert.assertEquals(localInstance.getId(), executed.get());
        Assert.assertEquals(Sets.newHashSet(extInstance.getId()), commitNotifications);
    }
}

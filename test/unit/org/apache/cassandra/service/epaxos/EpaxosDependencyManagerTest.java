package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;
import java.util.UUID;

public class EpaxosDependencyManagerTest
{

    /**
     * Tests that dependencies are evicted once they've
     * been executed and acknowledged
     */
    @Test
    public void eviction() throws Exception
    {
        DependencyManager dm = new DependencyManager();
        UUID dep1 = UUIDGen.getTimeUUID();
        UUID dep2 = UUIDGen.getTimeUUID();
        UUID dep3 = UUIDGen.getTimeUUID();

        dm.getOrCreate(dep1).addedExplicitly = true;
        dm.getOrCreate(dep2).addedExplicitly = true;
        dm.getOrCreate(dep3).addedExplicitly = true;

        // all 3 should be in manager
        Set<UUID> expected = Sets.newHashSet(dep1, dep2, dep3);
        Assert.assertEquals(expected, dm.getDeps());

        // still expecting all 3
        dm.markAcknowledged(Sets.newHashSet(dep1));
        dm.markExecuted(dep2);
        Assert.assertEquals(expected, dm.getDeps());

        // executing dep1 should remove it
        dm.markExecuted(dep1);
        expected.remove(dep1);
        Assert.assertEquals(expected, dm.getDeps());

        // acknowledging dep2 should remove it
        dm.markAcknowledged(Sets.newHashSet(dep2));
        expected.remove(dep2);
        Assert.assertEquals(expected, dm.getDeps());
    }

    @Test
    public void depsExcludeImplicitEntries() throws Exception
    {
        DependencyManager dm = new DependencyManager();
        DependencyManager.Entry entry = dm.getOrCreate(UUIDGen.getTimeUUID());
        Assert.assertFalse(entry.addedExplicitly);
        Assert.assertEquals(0, dm.getDeps().size());

    }
}

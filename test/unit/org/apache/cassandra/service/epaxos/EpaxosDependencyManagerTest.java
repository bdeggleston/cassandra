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

        UUID dep0 = UUIDGen.getTimeUUID();
        Set<UUID> expected = Sets.newHashSet();
        Assert.assertEquals(expected, dm.getDepsAndAdd(dep0));

        expected.add(dep0);
        UUID dep1 = UUIDGen.getTimeUUID();
        Assert.assertEquals(expected, dm.getDepsAndAdd(dep1));

        UUID dep2 = UUIDGen.getTimeUUID();
        dm.markAcknowledged(dep0);
        Assert.assertTrue(dm.get(dep0).acknowledged);
        expected.add(dep1);
        Assert.assertEquals(expected, dm.getDepsAndAdd(dep2));

        UUID dep3 = UUIDGen.getTimeUUID();
        dm.markExecuted(dep0);
        Assert.assertTrue(dm.get(dep0).executed);
        expected.add(dep2);
        Assert.assertEquals(expected, dm.getDepsAndAdd(dep3));

        UUID dep4 = UUIDGen.getTimeUUID();
        expected.remove(dep0);
        expected.add(dep3);
        Assert.assertEquals(expected, dm.getDepsAndAdd(dep4));

    }

    /**
     * Tests instances aren't assigned themselves as dependencies
     */
    @Test
    public void selfDependencies() throws Exception
    {
        DependencyManager dm = new DependencyManager();

        UUID dep0 = UUIDGen.getTimeUUID();
        Set<UUID> expected = Sets.newHashSet();
        Assert.assertEquals(expected, dm.getDepsAndAdd(dep0));

        expected.add(dep0);
        UUID dep1 = UUIDGen.getTimeUUID();
        Assert.assertEquals(expected, dm.getDepsAndAdd(dep1));

        expected.add(dep1);
        expected.remove(dep0);
        Assert.assertEquals(expected, dm.getDepsAndAdd(dep0));
    }
}

package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
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
        DependencyManager dm = new DependencyManager(0);

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
        DependencyManager dm = new DependencyManager(0);

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

    @Test
    public void executionCountIncrementedOnMarkExecuted() throws Exception
    {
        DependencyManager dm = new DependencyManager(0);

        UUID dep = UUIDGen.getTimeUUID();
        dm.getDepsAndAdd(dep);

        Assert.assertEquals(0, dm.getEpoch());
        Assert.assertEquals(0, dm.getExecutionCount());

        dm.markExecuted(dep);
        Assert.assertEquals(1, dm.getExecutionCount());
    }

    @Test
    public void executionCountResetOnEpochChange() throws Exception
    {
        DependencyManager dm = new DependencyManager(0);

        UUID dep = UUIDGen.getTimeUUID();

        dm.getDepsAndAdd(dep);

        Assert.assertEquals(0, dm.getEpoch());
        Assert.assertEquals(0, dm.getExecutionCount());

        dm.markExecuted(dep);
        Assert.assertEquals(1, dm.getExecutionCount());

        dm.setEpoch(1);
        Assert.assertEquals(0, dm.getExecutionCount());
    }

    @Test(expected=RuntimeException.class)
    public void epochDecrementFailure() throws Exception
    {
        DependencyManager dm = new DependencyManager(1);
        Assert.assertEquals(1, dm.getEpoch());
        dm.setEpoch(0);
    }

    @Test
    public void executedInstanceBucketedByEpoch() throws Exception
    {
        DependencyManager dm = new DependencyManager(0);

        Assert.assertEquals(0, dm.getEpoch());

        UUID dep1 = UUIDGen.getTimeUUID();
        UUID dep2 = UUIDGen.getTimeUUID();
        UUID dep3 = UUIDGen.getTimeUUID();

        dm.setEpoch(1);
        dm.markExecuted(dep1);

        dm.setEpoch(2);
        dm.markExecuted(dep2);
        dm.markExecuted(dep3);

        Map<Long, Set<UUID>> executions = dm.getEpochExecutions();
        Assert.assertNull(executions.get((long)0));
        Assert.assertEquals(Sets.newHashSet(dep1), executions.get((long)1));
        Assert.assertEquals(Sets.newHashSet(dep2, dep3), executions.get((long)2));
    }

    @Test
    public void epochSetIsIdempotent() throws Exception
    {
        DependencyManager dm = new DependencyManager(1, 2);

        Assert.assertEquals(2, dm.getExecutionCount());

        UUID dep = UUIDGen.getTimeUUID();
        dm.markExecuted(dep);
        Assert.assertEquals(3, dm.getExecutionCount());
        dm.markExecuted(dep);
        Assert.assertEquals(3, dm.getExecutionCount());
    }

    @Test
    public void markExecutedIdempotent() throws Exception
    {
        DependencyManager dm = new DependencyManager(0);

        UUID dep = UUIDGen.getTimeUUID();
        dm.getDepsAndAdd(dep);

        Assert.assertEquals(0, dm.getEpoch());
        Assert.assertEquals(0, dm.getExecutionCount());

        dm.markExecuted(dep);
        Assert.assertEquals(1, dm.getExecutionCount());
    }
}

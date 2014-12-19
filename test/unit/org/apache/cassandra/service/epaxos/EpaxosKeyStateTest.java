package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class EpaxosKeyStateTest
{

    /**
     * Tests that dependencies are evicted once they've
     * been executed and acknowledged
     */
    @Test
    public void eviction() throws Exception
    {
        KeyState dm = new KeyState(0);

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
        KeyState dm = new KeyState(0);

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
        KeyState dm = new KeyState(0);

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
        KeyState dm = new KeyState(0);

        UUID dep = UUIDGen.getTimeUUID();

        dm.getDepsAndAdd(dep);

        Assert.assertEquals(0, dm.getEpoch());
        Assert.assertEquals(0, dm.getExecutionCount());

        dm.markExecuted(dep);
        Assert.assertEquals(1, dm.getExecutionCount());

        dm.setEpoch(1);
        Assert.assertEquals(0, dm.getExecutionCount());
    }

    @Test(expected=IllegalArgumentException.class)
    public void epochDecrementFailure() throws Exception
    {
        KeyState dm = new KeyState(1);
        Assert.assertEquals(1, dm.getEpoch());
        dm.setEpoch(0);
    }

    @Test
    public void executedInstanceBucketedByEpoch() throws Exception
    {
        KeyState dm = new KeyState(0);

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
        Assert.assertEquals(Sets.newHashSet(dep1), executions.get((long) 1));
        Assert.assertEquals(Sets.newHashSet(dep2, dep3), executions.get((long)2));
    }

    @Test
    public void epochSetIsIdempotent() throws Exception
    {
        KeyState dm = new KeyState(1, 2);

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
        KeyState dm = new KeyState(0);

        UUID dep = UUIDGen.getTimeUUID();
        dm.getDepsAndAdd(dep);

        Assert.assertEquals(0, dm.getEpoch());
        Assert.assertEquals(0, dm.getExecutionCount());

        dm.markExecuted(dep);
        Assert.assertEquals(1, dm.getExecutionCount());
    }

    @Test
    public void getEpochsOlderThanSuccess() throws Exception
    {
        Map<Long, Set<UUID>> expectedEpochs = new HashMap<>();
        long targetEpoch = 4;

        KeyState keyState = new KeyState(0);
        for (long i=0; i<targetEpoch; i++)
        {
            keyState.setEpoch(i);
            Set<UUID> ids = new HashSet<>();
            assert keyState.getEpoch() == i;
            for (UUID id: Lists.newArrayList(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID()))
            {
                keyState.markExecuted(id);
                ids.add(id);
            }
            assert keyState.getExecutionCount() == 2;

            // add to expected if we're below the end epoch
            if (i < targetEpoch - 2)
            {
                expectedEpochs.put(i, ids);
            }
        }

        keyState.setEpoch(targetEpoch);
        Map<Long, Set<UUID>> actualEpochs = keyState.getEpochsOlderThan(targetEpoch - 2);
        Assert.assertEquals(expectedEpochs, actualEpochs);
    }

    @Test(expected=IllegalArgumentException.class)
    public void getEpochsOlderThanFailure() throws Exception
    {
        long targetEpoch = 4;

        KeyState keyState = new KeyState(0);
        for (long i=0; i<targetEpoch; i++)
        {
            keyState.setEpoch(i);
            assert keyState.getEpoch() == i;
            for (UUID id: Lists.newArrayList(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID()))
            {
                keyState.markExecuted(id);
            }
            assert keyState.getExecutionCount() == 2;
        }

        keyState.setEpoch(targetEpoch);
        keyState.getEpochsOlderThan(targetEpoch - 1);
    }

    @Test
    public void canIncrementToEpochTrue() throws Exception
    {
        long targetEpoch = 4;
        long currentEpoch = targetEpoch - 1;

        KeyState keyState = new KeyState(0);
        for (long i=0; i<targetEpoch; i++)
        {
            keyState.setEpoch(i);
            assert keyState.getEpoch() == i;
            for (UUID id: Lists.newArrayList(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID()))
            {
                keyState.markExecuted(id);
                if (i == currentEpoch)
                {
                    // if this is the 'current' or previous epoch, set the dependency as active
                    keyState.create(id);
                }
            }
            assert keyState.getExecutionCount() == 2;
        }

        Assert.assertTrue(keyState.canIncrementToEpoch(targetEpoch));
    }

    @Test
    public void canIncrementToEpochFalse() throws Exception
    {
        long targetEpoch = 4;
        long currentEpoch = targetEpoch - 1;

        KeyState keyState = new KeyState(0);
        for (long i=0; i<targetEpoch; i++)
        {
            keyState.setEpoch(i);
            for (UUID id: Lists.newArrayList(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID()))
            {
                keyState.markExecuted(id);
                if (i == currentEpoch - 1)
                {
                    // if this is the 'current' epoch, set the dependency as active
                    keyState.create(id);
                }
            }
        }

        Assert.assertFalse(keyState.canIncrementToEpoch(targetEpoch));
    }
}


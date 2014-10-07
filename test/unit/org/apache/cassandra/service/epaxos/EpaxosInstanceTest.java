package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import org.apache.cassandra.service.epaxos.exceptions.BallotException;
import org.apache.cassandra.service.epaxos.exceptions.InvalidInstanceStateChange;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class EpaxosInstanceTest
{
    @Test
    public void instanceIdIsntRecordedAsDep() throws Exception
    {
        Instance instance = new Instance(null);
        Set<UUID> deps = Sets.newHashSet(UUIDGen.getTimeUUID(), instance.getId());

        instance.setDependencies(deps);

        Set<UUID> instanceDeps = instance.getDependencies();
        Assert.assertFalse(instanceDeps.contains(instance.getId()));
    }

    @Test(expected = InvalidInstanceStateChange.class)
    public void invalidPromotionFailure() throws Exception
    {
        Instance instance = new Instance(null);
        instance.setState(Instance.State.ACCEPTED);
        instance.setState(Instance.State.PREACCEPTED);
    }

    @Test(expected = BallotException.class)
    public void invalidBallot() throws Exception
    {
        Instance instance = new Instance(null);
        instance.updateBallot(5);
        instance.checkBallot(4);
    }

    @Test
    public void preacceptSuccess() throws Exception
    {
        Instance instance = new Instance(null);
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID());

        instance.preaccept(expectedDeps);

        Assert.assertEquals(Instance.State.PREACCEPTED, instance.getState());
        Assert.assertEquals(expectedDeps, instance.getDependencies());
    }

    @Test
    public void preacceptSuccessLeaderAgree() throws Exception
    {
        Instance instance = new Instance(null);
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID());
        Set<UUID> leaderDeps = new HashSet<>(expectedDeps);

        instance.preaccept(expectedDeps, leaderDeps);

        Assert.assertEquals(expectedDeps, leaderDeps);
        Assert.assertTrue(instance.getLeaderDepsMatch());
    }

    @Test
    public void preacceptSuccessLeaderDisagree() throws Exception
    {
        Instance instance = new Instance(null);
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID());
        Set<UUID> leaderDeps = Sets.newHashSet(UUIDGen.getTimeUUID());

        instance.preaccept(expectedDeps, leaderDeps);

        Assert.assertNotSame(expectedDeps, leaderDeps);
        Assert.assertFalse(instance.getLeaderDepsMatch());
    }

}

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
        Instance instance = new Instance(null, null);
        Set<UUID> deps = Sets.newHashSet(UUIDGen.getTimeUUID(), instance.getId());

        instance.setDependencies(deps);

        Set<UUID> instanceDeps = instance.getDependencies();
        Assert.assertFalse(instanceDeps.contains(instance.getId()));
    }

    @Test(expected = InvalidInstanceStateChange.class)
    public void invalidPromotionFailure() throws Exception
    {
        Instance instance = new Instance(null, null);
        instance.setState(Instance.State.ACCEPTED);
        instance.setState(Instance.State.PREACCEPTED);
    }

    @Test(expected = BallotException.class)
    public void invalidBallot() throws Exception
    {
        Instance instance = new Instance(null, null);
        instance.updateBallot(5);
        instance.checkBallot(4);
    }

    @Test
    public void preacceptSuccess() throws Exception
    {
        Instance instance = new Instance(null, null);
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID());

        instance.preaccept(expectedDeps);

        Assert.assertEquals(Instance.State.PREACCEPTED, instance.getState());
        Assert.assertEquals(expectedDeps, instance.getDependencies());
    }

    @Test
    public void preacceptSuccessLeaderAgree() throws Exception
    {
        Instance instance = new Instance(null, null);
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID());
        Set<UUID> leaderDeps = new HashSet<>(expectedDeps);

        instance.preaccept(expectedDeps, leaderDeps);

        Assert.assertEquals(expectedDeps, leaderDeps);
        Assert.assertTrue(instance.getLeaderDepsMatch());
    }

    @Test
    public void preacceptSuccessLeaderDisagree() throws Exception
    {
        Instance instance = new Instance(null, null);
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID());
        Set<UUID> leaderDeps = Sets.newHashSet(UUIDGen.getTimeUUID());

        instance.preaccept(expectedDeps, leaderDeps);

        Assert.assertNotSame(expectedDeps, leaderDeps);
        Assert.assertFalse(instance.getLeaderDepsMatch());
    }

    @Test
    public void acceptSuccess() throws Exception
    {
        Instance instance = new Instance(null, null);
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID());

        instance.accept(expectedDeps);

        Assert.assertEquals(Instance.State.ACCEPTED, instance.getState());
        Assert.assertEquals(expectedDeps, instance.getDependencies());
    }

    @Test
    public void commitSuccess() throws Exception
    {
        Instance instance = new Instance(null, null);
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID());

        instance.commit(expectedDeps);

        Assert.assertEquals(Instance.State.COMMITTED, instance.getState());
        Assert.assertEquals(expectedDeps, instance.getDependencies());
    }

    @Test
    public void setExecutedSuccess() throws Exception
    {
        Instance instance = new Instance(null, null);
        instance.setExecuted();
        Assert.assertEquals(Instance.State.EXECUTED, instance.getState());
    }

    @Test
    public void copyRemote() throws Exception
    {
        // TODO: test that remotely copied instances have all the important things copied
    }

    @Test
    public void applyInstance() throws Exception
    {
        // TODO: test that
    }

}

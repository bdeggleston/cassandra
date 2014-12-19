package org.apache.cassandra.service.epaxos.integration;

import com.google.common.collect.Sets;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.epaxos.Instance;
import org.apache.cassandra.service.epaxos.TokenInstance;
import org.apache.cassandra.service.epaxos.TokenStateMaintenanceTask;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;
import java.util.UUID;

public class EpaxosEpochIntegrationTest extends AbstractEpaxosIntegrationTest.SingleThread
{
    private static final Token TOKEN = DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(1234));

    @Test
    public void successCase() throws Exception
    {
        int roundSize = 10;

        // sanity check
        for (Node node: nodes)
        {
            Assert.assertEquals(0, node.getCurrentEpoch(TOKEN));
            node.setEpochIncrementThreshold(roundSize);
        }

        Set<UUID> round1Ids = Sets.newHashSet();
        for (int i=0; i<roundSize; i++)
        {
            Node leader = nodes.get(i % nodes.size());
            leader.query(getSerializedCQLRequest(i, 0));
            Instance instance = leader.getLastCreatedInstance();
            round1Ids.add(instance.getId());
        }

        Instance lastTokenInstance;
        {
            Node leader = nodes.get(0);
            TokenStateMaintenanceTask maintenanceTask = leader.newTokenStateMaintenanceTask();
            maintenanceTask.run();

            for (Node node: nodes)
            {
                Assert.assertEquals(1, node.getCurrentEpoch(TOKEN));
            }

            Assert.assertTrue(leader.getLastCreatedInstance() instanceof TokenInstance);
            TokenInstance instance = (TokenInstance) leader.getLastCreatedInstance();

            Assert.assertEquals(1, instance.getEpoch());
            Assert.assertFalse(instance.isVetoed());

            // should have taken all of the first round of instances
            // as deps since they all targeted a different key
            Assert.assertEquals(round1Ids, instance.getDependencies());

            lastTokenInstance = instance;
        }

        Set<UUID> round2Ids = Sets.newHashSet();
        for (int i=0; i<roundSize; i++)
        {
            Node leader = nodes.get(i % nodes.size());
            leader.query(getSerializedCQLRequest(i + roundSize, 0));
//            leader.query(getSerializedCQLRequest(i, 0));
            Instance instance = leader.getLastCreatedInstance();
            round2Ids.add(instance.getId());
        }

        {
            Node leader = nodes.get(0);
            TokenStateMaintenanceTask maintenanceTask = leader.newTokenStateMaintenanceTask();
            maintenanceTask.run();

            for (Node node: nodes)
            {
                Assert.assertEquals(2, node.getCurrentEpoch(TOKEN));
            }

            Assert.assertTrue(leader.getLastCreatedInstance() instanceof TokenInstance);
            TokenInstance instance = (TokenInstance) leader.getLastCreatedInstance();

            Assert.assertEquals(2, instance.getEpoch());
            Assert.assertFalse(instance.isVetoed());

            // should have taken all of the first round of instances
            // as deps since they all targeted a different key
            Set<UUID> expectedDeps = Sets.newHashSet(round2Ids);
            expectedDeps.add(lastTokenInstance.getId());

            Set<UUID> oldDeps = Sets.intersection(round1Ids, instance.getDependencies());
            Assert.assertEquals(0, oldDeps.size());
            Assert.assertEquals(expectedDeps, instance.getDependencies());
        }
    }
}

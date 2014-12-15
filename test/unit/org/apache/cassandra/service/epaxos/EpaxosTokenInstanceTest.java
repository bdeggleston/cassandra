package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.UUID;

public class EpaxosTokenInstanceTest
{
    static final InetAddress LEADER;
    static final Token TOKEN = DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(1234));

    static
    {
        try
        {
            LEADER = InetAddress.getByAddress(new byte[]{0, 0, 0, 2});
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    @Test
    public void serialization() throws Exception
    {

    }

    @Test
    public void executeIncrementsEpoch() throws Exception
    {

    }

    @Test
    public void executeSubmitsGCTask() throws Exception
    {

    }

    @Test
    public void epochIncrementConsistencyLevel() throws Exception
    {

    }

    @Test
    public void vetoed() throws Exception
    {
        TokenInstance instance = new TokenInstance(LEADER, TOKEN, 5);
        Set<UUID> deps = Sets.newHashSet(UUIDGen.getTimeUUID());
        instance.preaccept(deps, deps);

        Assert.assertFalse(instance.skipExecution());
        Assert.assertTrue(instance.getLeaderAttrsMatch());

        instance.setVetoed(true);
        Assert.assertTrue(instance.skipExecution());
        Assert.assertFalse(instance.getLeaderAttrsMatch());
    }
}

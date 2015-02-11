package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LongToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class EpaxosTokenStateTest extends AbstractEpaxosTest
{
    private static final IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
    private static final UUID CFID = UUIDGen.getTimeUUID();

    @Test
    public void serialization() throws IOException
    {
        TokenState ts = new TokenState(partitioner.getToken(ByteBufferUtil.bytes(123)), CFID, 4, 5, 6);
        DataOutputBuffer out = new DataOutputBuffer();
        TokenState.serializer.serialize(ts, out, 0);

        long expectedSize = out.getLength();
        Assert.assertEquals(expectedSize, TokenState.serializer.serializedSize(ts, 0));

        TokenState deserialized = TokenState.serializer.deserialize(ByteStreams.newDataInput(out.getData()), 0);
        Assert.assertEquals(ts.getToken(), deserialized.getToken());
        Assert.assertEquals(ts.getEpoch(), deserialized.getEpoch());
        Assert.assertEquals(ts.getHighEpoch(), deserialized.getHighEpoch());
        Assert.assertEquals(ts.getExecutions(), deserialized.getExecutions());
    }

    /**
     * Tests setHighEpoch only returns true if the given
     * value is higher than the current high epoch value
     */
    @Test
    public void highEpoch()
    {
        TokenState ts = new TokenState(partitioner.getToken(ByteBufferUtil.bytes(123)), CFID, 0, 0, 0);
        Assert.assertEquals(0, ts.getHighEpoch());

        Assert.assertTrue(ts.recordHighEpoch(1));
        Assert.assertEquals(1, ts.getHighEpoch());

        Assert.assertFalse(ts.recordHighEpoch(1));
        Assert.assertEquals(1, ts.getHighEpoch());
    }

    /**
     * Tests that the high epoch can't be decremented
     */
    @Test
    public void highEpochDecrementFailure()
    {

        TokenState ts = new TokenState(partitioner.getToken(ByteBufferUtil.bytes(123)), CFID, 0, 5, 0);
        Assert.assertEquals(5, ts.getHighEpoch());

        Assert.assertFalse(ts.recordHighEpoch(1));
        Assert.assertEquals(5, ts.getHighEpoch());
    }

    /**
     * Test that getExecutions and getNumUnrecordedExecutions increment for each call to recordExecution
     */
    @Test
    public void recordExecutions()
    {
        TokenState ts = new TokenState(partitioner.getToken(ByteBufferUtil.bytes(123)), CFID, 0, 0, 0);
        Assert.assertEquals(0, ts.getEpoch());
        Assert.assertEquals(0, ts.getExecutions());
        Assert.assertEquals(0, ts.getNumUnrecordedExecutions());

        ts.recordExecution();
        ts.recordExecution();
        ts.recordExecution();

        Assert.assertEquals(3, ts.getExecutions());
        Assert.assertEquals(3, ts.getNumUnrecordedExecutions());
    }

    @Test
    public void setEpochResetsExecutions()
    {
        TokenState ts = new TokenState(partitioner.getToken(ByteBufferUtil.bytes(123)), CFID, 4, 5, 6);
        Assert.assertEquals(4, ts.getEpoch());
        Assert.assertEquals(6, ts.getExecutions());

        ts.setEpoch(5);
        Assert.assertEquals(0, ts.getExecutions());
        Assert.assertEquals(0, ts.getNumUnrecordedExecutions());
    }

    /**
     * Test getNumUnrecordedExecutions goes back to zero when saved
     */
    @Test
    public void onSave()
    {
        TokenState ts = new TokenState(partitioner.getToken(ByteBufferUtil.bytes(123)), CFID, 0, 0, 0);
        Assert.assertEquals(0, ts.getEpoch());
        Assert.assertEquals(0, ts.getExecutions());
        Assert.assertEquals(0, ts.getNumUnrecordedExecutions());

        ts.recordExecution();
        ts.recordExecution();
        ts.recordExecution();

        Assert.assertEquals(3, ts.getNumUnrecordedExecutions());
        ts.onSave();
        Assert.assertEquals(0, ts.getNumUnrecordedExecutions());
    }

    /**
     * Check that correct token instance ids are returned, and removed
     * when epoch is changed
     */
    @Test
    public void epochInstances()
    {
        TokenState ts = new TokenState(partitioner.getToken(ByteBufferUtil.bytes(123)), CFID, 0, 0, 0);
        UUID i0 = UUIDGen.getTimeUUID();
        UUID i1 = UUIDGen.getTimeUUID();
        UUID i2 = UUIDGen.getTimeUUID();

        ts.recordEpochInstance(0, i0);
        ts.recordEpochInstance(0, i1);
        ts.recordEpochInstance(1, i2);

        Assert.assertEquals(Sets.newHashSet(i0, i1, i2), ts.getCurrentEpochInstances());

        ts.setEpoch(1);

        Assert.assertEquals(Sets.newHashSet(i2), ts.getCurrentEpochInstances());
    }

    @Test
    public void epochInstanceCleanup()
    {

    }

    @Test
    public void tokenInstances()
    {
        TokenState ts = new TokenState(new LongToken(200l), CFID, 0, 0, 0);
        UUID tId0 = UUIDGen.getTimeUUID();
        UUID tId1 = UUIDGen.getTimeUUID();
        ts.recordTokenInstance(new LongToken(75l), tId0);
        ts.recordTokenInstance(new LongToken(150l), tId1);

        Assert.assertEquals(Sets.newHashSet(tId0, tId1), ts.getCurrentTokenInstances(new LongToken(151l)));
        Assert.assertEquals(Sets.newHashSet(tId0, tId1), ts.getCurrentTokenInstances(new LongToken(150l)));
        Assert.assertEquals(Sets.newHashSet(tId0), ts.getCurrentTokenInstances(new LongToken(149l)));
        Assert.assertEquals(Sets.newHashSet(tId0), ts.getCurrentTokenInstances(new LongToken(75l)));
        Assert.assertEquals(Sets.<UUID>newHashSet(), ts.getCurrentTokenInstances(new LongToken(50l)));
    }

    /**
     * splitTokenInstances should return a map of all token instances less than or equal to the
     * given token. These token instances should also be removed from the token state
     */
    @Test
    public void tokenInstanceCleanup()
    {
        TokenState ts = new TokenState(new LongToken(200l), CFID, 0, 0, 0);
        UUID t50Id = UUIDGen.getTimeUUID();
        ts.recordTokenInstance(new LongToken(50l), t50Id);
        UUID t100Id = UUIDGen.getTimeUUID();
        ts.recordTokenInstance(new LongToken(100l), t100Id);
        UUID t150Id = UUIDGen.getTimeUUID();
        ts.recordTokenInstance(new LongToken(150l), t150Id);

        Assert.assertEquals(Sets.<UUID>newHashSet(), ts.getCurrentTokenInstances(new LongToken(0l)));
        Assert.assertEquals(Sets.newHashSet(t50Id, t100Id, t150Id), ts.getCurrentTokenInstances(new LongToken(200l)));

        Map<Token, Set<UUID>> expectedDeps = new HashMap<>();
        expectedDeps.put(new LongToken(50l), Sets.newHashSet(t50Id));
        expectedDeps.put(new LongToken(100l), Sets.newHashSet(t100Id));
        Map<Token, Set<UUID>> actualDeps = ts.splitTokenInstances(new LongToken(100l));

        Assert.assertEquals(expectedDeps, actualDeps);

        Assert.assertEquals(Sets.newHashSet(t150Id), ts.getCurrentTokenInstances(new LongToken(200l)));
    }
}

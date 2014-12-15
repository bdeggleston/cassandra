package org.apache.cassandra.service.epaxos;

import com.google.common.io.ByteStreams;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class EpaxosTokenStateTest extends AbstractEpaxosTest
{
    private static final IPartitioner partitioner = DatabaseDescriptor.getPartitioner();

    @Test
    public void serialization() throws IOException
    {
        TokenState ts = new TokenState(partitioner.getToken(ByteBufferUtil.bytes(123)), 4, 5, 6);
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
        TokenState ts = new TokenState(partitioner.getToken(ByteBufferUtil.bytes(123)), 0, 0, 0);
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

        TokenState ts = new TokenState(partitioner.getToken(ByteBufferUtil.bytes(123)), 0, 5, 0);
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
        TokenState ts = new TokenState(partitioner.getToken(ByteBufferUtil.bytes(123)), 0, 0, 0);
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
        TokenState ts = new TokenState(partitioner.getToken(ByteBufferUtil.bytes(123)), 4, 5, 6);
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
        TokenState ts = new TokenState(partitioner.getToken(ByteBufferUtil.bytes(123)), 0, 0, 0);
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
}

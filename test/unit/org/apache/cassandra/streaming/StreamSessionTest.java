package org.apache.cassandra.streaming;

import org.apache.cassandra.service.epaxos.ExecutionInfo;
import org.apache.cassandra.service.epaxos.Scope;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.nio.ByteBuffer;

public class StreamSessionTest
{
    @Test
    public void epaxosCorrections() throws Exception
    {
        InetAddress peer = InetAddress.getByAddress(new byte[] {127, 0, 0 , 1});
        StreamSession session = new StreamSession(peer, null, 0);

        ByteBuffer key = ByteBufferUtil.bytes(1234);
        ExecutionInfo executionInfo1 = new ExecutionInfo(2, 5);
        ExecutionInfo executionInfo2 = new ExecutionInfo(2, 6);
        ExecutionInfo executionInfo3 = new ExecutionInfo(1, 3);

        Assert.assertTrue(session.addEpaxosCorrection(key, Scope.DC.global(), executionInfo1));
        Assert.assertTrue(session.addEpaxosCorrection(key, Scope.DC.global(), executionInfo2));
        Assert.assertFalse(session.addEpaxosCorrection(key, Scope.DC.global(), executionInfo3));
        Assert.assertFalse(session.addEpaxosCorrection(key, Scope.DC.global(), executionInfo3));
        ExecutionInfo actual = session.getExpaxosCorrections().get(Scope.DC.global()).get(key);
        Assert.assertEquals(executionInfo2, actual);
    }
}

package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.UUID;

/**
 * tests the AbstractEpochVerbHandler
 */
public class EpaxosEpochVerbHandlerTest extends AbstractEpaxosTest
{
    private static final Token TOKEN = DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(1234));
    private static final UUID CFID = UUIDGen.getTimeUUID();

    private static MessageIn<Message> getMessage(long epoch) throws UnknownHostException
    {
        return MessageIn.create(InetAddress.getLocalHost(),
                                new Message(TOKEN, CFID, epoch),
                                Collections.EMPTY_MAP,
                                MessagingService.Verb.ECHO,
                                0);
    }

    private static class Message extends AbstractEpochMessage
    {
        private Message(Token token, UUID cfId, long epoch)
        {
            super(token, cfId, epoch);
        }
    }

    private static class Handler extends AbstractEpochVerbHandler<Message>
    {
        public volatile int doEpochVerbCalls = 0;

        private Handler(EpaxosState state)
        {
            super(state);
        }

        @Override
        public void doEpochVerb(MessageIn<Message> message, int id)
        {
            doEpochVerbCalls++;
        }
    }

    private static class State extends MockVerbHandlerState
    {
        public volatile int remoteFailureCalls = 0;
        public volatile int localFailureCalls = 0;

        public final long epoch;

        private State(long epoch)
        {
            this.epoch = epoch;
        }

        @Override
        public void startRemoteFailureRecovery(InetAddress endpoint, Token token, long epoch)
        {
            remoteFailureCalls++;
        }

        @Override
        public void startLocalFailureRecovery(Token token, long epoch)
        {
            localFailureCalls++;
        }

        @Override
        public long getCurrentEpoch(Token token, UUID cfId)
        {
            return epoch;
        }
    }

    @Test
    public void successCase() throws Exception
    {
        State state = new State(5);
        Handler handler = new Handler(state);

        Assert.assertEquals(0, handler.doEpochVerbCalls);
        Assert.assertEquals(0, state.remoteFailureCalls);
        Assert.assertEquals(0, state.localFailureCalls);

        handler.doVerb(getMessage(4), 0);

        Assert.assertEquals(1, handler.doEpochVerbCalls);
        Assert.assertEquals(0, state.remoteFailureCalls);
        Assert.assertEquals(0, state.localFailureCalls);

        handler.doVerb(getMessage(5), 0);

        Assert.assertEquals(2, handler.doEpochVerbCalls);
        Assert.assertEquals(0, state.remoteFailureCalls);
        Assert.assertEquals(0, state.localFailureCalls);

        handler.doVerb(getMessage(6), 0);

        Assert.assertEquals(3, handler.doEpochVerbCalls);
        Assert.assertEquals(0, state.remoteFailureCalls);
        Assert.assertEquals(0, state.localFailureCalls);
    }

    @Test
    public void localFailure() throws Exception
    {
        State state = new State(5);
        Handler handler = new Handler(state);

        Assert.assertEquals(0, handler.doEpochVerbCalls);
        Assert.assertEquals(0, state.remoteFailureCalls);
        Assert.assertEquals(0, state.localFailureCalls);

        handler.doVerb(getMessage(7), 0);

        Assert.assertEquals(0, handler.doEpochVerbCalls);
        Assert.assertEquals(0, state.remoteFailureCalls);
        Assert.assertEquals(1, state.localFailureCalls);
    }

    @Test
    public void remoteFailure() throws Exception
    {
        State state = new State(5);
        Handler handler = new Handler(state);

        Assert.assertEquals(0, handler.doEpochVerbCalls);
        Assert.assertEquals(0, state.remoteFailureCalls);
        Assert.assertEquals(0, state.localFailureCalls);

        handler.doVerb(getMessage(3), 0);

        Assert.assertEquals(0, handler.doEpochVerbCalls);
        Assert.assertEquals(1, state.remoteFailureCalls);
        Assert.assertEquals(0, state.localFailureCalls);
    }
}

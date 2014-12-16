package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;

public class EpaxosEpochCallbackTest
{
    private static final Token TOKEN = DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(1234));

    private static MessageIn<Message> getMessage(long epoch) throws UnknownHostException
    {
        return MessageIn.create(InetAddress.getLocalHost(),
                                new Message(TOKEN, epoch),
                                Collections.EMPTY_MAP,
                                MessagingService.Verb.ECHO,
                                0);
    }

    private static class Message extends AbstractEpochMessage
    {
        private Message(Token token, long epoch)
        {
            super(token, epoch);
        }
    }

    private static class Callback extends AbstractEpochCallback<Message>
    {
        public volatile int epochResponseCalls = 0;

        private Callback(EpaxosState state)
        {
            super(state);
        }

        @Override
        public void epochResponse(MessageIn<Message> msg)
        {
            epochResponseCalls++;
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
        public long getCurrentEpoch(Token token)
        {
            return epoch;
        }
    }

    @Test
    public void successCase() throws Exception
    {
        State state = new State(5);
        Callback callback = new Callback(state);

        Assert.assertEquals(0, callback.epochResponseCalls);
        Assert.assertEquals(0, state.remoteFailureCalls);
        Assert.assertEquals(0, state.localFailureCalls);

        callback.response(getMessage(4));

        Assert.assertEquals(1, callback.epochResponseCalls);
        Assert.assertEquals(0, state.remoteFailureCalls);
        Assert.assertEquals(0, state.localFailureCalls);

        callback.response(getMessage(5));

        Assert.assertEquals(2, callback.epochResponseCalls);
        Assert.assertEquals(0, state.remoteFailureCalls);
        Assert.assertEquals(0, state.localFailureCalls);

        callback.response(getMessage(6));

        Assert.assertEquals(3, callback.epochResponseCalls);
        Assert.assertEquals(0, state.remoteFailureCalls);
        Assert.assertEquals(0, state.localFailureCalls);
    }

    @Test
    public void localFailure() throws Exception
    {
        State state = new State(5);
        Callback callback = new Callback(state);

        Assert.assertEquals(0, callback.epochResponseCalls);
        Assert.assertEquals(0, state.remoteFailureCalls);
        Assert.assertEquals(0, state.localFailureCalls);

        callback.response(getMessage(7));

        Assert.assertEquals(0, callback.epochResponseCalls);
        Assert.assertEquals(0, state.remoteFailureCalls);
        Assert.assertEquals(1, state.localFailureCalls);
    }

    @Test
    public void remoteFailure() throws Exception
    {
        State state = new State(5);
        Callback callback = new Callback(state);

        Assert.assertEquals(0, callback.epochResponseCalls);
        Assert.assertEquals(0, state.remoteFailureCalls);
        Assert.assertEquals(0, state.localFailureCalls);

        callback.response(getMessage(3));

        Assert.assertEquals(0, callback.epochResponseCalls);
        Assert.assertEquals(1, state.remoteFailureCalls);
        Assert.assertEquals(0, state.localFailureCalls);

    }
}

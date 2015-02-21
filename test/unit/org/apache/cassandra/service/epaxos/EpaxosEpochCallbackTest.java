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

public class EpaxosEpochCallbackTest
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
        public final TokenState.State state;

        private State(long epoch)
        {
            this(epoch, TokenState.State.NORMAL);
        }

        private State(long epoch, TokenState.State state)
        {
            this.epoch = epoch;
            this.state = state;
        }

        @Override
        public void startRemoteFailureRecovery(InetAddress endpoint, Token token, UUID cfId, long epoch)
        {
            remoteFailureCalls++;
        }

        @Override
        public void startLocalFailureRecovery(Token token, UUID cfId, long epoch)
        {
            localFailureCalls++;
        }

        @Override
        public TokenState getTokenState(IEpochMessage message)
        {
            return new TokenState(message.getToken(), message.getCfId(), epoch, epoch, 0, state);
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

    private void assertModeResponse(TokenState.State mode, boolean doCallbackExpected) throws UnknownHostException
    {
        State state = new State(5, mode);
        Callback callback = new Callback(state);

        Assert.assertEquals(0, callback.epochResponseCalls);
        Assert.assertEquals(0, state.remoteFailureCalls);
        Assert.assertEquals(0, state.localFailureCalls);

        callback.response(getMessage(5));

        Assert.assertEquals(doCallbackExpected ? 1 : 0, callback.epochResponseCalls);
        Assert.assertEquals(0, state.remoteFailureCalls);
        Assert.assertEquals(0, state.localFailureCalls);
    }

    @Test
    public void recoveryModes() throws Exception
    {
        assertModeResponse(TokenState.State.NORMAL, true);
        assertModeResponse(TokenState.State.PRE_RECOVERY, false);
        assertModeResponse(TokenState.State.RECOVERING_INSTANCES, false);
        assertModeResponse(TokenState.State.RECOVERING_DATA, true);
    }
}

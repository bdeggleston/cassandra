package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
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
    private static final Token MANAGED_TOKEN = token(100);
    private static final Range<Token> MANAGED_RANGE = range(TOKEN0, MANAGED_TOKEN);
    private static final Token MESSAGE_TOKEN = token(50);
    private static final UUID CFID = UUIDGen.getTimeUUID();

    private static MessageIn<Message> getMessage(long epoch) throws UnknownHostException
    {
        return MessageIn.create(LOCALHOST,
                                new Message(MESSAGE_TOKEN, CFID, epoch),
                                Collections.<String, byte[]>emptyMap(),
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
            Assert.assertEquals(MANAGED_TOKEN, token);
        }

        @Override
        public void startLocalFailureRecovery(Token token, UUID cfId, long epoch)
        {
            localFailureCalls++;
            Assert.assertEquals(MANAGED_TOKEN, token);
        }

        @Override
        public TokenState getTokenState(IEpochMessage message)
        {
            return new TokenState(MANAGED_RANGE, message.getCfId(), epoch, 0, state);
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

    private void assertModeResponse(TokenState.State mode, boolean doVerbExpected, final boolean passiveRecord) throws UnknownHostException
    {
        State state = new State(5, mode);
        Handler handler = new Handler(state) {
            @Override
            public boolean canPassiveRecord()
            {
                return passiveRecord;
            }
        };

        Assert.assertEquals(0, handler.doEpochVerbCalls);
        Assert.assertEquals(0, state.remoteFailureCalls);
        Assert.assertEquals(0, state.localFailureCalls);

        handler.doVerb(getMessage(5), 0);

        Assert.assertEquals(doVerbExpected ? 1 : 0, handler.doEpochVerbCalls);
        Assert.assertEquals(0, state.remoteFailureCalls);
        Assert.assertEquals(0, state.localFailureCalls);
    }

    @Test
    public void recoveryModes() throws Exception
    {
        assertModeResponse(TokenState.State.NORMAL, true, false);
        assertModeResponse(TokenState.State.NORMAL, true, true);

        assertModeResponse(TokenState.State.PRE_RECOVERY, false, false);
        assertModeResponse(TokenState.State.PRE_RECOVERY, false, true);

        assertModeResponse(TokenState.State.RECOVERING_INSTANCES, false, false);
        assertModeResponse(TokenState.State.RECOVERING_INSTANCES, true, true);

        assertModeResponse(TokenState.State.RECOVERING_DATA, true, false);
        assertModeResponse(TokenState.State.RECOVERING_DATA, true, true);
    }

    @Test
    public void recoveryRequiredTokenState() throws Exception
    {
        State state = new State(5, TokenState.State.RECOVERY_REQUIRED);
        Handler handler = new Handler(state);
        handler.doVerb(getMessage(5), 0);
        Assert.assertEquals(1, state.localFailureCalls);
    }
}

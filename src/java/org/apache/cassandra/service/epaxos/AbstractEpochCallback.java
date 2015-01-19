package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractEpochCallback<T extends IEpochMessage> implements IAsyncCallback<T>
{
    private static final Logger logger = LoggerFactory.getLogger(PreacceptVerbHandler.class);

    protected final EpaxosState state;

    protected AbstractEpochCallback(EpaxosState state)
    {
        this.state = state;
    }

    @Override
    public final void response(MessageIn<T> message)
    {
        // TODO: consider the null instance behavior of PrepareVerbHandler when evaluating tokens (probably shouldn't quantize them to token boundaries)

        TokenState tokenState = state.getTokenState(message.payload);
        tokenState.rwLock.readLock().lock();
        EpochDecision decision;
        try
        {
            TokenState.State s = tokenState.getState();
            if (!s.isOkToParticipate())
            {
                return;
            }
            decision = tokenState.evaluateMessageEpoch(message.payload);
        }
        finally
        {
            tokenState.rwLock.readLock().unlock();
        }

        switch (decision.outcome)
        {
            case LOCAL_FAILURE:
                logger.debug("Unrecoverable local state", decision);
                state.startLocalFailureRecovery(decision.token, decision.remoteEpoch);
                break;
            case REMOTE_FAILURE:
                logger.debug("Unrecoverable remote state", decision);
                state.startRemoteFailureRecovery(message.from, decision.token, decision.localEpoch);
                break;
            case OK:
                epochResponse(message);
                break;
            default:
                throw new IllegalStateException();
        }
    }

    public abstract void epochResponse(MessageIn<T> msg);

    @Override
    public boolean isLatencyForSnitch()
    {
        return false;
    }
}

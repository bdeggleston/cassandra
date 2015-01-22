package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the validation of message epochs
 */
public abstract class AbstractEpochVerbHandler<T extends IEpochMessage> implements IVerbHandler<T>
{
    private static final Logger logger = LoggerFactory.getLogger(PreacceptVerbHandler.class);

    protected final EpaxosState state;

    protected AbstractEpochVerbHandler(EpaxosState state)
    {
        this.state = state;
    }

    @Override
    public final void doVerb(MessageIn<T> message, int id)
    {

        TokenState tokenState = state.getTokenState(message.payload);
        tokenState.rwLock.readLock().lock();
        EpochDecision decision;
        try
        {
            TokenState.State s = tokenState.getState();
            if (!s.isOkToParticipate())
            {
                if (!(s.isPassiveRecord() && canPassiveRecord()))
                {
                    // can't do anything, don't respond
                    return;
                }
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
                doEpochVerb(message, id);
                break;
            default:
                throw new IllegalStateException();
        }
    }

    /**
     * indicates that this verb handler can still process messages
     * if the token state is in passive record mode
     */
    public boolean canPassiveRecord()
    {
        return false;
    }

    public abstract void doEpochVerb(MessageIn<T> message, int id);
}

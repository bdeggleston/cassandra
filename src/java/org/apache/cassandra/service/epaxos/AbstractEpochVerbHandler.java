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
        EpochDecision decision = state.validateMessageEpoch(message.payload);
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

    public abstract void doEpochVerb(MessageIn<T> message, int id);
}

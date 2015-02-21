package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;

public class FailureRecoveryVerbHandler implements IVerbHandler<FailureRecoveryRequest>
{
    private final EpaxosState state;

    public FailureRecoveryVerbHandler(EpaxosState state)
    {
        this.state = state;
    }

    @Override
    public void doVerb(MessageIn<FailureRecoveryRequest> message, int id)
    {
        FailureRecoveryRequest request = message.payload;
        state.startLocalFailureRecovery(request.token, request.cfId, request.epoch);
    }
}

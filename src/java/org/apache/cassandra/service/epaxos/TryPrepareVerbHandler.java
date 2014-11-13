package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

public class TryPrepareVerbHandler implements IVerbHandler<UUID>
{
    private EpaxosState state;

    public TryPrepareVerbHandler(EpaxosState state)
    {
        this.state = state;
    }

    @Override
    public void doVerb(MessageIn<UUID> message, int id)
    {
        UUID iid = message.payload;
        ReadWriteLock lock = state.getInstanceLock(iid);
        lock.readLock().lock();
        try
        {
            Instance instance = state.loadInstance(iid);
            if (instance.getState().atLeast(Instance.State.COMMITTED))
            {
                TryPrepareResponse response = new TryPrepareResponse(false, false, 0, instance);
                MessageOut<TryPrepareResponse> reply = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE,
                                                                        response,
                                                                        TryPrepareResponse.serializer);
                state.sendReply(reply, id, message.from);
            }
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
}
